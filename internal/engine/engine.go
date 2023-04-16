package engine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/chen3feng/stl4go"
	"github.com/dablelv/go-huge-util/conv"
	"github.com/golang/protobuf/proto"
	"github.com/peterbourgon/diskv/v3"
	"github.com/sirupsen/logrus"
	"io"
	"iot-db/internal/filemanager"
	"iot-db/internal/pb"
	"iot-db/internal/shardgroup"
	"iot-db/pkg/skiplist"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type Item struct {
	*shardgroup.DeviceSkipList
	LatestIndexId int
}

type ShardGroup map[int64]*Item

type Cake struct {
	size           int
	shardGroup     map[int64]skiplist.MapI[*pb.Data, struct{}]
	shardGroupChan chan dump
	queryMutex     sync.RWMutex
	fileMutex      sync.RWMutex
	fieldDisk      *diskv.Diskv
	dataDisk       *diskv.Diskv
	fileManager    *filemanager.FileManager
	Optional       *Optional
	files          map[string]int
	noise          uint64
}

type dump struct {
	shardGroup skiplist.MapI[*pb.Data, struct{}]
	lastIndex  uint64
	shardId    int64
}

type Optional struct {
	FieldCachePath string
	DataCachePath  string
	WalPath        string
	CmdWalPath     string
	TempFilePath   string
	DataFilePath   string
}

func newShardGroup() skiplist.MapI[*pb.Data, struct{}] {
	return skiplist.NewSkipListMap[*pb.Data, struct{}](&DataCompare{})
}

func NewDefaultEngine() *Cake {
	return NewEngine(&Optional{
		FieldCachePath: "/data/iot-db/data/cache/files",
		DataCachePath:  "/data/iot-db/data/cache/data",
		WalPath:        "/data/iot-db/data/wal/data",
		CmdWalPath:     "/data/iot-db/data/wal/cmd",
		TempFilePath:   "/data/iot-db/data/tmp",
		DataFilePath:   "/data/iot-db/data/data",
	})
}

func NewEngine(op *Optional) *Cake {

	_ = os.MkdirAll(op.FieldCachePath, 0777)
	_ = os.MkdirAll(op.DataCachePath, 0777)
	_ = os.MkdirAll(op.WalPath, 0777)
	_ = os.MkdirAll(op.CmdWalPath, 0777)
	_ = os.MkdirAll(op.CmdWalPath, 0777)
	_ = os.MkdirAll(op.TempFilePath, 0777)
	_ = os.MkdirAll(op.DataFilePath, 0777)

	// 存储寄存器信息
	flatTransform := func(s string) []string { return []string{} }
	fieldDisk := diskv.New(diskv.Options{
		BasePath:     op.FieldCachePath,
		Transform:    flatTransform,
		CacheSizeMax: 1024 * 1024 * 20,
	})

	// 存储寄存器值信息
	dataDisk := diskv.New(diskv.Options{
		BasePath: op.DataCachePath,
		Transform: func(s string) []string {
			// ShardSize_ShardId_createdAt_(data/index/sIndex)
			ret := strings.Split(s, "_")
			return ret[0 : len(ret)-2]
		},
		CacheSizeMax: 1024 * 1024 * 20,
	})

	obj := &Cake{
		size:           0,
		shardGroup:     map[int64]skiplist.MapI[*pb.Data, struct{}]{},
		shardGroupChan: make(chan dump, 10),
		fieldDisk:      fieldDisk,
		dataDisk:       dataDisk,
		Optional:       op,
		files:          map[string]int{},
		noise:          0,
	}

	go obj.dump()

	return obj
}

func (c *Cake) readData(reader io.Reader) (*pb.Data, error) {
	var length uint32
	err := binary.Read(reader, binary.BigEndian, &length)
	if err != nil {
		return nil, err

	}
	body := make([]byte, length)
	_, err = reader.Read(body)
	if err != nil {
		return nil, err
	}

	v := pb.Data{}
	err = proto.Unmarshal(body, &v)
	if err != nil {
		return nil, err
	}
	return &v, nil

}

func (c *Cake) compactWithFile(noiseFile []string, start, end int64) {

	type Data struct {
		*pb.Data
		dataFile *os.File
	}

	var files []*File
	defer func() {
		for _, i := range files {
			_ = i.close()
		}
	}()

	fileName := fmt.Sprintf("%d_%d_%d", start, end, time.Now().UnixNano())

	tempFile, err := c.createTempFile(fileName)
	if err != nil {
		logrus.Fatalln(err)
	}

	writer, err := NewWriter(tempFile)
	if err != nil {
		return
	}

	h := stl4go.NewPriorityQueueFunc[Data](func(a, b Data) bool {
		if a.Did != b.Did {
			return a.Did < b.Did
		}
		if a.Timestamp != b.Timestamp {
			return a.Timestamp < b.Timestamp
		}
		return a.CreatedAt > b.CreatedAt
	})

	for _, i := range noiseFile {
		file, err := OpenFile(c.Optional.DataFilePath, i)
		if err != nil {
			logrus.Fatalln(err)
		}
		files = append(files, file)
		data, err := c.readData(file.DataFile)
		if err != nil {
			return
		}
		h.Push(Data{
			Data:     data,
			dataFile: file.DataFile,
		})
	}

	var index []*pb.IndexHeader_Index
	type Range struct {
		start, end int64
	}
	timeRange := map[int64]*Range{}
	offset := 0
	lastOffset := 0
	var did int64 = -1

	var lastTimestamp int64 = -1
	for !h.IsEmpty() {
		v := h.Top()
		h.Pop()

		// unique
		if v.Timestamp == lastTimestamp {
			continue
		}
		lastTimestamp = v.Timestamp

		if timeRange[v.Did] == nil {
			timeRange[v.Did] = &Range{
				start: math.MaxInt64,
				end:   math.MinInt64,
			}
		}
		if timeRange[v.Did].start > v.Timestamp {
			timeRange[v.Did].start = v.Timestamp
		}

		if timeRange[v.Did].end < v.Timestamp {
			timeRange[v.Did].end = v.Timestamp
		}

		if v.GetDid() != did && did != -1 {
			index = append(index, &pb.IndexHeader_Index{
				DeviceId: v.GetDid(),
				Offset:   int64(lastOffset),
				Start:    timeRange[v.Did].start,
				End:      timeRange[v.Did].end,
			})
			lastOffset = offset
		}
		offset++
		did = v.GetDid()

		err := writer.WriteData(v.Data)
		if err != nil {
			logrus.Fatalln(err)

		}

		err = writer.WriteFirstIndex()
		if err != nil {
			logrus.Fatalln(err)

		}

		data, err := c.readData(v.dataFile)
		if err != nil {
			continue
		}

		h.Push(Data{
			Data:     data,
			dataFile: v.dataFile,
		})

	}

	header := &pb.IndexHeader{
		Index: index,
	}
	marshal, err := proto.Marshal(header)
	if err != nil {
		logrus.Fatalln(err)
	}

	// write second index head
	err = binary.Write(tempFile.SecondIndexFile, binary.BigEndian, int32(len(marshal)))
	if err != nil {
		logrus.Fatalln(err)
	}

	_, err = tempFile.SecondIndexFile.Write(marshal)
	if err != nil {
		logrus.Fatalln(err)
	}

	logrus.Infoln("compact file:", tempFile.Name)
	err = c.saveTempFile(tempFile)
	if err != nil {
		logrus.Fatalln(err)
	}

	c.closeFiles(noiseFile)

	c.queryMutex.Lock()
	defer c.queryMutex.Unlock()

	for _, i := range files {
		name := i.Name
		go func() {
			logrus.Infoln("try remove file", name)
			for !c.tryRemoveFiles(name) {
				time.Sleep(time.Second * 5)
			}
		}()
	}

}

func (c *Cake) compact() {

	logrus.Infoln("compact...")
	for {
		noiseFile, start, end := c.getNoiseFile(1024 * 1024 * 100) // 100M
		if len(noiseFile) < 10 {
			logrus.Infoln("There are too few files to merge")
			c.closeFiles(noiseFile)
			time.Sleep(time.Second * 5)
			continue
		}
		logrus.Infoln("start compact...")
		c.compactWithFile(noiseFile, start, end)
		logrus.Infoln("stop compact...")

		time.Sleep(time.Second * 10)

	}
}

func (c *Cake) Insert(data []*pb.FullData) error {
	return c.insertMemory(data)
}

// 写入设备寄存器信息
func (c *Cake) writeKey(data []*pb.FullData) error {
	for _, data := range data {
		deviceId := conv.ToAny[string](data.Did)
		has := c.fieldDisk.Has(deviceId)
		if !has {
			key := bytes.NewBuffer([]byte{})
			err := binary.Write(key, binary.BigEndian, data.Key) // regs
			if err != nil {
				return err
			}
			err = c.fieldDisk.Write(deviceId, key.Bytes())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

const ShardGroupSize = int64(time.Hour * 24 * 7)

func toShardId(timestamp int64) int64 {
	return timestamp / ShardGroupSize
}

func (c *Cake) insertMemory(data []*pb.FullData) error {

	for _, data := range data {
		shardId := toShardId(data.Timestamp)
		s, ok := c.shardGroup[shardId]
		if !ok {
			c.shardGroup[shardId] = newShardGroup()
			s = c.shardGroup[shardId]
		}
		s.Insert(&pb.Data{
			Did:       data.Did,
			Timestamp: data.Timestamp,
			CreatedAt: data.CreatedAt,
			Value:     data.Value,
		}, struct{}{})

		c.size += len(data.Value)*4 + 12
	}

	if c.size > 1e6*20 {
		c.size = 0
		for shardId := range c.shardGroup {
			if c.shardGroup[shardId].Size() > 0 {
				logrus.Infoln("shard id", shardId)
				c.shardGroupChan <- dump{
					shardGroup: c.shardGroup[shardId],
					lastIndex:  0,
					shardId:    shardId,
				}
				c.shardGroup[shardId] = newShardGroup()
			}
		}
	}

	return nil
}

func (c *Cake) writeIndexHeader(shardGroup skiplist.MapI[*pb.Data, struct{}], file *File) {
	// get device
	it, err := shardGroup.Iterator()
	if err != nil {
		return
	}

	var index []*pb.IndexHeader_Index
	type Range struct {
		start, end int64
	}
	timeRange := map[int64]*Range{}
	offset := 0
	lastOffset := 0
	var did int64 = -1
	for {
		k, _, err := it.Next()
		if err != nil {
			break
		}

		if timeRange[k.Did] == nil {
			timeRange[k.Did] = &Range{
				start: math.MaxInt64,
				end:   math.MinInt64,
			}
		}
		if timeRange[k.Did].start > k.Timestamp {
			timeRange[k.Did].start = k.Timestamp
		}

		if timeRange[k.Did].end < k.Timestamp {
			timeRange[k.Did].end = k.Timestamp
		}

		if k.GetDid() != did && did != -1 {
			index = append(index, &pb.IndexHeader_Index{
				DeviceId: k.GetDid(),
				Offset:   int64(lastOffset),
				Start:    timeRange[k.Did].start,
				End:      timeRange[k.Did].end,
			})
			lastOffset = offset
		}
		offset++
		did = k.GetDid()
	}

	header := &pb.IndexHeader{
		Index: index,
	}
	marshal, err := proto.Marshal(header)
	if err != nil {
		logrus.Fatalln(err)
	}

	// write index head
	err = binary.Write(file.SecondIndexFile, binary.BigEndian, int32(len(marshal)))
	if err != nil {
		return
	}

	_, err = file.SecondIndexFile.Write(marshal)
	if err != nil {
		return
	}
}

func (c *Cake) readIndexHeader(reader io.Reader) (*pb.IndexHeader, error) {
	var length int32
	// write index head
	err := binary.Read(reader, binary.BigEndian, &length)
	if err != nil {
		return nil, err
	}

	marshal := make([]byte, length)
	_, err = reader.Read(marshal)
	if err != nil {
		return nil, err
	}
	header := &pb.IndexHeader{}
	err = proto.Unmarshal(marshal, header)
	if err != nil {
		return nil, err
	}
	return header, nil

}

func (c *Cake) getShardGroupMeta(shardGroup skiplist.MapI[*pb.Data, struct{}]) (int64, int64) {
	it, err := shardGroup.Iterator()
	if err != nil {
		return 0, 0
	}

	start := int64(math.MaxInt64)
	end := int64(math.MinInt64)

	for {
		k, _, err := it.Next()
		if err != nil {
			break
		}

		if start > k.Timestamp {
			start = k.Timestamp
		}

		if end < k.Timestamp {
			end = k.Timestamp
		}

	}
	return start, end
}

func (c *Cake) dump() {
	for msg := range c.shardGroupChan {
		shardGroup := msg.shardGroup

		logrus.Infoln("size:", shardGroup.Size())

		dataReader, dataWriter, err := os.Pipe()
		if err != nil {
			logrus.Fatalln(err)
		}
		var indexs []*Index
		go func() {
			it, err := shardGroup.Iterator()
			if err != nil {
				return
			}

			var offset uint64
			for {
				data, _, err := it.Next()
				if err != nil {
					break
				}

				marshal, err := proto.Marshal(data)
				if err != nil {
					logrus.Fatalln(err)
				}

				n, err := dataWriter.Write(marshal)
				if err != nil {
					logrus.Fatalln(err)
				}

				index := &Index{
					Did:       uint32(data.Did),
					Offset:    offset,
					Length:    uint32(len(marshal)),
					Timestamp: uint64(data.Timestamp),
					Flag:      0,
				}
				indexs = append(indexs, index)

				offset += uint64(n)
			}
			_ = dataWriter.Close()
		}()

		t := time.Now().UnixNano()
		dataKey := fmt.Sprintf("%d_%d_%d_data", ShardGroupSize, msg.shardId, t)
		indexKey := fmt.Sprintf("%d_%d_%d_index", ShardGroupSize, msg.shardId, t)
		logrus.Infoln("dump", dataKey)

		err = c.dataDisk.WriteStream(dataKey, dataReader, false)
		if err != nil {
			logrus.Fatalln(err)
		}

		indexReader, indexWriter, err := os.Pipe()
		if err != nil {
			logrus.Fatalln(err)
		}
		go func() {
			for _, i := range indexs {
				_, err := i.Write(indexWriter)
				if err != nil {
					logrus.Fatalln(err)
				}
			}
			_ = indexWriter.Close()
		}()
		err = c.dataDisk.WriteStream(indexKey, indexReader, false)
		if err != nil {
			logrus.Fatalln(err)
		}

	}
}

func (c *Cake) Query(did int64, start, end int64) ([]*pb.Data, error) {
	startShardId := toShardId(start)
	endShardId := toShardId(end)
	cancel := make(chan struct{})
	keys := c.dataDisk.Keys(cancel)
	for key := range keys {
		if strings.HasSuffix(key, "index") {
			meta := strings.Split(key, "_")
			shardId := conv.ToAny[int64](meta[1])
			if shardId < startShardId || shardId > endShardId {
				continue
			}
			readStream, err := c.dataDisk.ReadStream(key, false)
			if err != nil {
				return nil, err
			}
			var indexs []*Index
			for {
				index := &Index{}
				err := index.Read(readStream)
				if err != nil {
					break
				}
				indexs = append(indexs, index)
			}

			l := sort.Search(len(indexs), func(i int) bool {
				if int64(indexs[i].Did) != did {
					return int64(indexs[i].Did) >= did
				}
				return int64(indexs[i].Timestamp) >= start
			})
			r := sort.Search(len(indexs), func(i int) bool {
				if int64(indexs[i].Did) != did {
					return int64(indexs[i].Did) >= did
				}
				return int64(indexs[i].Timestamp) >= end
			})
			indexs = indexs[l:r]
			for _, i := range indexs {
				logrus.Infoln("index", i)
			}
			stream, err := c.dataDisk.ReadStream(key[:len(key)-5]+"data", false)
			if err != nil {
				return nil, err
			}
			if len(indexs) > 0 {
				_, err = io.CopyN(io.Discard, stream, int64(indexs[0].Offset))
				if err != nil {
					return nil, err
				}
				for i := 0; i < len(indexs); i++ {
					body := make([]byte, indexs[i].Length)
					_, err := stream.Read(body)
					if err != nil {
						return nil, err
					}
					var v pb.Data
					proto.Unmarshal(body, &v)
					logrus.Infoln("data:", v)
				}
			}

		}
	}
	return nil, nil
}

//func (c *Cake) queryFromMemory(did int64, start, end int64) ([]*pb.Data, error) {
//	c.walMutex.RLock()
//	defer c.walMutex.RUnlock()
//	between, err := c.shardGroup.IteratorBetween(&pb.Data{
//		Did:       did,
//		Timestamp: start,
//		CreatedAt: 0,
//		Value:     nil,
//	}, &pb.Data{
//		Did:       did,
//		Timestamp: end,
//		CreatedAt: 0,
//		Value:     nil,
//	})
//	if err != nil {
//		return nil, err
//	}
//	var ret []*pb.Data
//	for {
//		k, _, err := between.Next()
//		if err != nil {
//			break
//		}
//		ret = append(ret, k)
//	}
//	return ret, nil
//}
//
//func (c *Cake) Query(did int64, start, end int64) ([]int32, []*pb.Data, error) {
//	c.queryMutex.Lock()
//	defer c.queryMutex.Unlock()
//
//	if !c.fieldDisk.Has(conv.ToAny[string](did)) {
//		return nil, nil, nil
//	}
//
//	// query disk
//	files := c.openFiles(start, end)
//	var values []*pb.Data
//	var key []int32
//	var mutex sync.Mutex
//	for _, i := range files {
//		i := i
//		func() {
//			file, err := OpenFile(c.Optional.DataFilePath, i)
//			if err != nil {
//				logrus.Fatalln(err)
//			}
//			defer func(file *File) {
//				err := file.close()
//				if err != nil {
//					logrus.Fatalln(err)
//				}
//			}(file)
//
//			indexHeader, err := c.readIndexHeader(file.SecondIndexFile)
//			if err != nil {
//				logrus.Fatalln("readIndexHeader", err)
//			}
//
//			idx := sort.Search(len(indexHeader.Index), func(i int) bool {
//				return indexHeader.Index[i].DeviceId >= did
//			})
//
//			if idx >= len(indexHeader.Index) {
//				return
//			}
//
//			//logrus.Infoln(indexHeader.Index[idx], "idx:", idx, len(indexHeader.Index))
//
//			if indexHeader.Index[idx].Start > end || indexHeader.Index[idx].End < start {
//				return
//			}
//
//			l := indexHeader.Index[idx].Offset
//			_, err = file.FirstIndexFile.Seek(l*datastructure.FirstIndexMetaSize, io.SeekCurrent)
//			if err != nil {
//				return
//			}
//			var index []*datastructure.FirstIndexMeta
//
//			if idx == len(indexHeader.Index)-1 {
//				for {
//					v := datastructure.FirstIndexMeta{}
//					err := v.ReadFirstIndexMeta(file.FirstIndexFile)
//					if err != nil {
//						break
//					}
//					index = append(index, &v)
//				}
//			} else {
//				r := indexHeader.Index[idx+1].Offset
//				for i := 0; i < int(r-l); i++ {
//					v := datastructure.FirstIndexMeta{}
//					err := v.ReadFirstIndexMeta(file.FirstIndexFile)
//					if err != nil {
//						break
//					}
//					index = append(index, &v)
//				}
//			}
//
//			startIdx := sort.Search(len(index), func(i int) bool {
//				return index[i].Timestamp >= start
//			})
//
//			endIdx := sort.Search(len(index), func(i int) bool {
//				return index[i].Timestamp > end
//			})
//
//			_, err = file.DataFile.Seek(int64(index[startIdx].Offset), io.SeekStart)
//			if err != nil {
//				logrus.Fatalln(err)
//			}
//
//			for i := startIdx; i < endIdx; i++ {
//				var length uint32
//				err := binary.Read(file.DataFile, binary.BigEndian, &length)
//				if err != nil {
//					logrus.Fatalln(err)
//
//				}
//				body := make([]byte, length)
//				_, err = file.DataFile.Read(body)
//				if err != nil {
//					logrus.Fatalln(err)
//
//				}
//
//				v := pb.Data{}
//				err = proto.Unmarshal(body, &v)
//				if err != nil {
//					logrus.Fatalln(err)
//
//				}
//				mutex.Lock()
//				values = append(values, &v)
//				mutex.Unlock()
//
//			}
//
//		}()
//	}
//
//	c.closeFiles(files)
//
//	// read from memory
//	fromMemory, err := c.queryFromMemory(did, start, end)
//	if err != nil {
//		return nil, nil, err
//	}
//	values = append(values, fromMemory...)
//
//	if len(values) > 0 {
//		// read key
//		read, err := c.fieldDisk.Read(conv.ToAny[string](did))
//		if err != nil {
//			return nil, nil, err
//		}
//		key = make([]int32, len(read)/4)
//		err = binary.Read(bytes.NewReader(read), binary.BigEndian, key)
//		if err != nil {
//			return nil, nil, err
//		}
//
//	}
//
//	return key, values, nil
//}
//
//func (c *Cake) QueryLatest(did int64) ([]int32, []int64, error) {
//
//	return nil, nil, nil
//}
//
//func (c *Cake) delWal(did int64, start, end int64) error {
//	c.walMutex.Lock()
//	defer c.walMutex.Unlock()
//
//	// write wal del command
//	lastIndex, err := c.cmdLog.LastIndex()
//	if err != nil {
//		return err
//	}
//
//	removeRequest := &pb.Request_RemoveRequest{
//		RemoveRequest: &pb.RemoveRequest{Did: did,
//			Start:   start,
//			End:     end,
//			Created: time.Now().UnixNano(),
//		},
//	}
//
//	request := pb.Request{Command: removeRequest}
//	marshal, err := proto.Marshal(&request)
//	if err != nil {
//		return err
//	}
//
//	err = c.cmdLog.Write(lastIndex+1, marshal)
//	if err != nil {
//		return err
//	}
//	return nil
//}
//
//func (c *Cake) delMemory(did int64, start, end int64) error {
//	c.walMutex.Lock()
//	defer c.walMutex.Unlock()
//
//	// del memory
//	between, err := c.shardGroup.IteratorBetween(&pb.Data{
//		Did:       did,
//		Timestamp: start,
//		CreatedAt: 0,
//		Value:     nil,
//	}, &pb.Data{
//		Did:       did,
//		Timestamp: end,
//		CreatedAt: 0,
//		Value:     nil,
//	})
//	if err != nil {
//		return err
//	}
//
//	for {
//		k, _, err := between.Next()
//		if err != nil {
//			break
//		}
//		c.shardGroup.Delete(k)
//	}
//	return nil
//}
//
//func (c *Cake) Delete(did int64, start, end int64) error {
//
//	err := c.delWal(did, start, end)
//	if err != nil {
//		return err
//	}
//
//	err = c.delMemory(did, start, end)
//	if err != nil {
//		return err
//	}
//
//	// del disk
//
//	return nil
//}
