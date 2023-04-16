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
	"github.com/tidwall/wal"
	"io"
	"iot-db/internal/datastructure"
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

type Engine interface {
	Insert(did int64, timestamp int64, created int, data map[int64]int64) error
	Query(did int64, start, end int64) (map[int64]int64, error)
	Delete(did int64, start, end int64) error
}

type Item struct {
	*shardgroup.DeviceSkipList
	LatestIndexId int
}

type ShardGroup map[int64]*Item

type Cake struct {
	size           int
	shardGroup     skiplist.MapI[*pb.Data, struct{}]
	noiseGroup     skiplist.MapI[*pb.Data, struct{}]
	shardGroupChan chan dump
	dataLog        *wal.Log
	cmdLog         *wal.Log
	walMutex       sync.RWMutex
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

	flatTransform := func(s string) []string { return []string{} }

	fieldDisk := diskv.New(diskv.Options{
		BasePath:     op.FieldCachePath,
		Transform:    flatTransform,
		CacheSizeMax: 1024 * 1024 * 20,
	})

	dataDisk := diskv.New(diskv.Options{
		BasePath: op.DataCachePath,
		Transform: func(s string) []string {
			return strings.Split(s, "_")[0:1]
		},
		CacheSizeMax: 1024 * 1024 * 20,
	})

	log, err := wal.Open(op.WalPath, &wal.Options{
		NoSync:           false,      // Fsync after every write
		SegmentSize:      20971520,   // 20 MB log segment files.
		LogFormat:        wal.Binary, // Binary format is small and fast.
		SegmentCacheSize: 2,          // Number of cached in-memory segments
		NoCopy:           false,      // Make a new copy of data for every Read call.
		DirPerms:         0750,       // Permissions for the created directories
		FilePerms:        0640,       // Permissions for the created data files
	})
	if err != nil {
		return nil
	}

	cmdLog, err := wal.Open(op.CmdWalPath, &wal.Options{
		NoSync:           false,      // Fsync after every write
		SegmentSize:      20971520,   // 20 MB log segment files.
		LogFormat:        wal.Binary, // Binary format is small and fast.
		SegmentCacheSize: 2,          // Number of cached in-memory segments
		NoCopy:           false,      // Make a new copy of data for every Read call.
		DirPerms:         0750,       // Permissions for the created directories
		FilePerms:        0640,       // Permissions for the created data files
	})
	if err != nil {
		return nil
	}

	obj := &Cake{
		size:           0,
		shardGroup:     newShardGroup(),
		noiseGroup:     newShardGroup(),
		shardGroupChan: make(chan dump, 10),
		dataLog:        log,
		cmdLog:         cmdLog,
		walMutex:       sync.RWMutex{},
		fileMutex:      sync.RWMutex{},
		fieldDisk:      fieldDisk,
		dataDisk:       dataDisk,
		fileManager:    nil,
		Optional:       op,
		files:          map[string]int{},
		noise:          0,
	}

	go obj.dump()

	err = obj.init()
	if err != nil {
		logrus.Fatalln(err)
	}

	go obj.compact()

	return obj
}

func (c *Cake) init() error {

	// wal read
	firstIndex, err := c.dataLog.FirstIndex()
	if err != nil {
		return err
	}
	lastIndex, err := c.dataLog.LastIndex()
	if err != nil {
		return err
	}

	logrus.Infoln("firstIndex:", firstIndex, "lastIndex", lastIndex)

	if firstIndex < lastIndex {
		for i := firstIndex; i <= lastIndex; i++ {
			body, err := c.dataLog.Read(i)
			if err != nil {
				logrus.Errorln(err)
				return err
			}
			var v pb.FullData
			err = proto.Unmarshal(body, &v)
			if err != nil {
				logrus.Errorln(err)
				return err
			}
			err = c.insertMemory([]*pb.FullData{&v}, len(body), i+1)
			if err != nil {
				logrus.Errorln(err)
				return err
			}
		}
	}

	dir, err := os.ReadDir(c.Optional.DataFilePath)
	if err != nil {
		logrus.Infoln("ReadDir", err)
		return err
	}

	for _, i := range dir {
		if strings.HasSuffix(i.Name(), ".data") {
			c.files[i.Name()[:len(i.Name())-5]] = 0

		}
	}

	return nil

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

func (c *Cake) wal(data [][]byte) (uint64, error) {
	c.walMutex.Lock()
	defer c.walMutex.Unlock()

	lastIndex, err := c.dataLog.LastIndex()
	if err != nil {
		return 0, err
	}

	logrus.Infoln(lastIndex, "lastIndex")

	batch := wal.Batch{}
	for idx, i := range data {
		batch.Write(lastIndex+uint64(idx+1), i)
	}
	err = c.dataLog.WriteBatch(&batch)
	if err != nil {
		logrus.Infoln(err, "WriteBatch")
		return 0, err
	}
	return lastIndex + uint64(len(data)), nil
}

func (c *Cake) Insert(data []*pb.FullData) error {

	var size int
	// wal
	var walData [][]byte
	for _, i := range data {
		marshal, err := proto.Marshal(i)
		if err != nil {
			return err
		}
		walData = append(walData, marshal)
		size += len(marshal)
	}

	lastIndex, err := c.wal(walData)
	if err != nil {
		return err
	}

	return c.insertMemory(data, size, lastIndex)
}

func (c *Cake) insertMemory(data []*pb.FullData, size int, lastIndex uint64) error {
	for _, data := range data {
		//  write field

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

		if data.Timestamp == 0 {
			continue
		}

		if c.noise == 0 {
			c.noise += uint64(data.Timestamp)
		} else {
			c.noise = uint64(float64(c.noise)*0.8 + float64(data.Timestamp)*0.2)
		}

		if math.Abs(time.UnixMilli((int64(c.noise))/1e6).Sub(time.UnixMilli((int64(data.Timestamp))/1e6)).Hours()) > 24*7 {
			// write disk cache
			buf := bytes.NewBuffer([]byte{})
			err := binary.Write(buf, binary.BigEndian, data.Value)
			if err != nil {
				return err
			}
			err = c.dataDisk.Write(fmt.Sprintf("%d_%d_%d", data.Did, data.Timestamp, data.CreatedAt), buf.Bytes())
			if err != nil {
				return err
			}

		} else {
			// write value to memory
			c.shardGroup.Insert(&pb.Data{
				Did:       data.Did,
				Timestamp: data.Timestamp,
				CreatedAt: data.CreatedAt,
				Value:     data.Value,
			}, struct{}{})
		}

	}

	c.size += size
	// 20 MB
	if c.size >= 20971520 {
		//logrus.Infoln("avg time", time.UnixMilli(int64(c.noise)/1e6))
		//if c.noiseGroup.Size() > 0 {
		//	c.shardGroupChan <- dump{
		//		shardGroup: c.noiseGroup,
		//		lastIndex:  0,
		//	}
		//	c.noiseGroup = newShardGroup()
		//}
		c.size = 0
		c.shardGroupChan <- dump{
			shardGroup: c.shardGroup,
			lastIndex:  lastIndex,
		}
		c.shardGroup = newShardGroup()
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
		lastIndex := msg.lastIndex

		start, end := c.getShardGroupMeta(shardGroup)
		fileName := fmt.Sprintf("%d_%d_%d", start, end, time.Now().UnixNano())

		file, err := c.createTempFile(fileName)
		if err != nil {
			logrus.Fatalln(err)
		}

		c.writeIndexHeader(shardGroup, file)

		//  dump
		w, err := NewWriter(file)
		if err != nil {
			logrus.Fatalln(err)
		}

		it, err := shardGroup.Iterator()
		if err != nil {
			return
		}

		for {
			k, _, err := it.Next()
			if err != nil {
				break
			}

			err = w.WriteData(k)
			if err != nil {
				logrus.Fatalln(err)
			}

			err = w.WriteFirstIndex()
			if err != nil {
				logrus.Fatalln(err)
			}

		}

		logrus.Infoln("dump file:", file.Name)
		err = c.saveTempFile(file)
		if err != nil {
			logrus.Fatalln(err)
		}

		if lastIndex != 0 {
			err = c.dataLog.TruncateFront(lastIndex)
			if err != nil {
				logrus.Fatalln(err)
			}
		}

	}
}

func (c *Cake) queryFromMemory(did int64, start, end int64) ([]*pb.Data, error) {
	c.walMutex.RLock()
	defer c.walMutex.RUnlock()
	between, err := c.shardGroup.IteratorBetween(&pb.Data{
		Did:       did,
		Timestamp: start,
		CreatedAt: 0,
		Value:     nil,
	}, &pb.Data{
		Did:       did,
		Timestamp: end,
		CreatedAt: 0,
		Value:     nil,
	})
	if err != nil {
		return nil, err
	}
	var ret []*pb.Data
	for {
		k, _, err := between.Next()
		if err != nil {
			break
		}
		ret = append(ret, k)
	}
	return ret, nil
}

func (c *Cake) Query(did int64, start, end int64) ([]int32, []*pb.Data, error) {
	c.queryMutex.Lock()
	defer c.queryMutex.Unlock()

	if !c.fieldDisk.Has(conv.ToAny[string](did)) {
		return nil, nil, nil
	}

	// query disk
	files := c.openFiles(start, end)
	var values []*pb.Data
	var key []int32
	var mutex sync.Mutex
	for _, i := range files {
		i := i
		func() {
			file, err := OpenFile(c.Optional.DataFilePath, i)
			if err != nil {
				logrus.Fatalln(err)
			}
			defer func(file *File) {
				err := file.close()
				if err != nil {
					logrus.Fatalln(err)
				}
			}(file)

			indexHeader, err := c.readIndexHeader(file.SecondIndexFile)
			if err != nil {
				logrus.Fatalln("readIndexHeader", err)
			}

			idx := sort.Search(len(indexHeader.Index), func(i int) bool {
				return indexHeader.Index[i].DeviceId >= did
			})

			if idx >= len(indexHeader.Index) {
				return
			}

			//logrus.Infoln(indexHeader.Index[idx], "idx:", idx, len(indexHeader.Index))

			if indexHeader.Index[idx].Start > end || indexHeader.Index[idx].End < start {
				return
			}

			l := indexHeader.Index[idx].Offset
			_, err = file.FirstIndexFile.Seek(l*datastructure.FirstIndexMetaSize, io.SeekCurrent)
			if err != nil {
				return
			}
			var index []*datastructure.FirstIndexMeta

			if idx == len(indexHeader.Index)-1 {
				for {
					v := datastructure.FirstIndexMeta{}
					err := v.ReadFirstIndexMeta(file.FirstIndexFile)
					if err != nil {
						break
					}
					index = append(index, &v)
				}
			} else {
				r := indexHeader.Index[idx+1].Offset
				for i := 0; i < int(r-l); i++ {
					v := datastructure.FirstIndexMeta{}
					err := v.ReadFirstIndexMeta(file.FirstIndexFile)
					if err != nil {
						break
					}
					index = append(index, &v)
				}
			}

			startIdx := sort.Search(len(index), func(i int) bool {
				return index[i].Timestamp >= start
			})

			endIdx := sort.Search(len(index), func(i int) bool {
				return index[i].Timestamp > end
			})

			_, err = file.DataFile.Seek(int64(index[startIdx].Offset), io.SeekStart)
			if err != nil {
				logrus.Fatalln(err)
			}

			for i := startIdx; i < endIdx; i++ {
				var length uint32
				err := binary.Read(file.DataFile, binary.BigEndian, &length)
				if err != nil {
					logrus.Fatalln(err)

				}
				body := make([]byte, length)
				_, err = file.DataFile.Read(body)
				if err != nil {
					logrus.Fatalln(err)

				}

				v := pb.Data{}
				err = proto.Unmarshal(body, &v)
				if err != nil {
					logrus.Fatalln(err)

				}
				mutex.Lock()
				values = append(values, &v)
				mutex.Unlock()

			}

		}()
	}

	c.closeFiles(files)

	// read from memory
	fromMemory, err := c.queryFromMemory(did, start, end)
	if err != nil {
		return nil, nil, err
	}
	values = append(values, fromMemory...)

	if len(values) > 0 {
		// read key
		read, err := c.fieldDisk.Read(conv.ToAny[string](did))
		if err != nil {
			return nil, nil, err
		}
		key = make([]int32, len(read)/4)
		err = binary.Read(bytes.NewReader(read), binary.BigEndian, key)
		if err != nil {
			return nil, nil, err
		}

	}

	return key, values, nil
}

func (c *Cake) QueryLatest(did int64) ([]int32, []int64, error) {

	return nil, nil, nil
}

func (c *Cake) delWal(did int64, start, end int64) error {
	c.walMutex.Lock()
	defer c.walMutex.Unlock()

	// write wal del command
	lastIndex, err := c.cmdLog.LastIndex()
	if err != nil {
		return err
	}

	removeRequest := &pb.Request_RemoveRequest{
		RemoveRequest: &pb.RemoveRequest{Did: did,
			Start:   start,
			End:     end,
			Created: time.Now().UnixNano(),
		},
	}

	request := pb.Request{Command: removeRequest}
	marshal, err := proto.Marshal(&request)
	if err != nil {
		return err
	}

	err = c.cmdLog.Write(lastIndex+1, marshal)
	if err != nil {
		return err
	}
	return nil
}

func (c *Cake) delMemory(did int64, start, end int64) error {
	c.walMutex.Lock()
	defer c.walMutex.Unlock()

	// del memory
	between, err := c.shardGroup.IteratorBetween(&pb.Data{
		Did:       did,
		Timestamp: start,
		CreatedAt: 0,
		Value:     nil,
	}, &pb.Data{
		Did:       did,
		Timestamp: end,
		CreatedAt: 0,
		Value:     nil,
	})
	if err != nil {
		return err
	}

	for {
		k, _, err := between.Next()
		if err != nil {
			break
		}
		c.shardGroup.Delete(k)
	}
	return nil
}

func (c *Cake) Delete(did int64, start, end int64) error {

	err := c.delWal(did, start, end)
	if err != nil {
		return err
	}

	err = c.delMemory(did, start, end)
	if err != nil {
		return err
	}

	// del disk

	return nil
}
