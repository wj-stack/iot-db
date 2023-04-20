package engine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/dablelv/go-huge-util/conv"
	"github.com/gammazero/workerpool"
	"github.com/golang/protobuf/proto"
	"github.com/peterbourgon/diskv/v3"
	"github.com/sirupsen/logrus"
	"io"
	"iot-db/internal/pb"
	"iot-db/internal/shardgroup"
	"iot-db/pkg/skiplist"
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
	Optional       *Optional
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

func Transform(s string) []string {
	// ShardSize_ShardId_createdAt_(data/index/sIndex)
	ret := strings.Split(s, "_")
	return ret[0 : len(ret)-2]
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
	fieldDisk := diskv.New(diskv.Options{
		BasePath: op.FieldCachePath,
		Transform: func(s string) []string {
			if len(s) < 2 {
				s = strings.Repeat("0", 2-len(s)) + s
			}
			return []string{s[:2]}
		},
		CacheSizeMax: 1024 * 1024 * 200,
	})

	// 存储寄存器值信息
	dataDisk := diskv.New(diskv.Options{
		BasePath:     op.DataCachePath,
		Transform:    Transform,
		CacheSizeMax: 0,
	})

	obj := &Cake{
		size:           0,
		shardGroup:     map[int64]skiplist.MapI[*pb.Data, struct{}]{},
		shardGroupChan: make(chan dump, 10),
		fieldDisk:      fieldDisk,
		dataDisk:       dataDisk,
		Optional:       op,
	}

	go obj.dump()
	//go func() {
	//	for {
	//		obj.Compact()
	//		time.Sleep(time.Second * 10)
	//	}
	//}()
	return obj
}

func (c *Cake) Insert(data []*pb.FullData) error {

	err := c.writeKey(data)
	if err != nil {
		return err
	}
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
				delete(c.shardGroup, shardId)
			}
		}
	}

	return nil
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

func (c *Cake) compact(shardId int64, keys []string) {
	dataStreams := map[string]io.ReadCloser{}

	var lastDid uint32
	var lastTimestamp uint64
	created := time.Now().UnixNano()
	dataKey := fmt.Sprintf("%d_%d_%d_data", ShardGroupSize, shardId, created)
	indexKey := fmt.Sprintf("%d_%d_%d_index", ShardGroupSize, shardId, created)

	indexFile, err := os.CreateTemp("", indexKey)
	if err != nil {
		panic(err)
	}
	defer func() {
		os.Remove(indexFile.Name())
	}()

	dataFile, err := os.CreateTemp("", dataKey)
	if err != nil {
		panic(err)
	}
	defer func() {
		os.Remove(dataFile.Name())
	}()

	var pipelines []chan IndexAndKey
	for _, key := range keys {
		dataKey := key[:len(key)-5] + "data"
		_, ok := dataStreams[dataKey]
		if !ok {
			var err error
			dataStreams[dataKey], err = c.dataDisk.ReadStream(dataKey, false)
			if err != nil {
				panic(err)
			}
		}
		pipelines = append(pipelines, c.openIndexPipeline(key))
	}

	indexChan := MergeN(pipelines...)
	logrus.Infoln("len(pipelines):", len(pipelines))
	cnt := 0
	//defer pipeWriter.Close()
	defer indexFile.Close()
	defer dataFile.Close()
	defer func() {
		for _, v := range dataStreams {
			_ = v.Close()
		}
	}()
	size := 0
	t := time.Now()

	buf := make([]byte, 1024*5)
	for index := range indexChan {
		cnt++
		if int(index.Length) > len(buf) {
			buf = make([]byte, index.Length*2)
		}
		n, err := dataStreams[index.DataKey].Read(buf[:index.Length])
		if err != nil {
			panic(err)
		}

		if index.Timestamp != lastTimestamp || index.Did != lastDid {
			lastTimestamp = index.Timestamp
			lastDid = index.Did
			_, err := dataFile.Write(buf[:index.Length])
			if err != nil {
				panic(err)
			}
			index := &Index{
				Did:       index.Did,
				Offset:    uint64(size),
				Length:    uint32(n),
				Timestamp: index.Timestamp,
				Flag:      index.Flag,
			}
			_, err = index.Write(indexFile)
			if err != nil {
				panic(err)
			}
			size += int(index.Length)
		}

	}
	logrus.Infoln("write ok..", shardId, time.Now().UnixMilli()-t.UnixMilli())

	logrus.Infoln("close index chan...")
	logrus.Infoln("start compact..", shardId)
	t = time.Now()
	err = c.dataDisk.Import(dataFile.Name(), dataKey, true)
	if err != nil {
		panic(err)
	}
	// write index
	err = c.dataDisk.Import(indexFile.Name(), indexKey, true)
	if err != nil {
		panic(err)
	}
	logrus.Infoln("compact ok..", shardId, time.Now().UnixMilli()-t.UnixMilli())

	for _, indexKey := range keys {
		dataKey := indexKey[:len(indexKey)-5] + "data"
		logrus.Infoln("erase", dataKey)
		err := c.dataDisk.Erase(dataKey)
		if err != nil {
			panic(err)
		}
		err = c.dataDisk.Erase(indexKey)
		if err != nil {
			panic(err)
		}

	}

}

type IndexAndKey struct {
	Index
	//DataStream io.ReadCloser
	DataKey   string
	CreatedAt int64
	ShardId   int64
}

func (c *Cake) Compact() {
	cancel := make(chan struct{})
	keys := c.dataDisk.Keys(cancel)

	shardKey := map[int64][]string{}
	for key := range keys {
		if strings.HasSuffix(key, "index") {
			meta := strings.Split(key, "_")
			shardId := conv.ToAny[int64](meta[1])
			shardKey[shardId] = append(shardKey[shardId], key)
		}
	}

	for shardId, keys := range shardKey {
		type KeyAndSize struct {
			Key  string
			Size int64
		}
		var keyAndSize []KeyAndSize
		for _, key := range keys {
			path := c.Optional.DataCachePath + "/" + strings.Join(Transform(key), "/") + "/" + key
			stat, err := os.Stat(path)
			if err != nil {
				continue
			}

			// meta := strings.Split(key, "_")
			// created := conv.ToAny[int64](meta[2])

			// index 100MB
			if stat.Size() < 100*1e6 {
				keyAndSize = append(keyAndSize, KeyAndSize{Size: stat.Size(), Key: key})
			}
		}
		sort.Slice(keyAndSize, func(i, j int) bool {
			return keyAndSize[i].Size < keyAndSize[j].Size
		})
		var sortKeys []string
		for _, i := range keyAndSize {
			sortKeys = append(sortKeys, i.Key)
		}
		if len(sortKeys) > 10 {
			logrus.Infoln("compact:", sortKeys)
			c.compact(shardId, sortKeys)
		}
	}

}

func (c *Cake) Query(did int64, start, end int64) (chan *pb.Data, error) {
	startShardId := toShardId(start)
	endShardId := toShardId(end)
	cancel := make(chan struct{})
	keys := c.dataDisk.Keys(cancel)
	dataChan := make(chan *pb.Data, 1000)
	wp := workerpool.New(200)
	for key := range keys {
		if strings.HasSuffix(key, "index") {
			key := key
			wp.Submit(func() {
				//logrus.Infoln("query", key)
				meta := strings.Split(key, "_")
				shardId := conv.ToAny[int64](meta[1])
				if shardId < startShardId || shardId > endShardId {
					return
				}

				readStream, err := c.dataDisk.Read(key)
				if err != nil {
					return
				}

				//t := time.Now()

				var indexs []*Index

				l := sort.Search(len(readStream)/IndexSize, func(i int) bool {

					reader := bytes.NewReader(readStream[i*IndexSize : (i+1)*IndexSize])

					index := &Index{}
					err = index.Read(reader)
					if err != nil {
						return false
					}

					if int64(index.Did) != did {
						return int64(index.Did) >= did
					}
					return int64(index.Timestamp) >= start
				})
				r := sort.Search(len(readStream)/IndexSize, func(i int) bool {

					reader := bytes.NewReader(readStream[i*IndexSize : (i+1)*IndexSize])
					index := &Index{}
					err = index.Read(reader)
					if err != nil {
						return false
					}

					if int64(index.Did) != did {
						return int64(index.Did) > did
					}
					return int64(index.Timestamp) > end
				})

				reader := bytes.NewReader(readStream[l*IndexSize : (r)*IndexSize])
				for i := l; i < r; i++ {
					index := &Index{}
					err = index.Read(reader)
					if err != nil {
						break
					}
					indexs = append(indexs, index)
				}
				stream, err := c.dataDisk.ReadStream(key[:len(key)-5]+"data", false)
				if err != nil {
					return
				}

				func() {
					defer stream.Close()
					if len(indexs) > 0 {
						_, err = io.CopyN(io.Discard, stream, int64(indexs[0].Offset))
						if err != nil {
							return
						}
						for i := 0; i < len(indexs); i++ {
							body := make([]byte, indexs[i].Length)
							_, err := stream.Read(body)
							if err != nil {
								return
							}
							var v pb.Data
							err = proto.Unmarshal(body, &v)
							if err != nil {
								return
							}
							dataChan <- &v
						}
					}
				}()
			})
		}
	}
	go func() {
		wp.StopWait()
		close(dataChan)
	}()
	return dataChan, nil
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
