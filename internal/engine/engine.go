package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/allegro/bigcache/v3"
	"github.com/dablelv/go-huge-util/conv"
	"github.com/golang/protobuf/proto"
	"github.com/patrickmn/go-cache"
	"github.com/peterbourgon/diskv/v3"
	"github.com/sirupsen/logrus"
	"io"
	"iot-db/internal/pb"
	"iot-db/pkg/skiplist"
	"os"
	"strings"
	"sync"
	"time"
)

type Cake struct {
	size           int
	shardGroup     map[int64]skiplist.MapI[*pb.Data, struct{}]
	shardGroupChan chan dump
	fieldDisk      *diskv.Diskv // key
	dataDisk       *diskv.Diskv // value
	Keys           map[string]struct{}
	Cache          *cache.Cache
	BigCache       *bigcache.BigCache
	mu             sync.RWMutex
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
}

func newShardGroup() skiplist.MapI[*pb.Data, struct{}] {
	return skiplist.NewSkipListMap[*pb.Data, struct{}](&DataCompare{})
}

func NewDefaultEngine() *Cake {
	return NewEngine(&Optional{
		FieldCachePath: "/data/iot-db/data/field",
		DataCachePath:  "/data/iot-db/data/data",
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

	// 存储寄存器信息
	fieldDisk := diskv.New(diskv.Options{
		BasePath: op.FieldCachePath,
		Transform: func(s string) []string {
			if len(s) < 2 {
				s = strings.Repeat("0", 2-len(s)) + s
			}
			return []string{s[:2]}
		},
		CacheSizeMax: 1024 * 1024 * 20,
	})

	// 存储寄存器值信息
	dataDisk := diskv.New(diskv.Options{
		BasePath:     op.DataCachePath,
		Transform:    Transform,
		CacheSizeMax: 0,
	})

	c := cache.New(time.Hour, time.Minute) // save mmap fd

	bc, err := bigcache.New(context.Background(), bigcache.DefaultConfig(10*time.Minute))
	if err != nil {
		panic(err)
	}

	obj := &Cake{
		size:           0,
		shardGroup:     map[int64]skiplist.MapI[*pb.Data, struct{}]{},
		shardGroupChan: make(chan dump, 10),
		fieldDisk:      fieldDisk,
		dataDisk:       dataDisk,
		Keys:           map[string]struct{}{},
		Cache:          c,
		BigCache:       bc,
		mu:             sync.RWMutex{},
		Optional:       op,
	}

	obj.InitKey()
	go obj.dump()

	return obj
}
func (c *Cake) Size() int {
	return c.size
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

	// ~= 100 M
	if c.size > 1e6*200 {
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

		t := time.Now().UnixNano()
		dataKey := fmt.Sprintf("%d_%d_%d_data", ShardGroupSize, msg.shardId, t)
		indexKey := fmt.Sprintf("%d_%d_%d_index", ShardGroupSize, msg.shardId, t)

		c.AddFile(indexKey, dataKey, func(dataFile, indexFile io.Writer) {
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
					panic(err)
				}

				n, err := dataFile.Write(marshal)
				if err != nil {
					panic(err)
				}
				index := &Index{
					Did:       uint32(data.Did),
					Offset:    offset,
					Length:    uint32(len(marshal)),
					Timestamp: uint64(data.Timestamp),
					Flag:      0,
				}
				_, err = index.Write(indexFile)
				if err != nil {
					panic(err)
				}
				offset += uint64(n)
			}
		})
		logrus.Infoln("dump", indexKey, dataKey)
	}
}

type IndexAndKey struct {
	Index
	DataKey   string
	CreatedAt int64
	ShardId   int64
}
