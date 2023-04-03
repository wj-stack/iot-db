package memorytable

import (
	"fmt"
	"iot-db/internal/util"
	"sync"
	"time"
)

type ShardGroupId int64

func TimeConvertShardId(t time.Time, shardGroupSize int64) ShardGroupId {
	return ShardGroupId(t.UnixNano() / shardGroupSize)
}

func TimestampConvertShardId(t int64, shardGroupSize int64) ShardGroupId {
	return ShardGroupId(t / shardGroupSize)
}

type ShardGroupItem struct {
	*DeviceList
	UpdatedAt time.Time
}

type ShardGroupMap map[ShardGroupId]*ShardGroupItem

type ShardGroup struct {
	IMap           ShardGroupMap
	ItemChan       chan *ShardGroupItem
	ShardGroupSize int64
	FragmentSize   int64
	WorkPath       string
	mutex          sync.RWMutex
}

func NewShardGroup() *ShardGroup {
	var obj = &ShardGroup{
		IMap:           ShardGroupMap{},
		ItemChan:       make(chan *ShardGroupItem, 10),
		ShardGroupSize: int64(time.Hour),
		FragmentSize:   10e6,
		WorkPath:       "/home/wyatt/code/iot-db/data",
	}
	go obj.loopDump()
	go obj.timeoutDump()
	return obj
}

func (s *ShardGroup) Insert(deviceId int64, timestamp int64, body []byte) {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	shardGroupId := TimestampConvertShardId(timestamp, s.ShardGroupSize)
	item, ok := s.IMap[shardGroupId]
	if !ok {
		s.IMap[shardGroupId] = &ShardGroupItem{
			DeviceList: NewDeviceList(),
		}
		item = s.IMap[shardGroupId]
	}

	item.UpdatedAt = time.Now()

	item.Insert(deviceId, &Data{
		Length:    int32(len(body)),
		Flag:      0,
		Timestamp: timestamp,
		CreatedAt: item.UpdatedAt.UnixNano(),
		Body:      body,
	})

	// go dump
	if item.size > s.FragmentSize {
		s.ItemChan <- item
		delete(s.IMap, shardGroupId)
	}

}

func (s *ShardGroup) timeoutDump() {
	for {
		s.mutex.Lock()
		for k, v := range s.IMap {
			if v.UpdatedAt.Add(time.Minute * 30).Before(time.Now()) {
				go s.dump(v)
				delete(s.IMap, k)
			}
		}
		s.mutex.Unlock()
		time.Sleep(time.Minute * 30)
	}
}

func (s *ShardGroup) loopDump() {
	for item := range s.ItemChan {
		go s.dump(item)
	}
}

func (s *ShardGroup) dump(item *ShardGroupItem) {
	shardGroupId := TimestampConvertShardId(item.DeviceList.startTime, s.ShardGroupSize)
	fmt.Println("dump:", "ShardGroupSize:", s.ShardGroupSize, "shardGroupId:", shardGroupId,
		"fileSize：", item.size, "deviceCount:", item.count,
		"start:", item.startTime, "end:", item.endTime)
	firstIndex, secondFile, dataFile, name, err := util.CreateTempFile(s.WorkPath, int(s.ShardGroupSize), int(shardGroupId))
	if err != nil {
		panic(err)
	}

	err = item.DeviceList.dump(dataFile, firstIndex, secondFile)
	if err != nil {
		panic(err)
	}

	err = util.ReTempName(s.WorkPath, int(s.ShardGroupSize), int(shardGroupId), name)
	if err != nil {
		panic(err)
	}

}
