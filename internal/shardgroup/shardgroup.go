package shardgroup

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/wal"
	"iot-db/internal/datastructure"
	"iot-db/internal/filemanager"
	"sync"
	"time"
)

func TimeConvertShardId(t time.Time, shardGroupSize int64) int64 {
	return int64(t.UnixNano() / shardGroupSize)
}

func TimestampConvertShardId(t int64, shardGroupSize int64) int64 {
	return int64(t / shardGroupSize)
}

type Item struct {
	*DeviceSkipList
	UpdatedAt   time.Time
	LatestWalId int
}

type Map map[int64]*Item

type ShardGroup struct {
	IMap           Map
	ItemChan       chan *Item
	ShardGroupSize int64
	FragmentSize   int64
	MaxSize        int64
	CurrentSize    int64
	Wal            *wal.Log
	mutex          sync.RWMutex
	*filemanager.FileManager
}

func NewShardGroup(manager *filemanager.FileManager) *ShardGroup {
	log, err := wal.Open(manager.Config.Core.Path.Wal, nil)
	if err != nil {
		panic(err)
	}

	var obj = &ShardGroup{
		IMap:           Map{},                // map[ShardId]*Item
		ItemChan:       make(chan *Item, 10), // dump chan
		ShardGroupSize: int64(manager.Config.Core.ShardGroupSize[0]),
		FragmentSize:   100000000, // 100M
		MaxSize:        300000000, // 300M
		CurrentSize:    0,
		Wal:            log,
		mutex:          sync.RWMutex{},
		FileManager:    manager,
	}
	go obj.loopDump()
	go obj.timeoutDump()
	return obj
}

func (s *ShardGroup) Insert(deviceId int64, timestamp, createdAt int64, body []byte, privateData []byte) error {

	//insertRequest := pb.Request_InsertRequest{
	//	InsertRequest: &pb.InsertRequest{
	//		Data: &pb.Data{
	//			Did:         deviceId,
	//			Timestamp:   timestamp,
	//			CreatedAt:   createdAt,
	//			Body:        body,
	//			PrivateData: privateData,
	//		},
	//	},
	//}
	//// write wal
	//req := pb.Request{
	//	Command: &insertRequest,
	//}
	//
	//marshal, err := proto.Marshal(&req)
	//if err != nil {
	//	return err
	//}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	//lastIndex, err := s.Wal.LastIndex()
	//if err != nil {
	//	return err
	//}
	//
	//err = s.Wal.Write(lastIndex+1, marshal)
	//if err != nil {
	//	return err
	//}

	// 内存其实是只读取z
	shardGroupId := TimestampConvertShardId(timestamp, s.ShardGroupSize)
	item, ok := s.IMap[shardGroupId]
	if !ok {
		s.IMap[shardGroupId] = &Item{
			DeviceSkipList: NewDeviceSkipList(),
		}
		item = s.IMap[shardGroupId]
	}

	item.UpdatedAt = time.Now()

	item.Insert(deviceId, &datastructure.Data{
		DeviceId:  deviceId,
		Length:    int32(len(body)),
		Flag:      0,
		Timestamp: timestamp,
		CreatedAt: createdAt,
		Body:      body,
	}, privateData)

	// go dump
	if item.fileSize > s.FragmentSize {
		s.ItemChan <- item
		delete(s.IMap, shardGroupId)
		s.CurrentSize -= item.fileSize
	}

	s.CurrentSize += int64(len(body))
	if s.CurrentSize > s.MaxSize {
		for _, item := range s.IMap {
			s.ItemChan <- item
		}
		s.IMap = map[int64]*Item{}
		s.CurrentSize = 0
	}

}

func (s *ShardGroup) timeoutDump() {
	for {
		func() {
			s.mutex.Lock()
			defer s.mutex.Unlock()
			for k, v := range s.IMap {
				if v.UpdatedAt.Add(time.Minute * 1).Before(time.Now()) {
					delete(s.IMap, k)
					logrus.Infoln("timeout - dump", k, v.UpdatedAt)
					s.ItemChan <- v
				}
			}
		}()
		time.Sleep(time.Minute * 1)
	}
}

func (s *ShardGroup) Clean() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for k, v := range s.IMap {
		delete(s.IMap, k)
		logrus.Infoln("clean", k)
		s.dump(v)
		//s.ItemChan <- v
	}
}

func (s *ShardGroup) loopDump() {
	for item := range s.ItemChan {
		go s.dump(item)
	}
}

func (s *ShardGroup) dump(item *Item) {
	shardGroupId := TimestampConvertShardId(item.DeviceSkipList.startTime, s.ShardGroupSize)

	fmt.Println("dump:", "ShardGroupSize:", s.ShardGroupSize, "shardGroupId:", shardGroupId,
		"fileSize：", item.fileSize, "deviceCount:", item.timeList.Size(),
		"start:", item.startTime, "end:", item.endTime)

	fileName := filemanager.NewFileName(int(s.ShardGroupSize), int(shardGroupId), int(item.startTime), int(item.endTime), int(time.Now().UnixNano()))
	file, err := s.FileManager.CreateTempFile(fileName)
	if err != nil {
		logrus.Fatalln(err)
	}

	err = item.DeviceSkipList.dump(file)
	if err != nil {
		logrus.Fatalln(err)
	}

	err = s.FileManager.SaveTempFile(file)
	if err != nil {
		logrus.Fatalln(err)
	}

}
