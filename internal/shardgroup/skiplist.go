package shardgroup

import (
	"iot-db/internal/datastructure"
	"iot-db/internal/filemanager"
	"iot-db/internal/writer"
	"iot-db/pkg/skiplist"
	"math"
	"sync"
)

type DeviceList struct {
	list  skiplist.MapI[int64, *TimeSkipList]
	mutex sync.RWMutex

	// all device
	startTime int64
	endTime   int64
	fileSize  int64
}

func NewDeviceList() *DeviceList {
	return &DeviceList{
		list:      skiplist.NewSkipListMap[int64, *TimeSkipList](skiplist.OrderedComparator[int64]{}),
		mutex:     sync.RWMutex{},
		startTime: math.MaxInt64,
		endTime:   math.MinInt64,
	}
}

func (l *DeviceList) Insert(deviceId int64, data *datastructure.Data) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	timeList, err := l.list.Get(deviceId)
	if err != nil {
		timeList = NewTimeSkipList()
		l.list.Insert(deviceId, timeList)
	}
	timeList.Insert(data.Timestamp, data)
	l.fileSize += int64(data.Length)
	if data.Timestamp > l.endTime {
		l.endTime = data.Timestamp
	}
	if data.Timestamp < l.startTime {
		l.startTime = data.Timestamp
	}
}

func (l *DeviceList) Query(did, start, end int64) (ret []*datastructure.Data, err error) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	list, err := l.list.Get(did)
	if err != nil {
		return nil, err
	}
	return list.Query(start, end)
}

func (l *DeviceList) GetCount() int {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	iterator, err := l.list.Iterator()
	if err != nil {
		return 0
	}
	result := 0
	for {
		_, v, err := iterator.Next()
		if err != nil {
			break
		}
		result += v.Size()
	}
	return result
}

func (l *DeviceList) dump(file *filemanager.File) error {

	w, err := writer.NewWriter(file)
	if err != nil {
		return err
	}

	// device iterator
	iterator, err := l.list.Iterator()
	if err != nil {
		return err
	}
	threshold := int(float64(l.list.Size()) * 0.1)
	if threshold == 0 {
		threshold = 1
	}

	var cnt int64
	for {

		deviceId, timeSkipList, err := iterator.Next()
		if err != nil {
			break
		}

		it, err := timeSkipList.Iterator()
		if err != nil {
			return err
		}

		for {
			_, data, err := it.Next()
			if err != nil {
				break
			}

			err = w.WriteData(data)
			if err != nil {
				return err
			}

			if int(cnt)%threshold == 0 || cnt == 1 || int64(timeSkipList.Size()) == cnt {
				err := w.WriteFirstIndex()
				if err != nil {
					return err
				}

			}

		}

		err = w.WriteSecondIndex(deviceId)
		if err != nil {
			return err
		}

	}
	return nil
}

type TimeSkipList struct {
	skiplist.MapI[int64, *datastructure.Data]
	mutex sync.RWMutex

	// just for a device
	startTime int64
	endTime   int64
}

func NewTimeSkipList() *TimeSkipList {

	return &TimeSkipList{
		MapI:      skiplist.NewSkipListMap[int64, *datastructure.Data](skiplist.OrderedComparator[int64]{}),
		mutex:     sync.RWMutex{},
		startTime: math.MaxInt64,
		endTime:   math.MinInt64,
	}
}

func (l *TimeSkipList) Insert(timestamp int64, data *datastructure.Data) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.MapI.Insert(timestamp, data)
	if timestamp > l.endTime {
		l.endTime = timestamp
	}
	if timestamp < l.startTime {
		l.startTime = timestamp
	}
}

func (l *TimeSkipList) Query(start, end int64) (ret []*datastructure.Data, err error) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	between, err := l.MapI.IteratorBetween(start, end)
	if err != nil {
		return nil, err
	}

	for {
		_, v, err := between.Next()
		if err != nil {
			break
		}
		ret = append(ret, v)
	}

	return ret, nil
}
