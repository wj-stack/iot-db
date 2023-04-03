package memorytable

import (
	"iot-db/pkg/skiplist"
	"math"
	"os"
	"sync"
)

// SampleSizePerShardGroup limit first index size
const SampleSizePerShardGroup = 20

type DeviceList struct {
	list  skiplist.MapI[int64, *TimeSkipList]
	mutex sync.RWMutex

	// all device
	startTime int64
	endTime   int64
	count     int64
	size      int64
}

func NewDeviceList() *DeviceList {
	return &DeviceList{
		list:      skiplist.NewSkipListMap[int64, *TimeSkipList](skiplist.OrderedComparator[int64]{}),
		mutex:     sync.RWMutex{},
		startTime: math.MaxInt64,
		endTime:   math.MinInt64,
	}
}

func (l *DeviceList) Insert(deviceId int64, data *Data) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	timeList, err := l.list.Get(deviceId)
	if err != nil {
		timeList = NewTimeSkipList()
		l.list.Insert(deviceId, timeList)
	}
	timeList.Insert(data.Timestamp, data)
	l.count++
	l.size += int64(data.Length)
	if data.Timestamp > l.endTime {
		l.endTime = data.Timestamp
	}
	if data.Timestamp < l.startTime {
		l.startTime = data.Timestamp
	}
}

func (l *DeviceList) Query(did, start, end int64) (ret []*Data, err error) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	list, err := l.list.Get(did)
	if err != nil {
		return nil, err
	}
	return list.Query(start, end)
}

func (l *DeviceList) dump(dataFile *os.File, firstIndex *os.File, secondIndex *os.File) error {
	iterator, err := l.list.Iterator()
	if err != nil {
		return err
	}
	threshold := l.count / SampleSizePerShardGroup
	var cnt int64
	var offset int64
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
			timestamp, data, err := it.Next()
			if err != nil {
				break
			}

			err = data.Write(dataFile)
			if err != nil {
				return err
			}

			offset += int64(len(data.Body))

			cnt++
			if cnt%threshold == 0 || cnt == 1 || int64(timeSkipList.Size()) == cnt {
				firstIndexMeta := FirstIndexMeta{
					Timestamp: timestamp,
					Offset:    offset,
				}
				err := firstIndexMeta.WriteFirstIndex(firstIndex)
				if err != nil {
					return err
				}
			}
		}

		secondIndexMeta := SecondIndexMeta{
			Start:    timeSkipList.startTime,
			End:      timeSkipList.endTime,
			DeviceId: deviceId,
			Size:     int64(l.list.Size()),
			Offset:   offset,
			Flag:     0,
		}
		err = secondIndexMeta.WriteSecondIndex(secondIndex)
		if err != nil {
			return err
		}
	}
	err = firstIndex.Close()
	if err != nil {
		return err
	}
	err = dataFile.Close()
	if err != nil {
		return err
	}
	err = secondIndex.Close()
	if err != nil {
		return err
	}
	return nil
}

type TimeSkipList struct {
	skiplist.MapI[int64, *Data]
	mutex sync.RWMutex

	// just for a device
	startTime int64
	endTime   int64
}

func NewTimeSkipList() *TimeSkipList {
	return &TimeSkipList{
		MapI:      skiplist.NewSkipListMap[int64, *Data](skiplist.OrderedComparator[int64]{}),
		mutex:     sync.RWMutex{},
		startTime: math.MaxInt64,
		endTime:   math.MinInt64,
	}
}

func (l *TimeSkipList) Insert(timestamp int64, data *Data) {
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

func (l *TimeSkipList) Query(start, end int64) (ret []*Data, err error) {
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
