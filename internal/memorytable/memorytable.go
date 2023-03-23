package memorytable

import (
	"iot-db/pkg/skiplist"
	"math"
	"sync"
)

type Data []byte

type DeviceSkipList struct {
	list  skiplist.MapI[int64, *TimeSkipList]
	mutex sync.RWMutex
}

func (ds *DeviceSkipList) Get(key int64) (*TimeSkipList, error) {
	ds.mutex.RLock()
	defer ds.mutex.RUnlock()
	return ds.list.Get(key)
}

func (ds *DeviceSkipList) Traversal(cb func(deviceId int64, timeList *TimeSkipList) error) error {
	ds.mutex.RLock()
	defer ds.mutex.RUnlock()
	it, err := ds.list.Iterator()
	if err != nil {
		return err
	}
	for {
		k, v, err := it.Next()
		if err == skiplist.Done {
			break
		}
		err = cb(k, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ds *DeviceSkipList) NewTimeSkipList(key int64) *TimeSkipList {
	ds.mutex.Lock()
	defer ds.mutex.Unlock()
	list := NewTimeSkipList()
	ds.list.Insert(key, list)
	return list
}

type TimeSkipList struct {
	list       skiplist.MapI[int64, Data]
	start, end int64
	mutex      sync.RWMutex
}

func (ts *TimeSkipList) GetStart() int64 {
	ts.mutex.RLock()
	defer ts.mutex.RUnlock()
	return ts.start
}

func (ts *TimeSkipList) GetEnd() int64 {
	ts.mutex.RLock()
	defer ts.mutex.RUnlock()
	return ts.end
}

func (ts *TimeSkipList) Size() int64 {
	ts.mutex.RLock()
	defer ts.mutex.RUnlock()
	return int64(ts.list.Size())
}

func (ts *TimeSkipList) Get(t int64) (Data, error) {

	ts.mutex.RLock()
	defer ts.mutex.RUnlock()

	data, err := ts.list.Get(t)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (ts *TimeSkipList) Insert(t int64, data Data) {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()

	ts.list.Insert(t, data)
	if ts.start > t {
		ts.start = t
	}
	if ts.end < t {
		ts.end = t
	}
}

func (ts *TimeSkipList) Traversal(cb func(timestamp int64, data Data) error) error {
	ts.mutex.RLock()
	defer ts.mutex.RUnlock()
	it, err := ts.list.Iterator()
	if err != nil {
		return err
	}
	for {
		k, v, err := it.Next()
		if err == skiplist.Done {
			break
		}
		err = cb(k, v)
		if err != nil {
			return err
		}
	}
	return nil
}

type PointData struct {
	Data      Data
	Timestamp int64
}

func (ts *TimeSkipList) Between(t1, t2 int64) (ret []*PointData, err error) {
	ts.mutex.RLock()
	defer ts.mutex.RUnlock()

	it, err := ts.list.IteratorBetween(t1, t2)
	if err != nil {
		return nil, err
	}
	for {
		k, v, err := it.Next()
		if err == skiplist.Done {
			break
		}
		ret = append(ret, &PointData{Data: v, Timestamp: k})
	}
	return ret, nil

}

func (ts *TimeSkipList) Latest() (ret Data, err error) {
	ts.mutex.RLock()
	defer ts.mutex.RUnlock()

	ret, err = ts.list.Get(ts.end)
	if err != nil {
		return nil, err
	}
	return ret, nil

}

type Table struct {
	deviceList  *DeviceSkipList
	startTime   int64
	endTime     int64
	maxDeviceId int64
	cnt         int64
	mutex       sync.Mutex
}

func (t *Table) StartTime() int64 {
	return t.startTime
}

func (t *Table) EndTime() int64 {
	return t.endTime
}

func NewDeviceList() *DeviceSkipList {
	list := skiplist.NewSkipListMap[int64, *TimeSkipList](skiplist.OrderedComparator[int64]{})
	return &DeviceSkipList{list: list}
}

// NewTable key: deviceId value: *timeSkipList
func NewTable() *Table {
	list := skiplist.NewSkipListMap[int64, *TimeSkipList](skiplist.OrderedComparator[int64]{})
	return &Table{
		deviceList: &DeviceSkipList{list: list},
		startTime:  math.MaxInt64,
		endTime:    math.MinInt64,
		mutex:      sync.Mutex{},
	}
}

// NewTimeSkipList : timestamp value: data
func NewTimeSkipList() *TimeSkipList {
	return &TimeSkipList{
		list:  skiplist.NewSkipListMap[int64, Data](skiplist.OrderedComparator[int64]{}),
		start: math.MaxInt64,
		end:   math.MinInt64,
	}

}

func (t *Table) Insert(timestamp int64, deviceId int64, data Data) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	list, err := t.deviceList.Get(deviceId)
	if err == skiplist.NotFound {
		list = t.deviceList.NewTimeSkipList(deviceId)
	}
	list.Insert(timestamp, data)

	t.cnt++

	if t.startTime > timestamp {
		t.startTime = timestamp
	}
	if t.endTime < timestamp {
		t.endTime = timestamp
	}
	if deviceId > t.maxDeviceId {
		t.maxDeviceId = deviceId
	}
}

func (t *Table) GetTimeSkipList(deviceId int64) (*TimeSkipList, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	list, err := t.deviceList.Get(deviceId)
	if err == skiplist.NotFound {
		return nil, err
	}
	return list, nil
}

func (t *Table) GetDeviceSkipList() *DeviceSkipList {
	return t.deviceList
}

func (t *Table) GetMaxDeviceId() int64 {
	return t.maxDeviceId
}
func (t *Table) Count() int64 {
	return t.cnt
}
