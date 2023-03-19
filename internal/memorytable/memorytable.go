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
	list := newTimeSkipList()
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
	ts.start = int64(math.Min(float64(ts.start), float64(t)))
	ts.end = int64(math.Max(float64(ts.end), float64(t)))
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

type OnInsertCallBack func(table *Table)

type Table struct {
	List     *DeviceSkipList
	size     int
	OnInsert OnInsertCallBack
}

// NewTable key: deviceId value: *timeSkipList
func NewTable(OnInsert OnInsertCallBack) *Table {
	list := skiplist.NewSkipListMap[int64, *TimeSkipList](skiplist.OrderedComparator[int64]{})
	return &Table{
		List:     &DeviceSkipList{list: list},
		size:     0,
		OnInsert: OnInsert,
	}
}

// key: timestamp value: data
func newTimeSkipList() *TimeSkipList {
	return &TimeSkipList{
		list:  skiplist.NewSkipListMap[int64, Data](skiplist.OrderedComparator[int64]{}),
		start: 1e17,
		end:   0,
	}

}

func (t *Table) Insert(timestamp int64, deviceId int64, data Data) {
	list, err := t.List.Get(deviceId)
	if err == skiplist.NotFound {
		list = t.List.NewTimeSkipList(deviceId)
	}
	list.Insert(timestamp, data)
	t.size += len(data)
	t.OnInsert(t)
}

func (t *Table) GetSkipList(deviceId int64) (*TimeSkipList, error) {
	list, err := t.List.Get(deviceId)
	if err == skiplist.NotFound {
		return nil, err
	}
	return list, nil
}

func (t *Table) Size() int {
	return t.size
}
