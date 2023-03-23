package index

import (
	"iot-db/pkg/skiplist"
	"math"
	"os"
)

type FileInfo struct {
	*FirstIndexMeta
	*os.File
}

type Table struct {
	deviceList skiplist.MapI[int64, *TimeList]
	meta       map[int64]*SecondIndexMeta
}

func NewTable() *Table {
	return &Table{
		deviceList: skiplist.NewSkipListMap[int64, *TimeList](skiplist.OrderedComparator[int64]{}),
		meta:       map[int64]*SecondIndexMeta{},
	}
}

func (t *Table) Insert(timestamp int64, deviceId int64, info *FileInfo) {
	list, err := t.deviceList.Get(deviceId)
	if err == skiplist.NotFound {
		list = &TimeList{MapI: skiplist.NewSkipListMap[int64, *FileInfo](skiplist.OrderedComparator[int64]{})}
		t.deviceList.Insert(deviceId, list)
		t.meta[deviceId] = &SecondIndexMeta{
			Start: math.MaxInt64,
			End:   math.MinInt64,
		}
	}
	list.Insert(timestamp, info)
	if t.meta[deviceId].Start < timestamp {
		t.meta[deviceId].Start = timestamp
	}

	if t.meta[deviceId].End > timestamp {
		t.meta[deviceId].End = timestamp
	}
}

type TimeList struct {
	skiplist.MapI[int64, *FileInfo]
}

func (t *TimeList) Traversal(cb func(timestamp int64, v *FileInfo) error) error {
	it, err := t.Iterator()
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

func (t *Table) Traversal(cb func(deviceId int64, timeList *TimeList) error) error {
	it, err := t.deviceList.Iterator()
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

func (t *Table) Save(file *File) error {
	var offset int64
	err := t.Traversal(func(deviceId int64, timeList *TimeList) error {
		// write second index
		err := WriteSecondIndex(file.SecondIndex, &SecondIndexMeta{
			Start:    t.meta[deviceId].Start,
			End:      t.meta[deviceId].End,
			DeviceId: deviceId,
			Count:    int64(t.deviceList.Size()),
		})
		if err != nil {
			return err
		}
		err = timeList.Traversal(func(timestamp int64, v *FileInfo) error {

			// copy data
			data, err := GetData(v.File, v.Offset, v.Len)
			if err != nil {
				return err
			}

			_, err = file.DataFile.Write(data)
			if err != nil {
				return err
			}

			// write first index
			err = WriteFirstIndex(file.FirstIndex, &FirstIndexMeta{
				Timestamp: timestamp,
				Offset:    offset,
				Len:       v.Len,
			})
			if err != nil {
				return err
			}

			offset += v.Len
			return nil
		})

		return err
	})
	return err

}
