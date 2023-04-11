package compactor

import (
	"github.com/chen3feng/stl4go"
	"github.com/sirupsen/logrus"
	"iot-db/internal/datastructure"
	"iot-db/internal/filemanager"
	"iot-db/internal/shardgroup"
	"iot-db/internal/writer"
	"os"
	"sort"
	"time"
)

var path = "/home/wyatt/code/iot-db/data"

func SetWorkspace(s string) {
	path = s
}

type Task struct {
	CurrentShardGroupId   []string // directories waiting to be merged
	NextShardGroupId      int      // merged shard group id
	CurrentShardGroupSize int64
	NextShardGroupSize    int
}

type FileFd struct {
	FirstIndex, SecondIndex, DataFile *os.File
	Path                              string
}

type Compactor struct {
	*filemanager.FileManager
}

func NewCompactor(f *filemanager.FileManager) *Compactor {
	return &Compactor{
		FileManager: f,
	}
}

func (c *Compactor) Compact(index int) error {
	var x int64 = 1
	fileList := c.GetCompactFileList(int(shardgroup.ShardGroupSize[index]), // shard size
		int(shardgroup.ShardGroupSize[index+1]),
		int(shardgroup.TimestampConvertShardId(time.Now().UnixNano()-x*shardgroup.ShardGroupSize[index], shardgroup.ShardGroupSize[index]))) // max shard id

	// empty dir or too small
	if len(fileList) < 2 {
		logrus.Infoln("no compact...")
		return nil
	}
	for id, i := range fileList {
		target := filemanager.NewFileName(int(shardgroup.ShardGroupSize[index+1]), id, 0, 0, int(time.Now().UnixNano()))
		file, err := c.FileManager.CreateTempFile(target)
		if err != nil {
			return err
		}
		w, err := writer.NewWriter(file)
		if err != nil {
			return err
		}

		var files []*filemanager.File
		// open files
		for _, name := range i {
			file, err := c.FileManager.OpenDataFile(name)
			if err != nil {
				return err
			}
			files = append(files, file)
		}
		logrus.Infoln("merge ->", id, "len:", len(i))

		// merge
		err = merge(files, w)
		if err != nil {
			return err
		}

		// mv tmp -> data
		err = c.Rename(target)
		if err != nil {
			return err
		}

		// close data file
		for _, name := range i {
			err = c.CloseDataFile(name)
			if err != nil {
				logrus.Errorln(err)
			}
		}

		// remove
		for _, name := range i {
			func(name string) {
				for {
					err := c.Del(name)
					if err != nil {
						logrus.Warning(err)
						time.Sleep(time.Second * 1)
						continue
					}
					break
				}
			}(name)
		}
	}
	return nil
}

type DeviceHeap struct {
	DeviceId int
	Count    int
	FdIndex  int
}

type TimestampHeap struct {
	datastructure.Data
	FdIndex int
	Count   int
}

func merge(files []*filemanager.File, w *writer.Writer) error {
	var items []DeviceHeap

	for idx, fd := range files {

		for {
			var second datastructure.SecondIndexMeta
			err := second.ReadSecondIndexMeta(fd.SecondIndex)
			if err != nil {
				break
			}
			items = append(items, DeviceHeap{
				DeviceId: int(second.DeviceId),
				Count:    int(second.Size),
				FdIndex:  idx,
			})

		}

	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].DeviceId < items[j].DeviceId
	})

	logrus.Traceln("items:", len(items))

	i := 0
	j := 0
	var args []DeviceHeap
	for j < len(items) {
		if items[j].DeviceId != items[i].DeviceId {
			// commit
			err := mergeByTimeStamp(files, args, items[i].DeviceId, w)
			if err != nil {
				return err
			}
			args = nil
			i = j
		}
		args = append(args, items[j])
		j++
	}
	// commit
	if len(args) > 0 {
		err := mergeByTimeStamp(files, args, items[i].DeviceId, w)
		if err != nil {
			return err
		}
	}
	return nil
}

// did  是 相等的
func mergeByTimeStamp(files []*filemanager.File, items []DeviceHeap, did int, w *writer.Writer) error {

	//logrus.Traceln("merge did:", did)

	h := stl4go.NewPriorityQueueFunc[TimestampHeap](func(a, b TimestampHeap) bool {
		if a.Timestamp != b.Timestamp {
			return a.Timestamp < b.Timestamp
		}
		return a.CreatedAt > b.CreatedAt
	})

	size := 0 // 当前 did 的所有数量,没有去重

	//  归并排序， 把每一个队列的第一个元素放到堆中
	for _, item := range items {
		data := datastructure.Data{}
		err := data.Read(files[item.FdIndex].DataFile)
		if err != nil {
			continue
		}

		h.Push(TimestampHeap{
			Data:    data,
			FdIndex: item.FdIndex,
			Count:   item.Count - 1, // 剩余长度
		})

		size += item.Count

	}
	//logrus.Traceln("size:", size)

	// 计算阈值,用于划分first_index
	threshold := size / datastructure.SampleSizePerShardGroup
	if threshold == 0 {
		threshold = 1
	}

	var lastTimestamp int64 = -1
	var cnt int // 去重后的数量
	for !h.IsEmpty() {
		v := h.Top()
		h.Pop()

		// unique
		if v.Timestamp == lastTimestamp {
			continue
		}
		lastTimestamp = v.Timestamp

		// writer file , and writer first second
		err := w.WriteData(&v.Data)
		if err != nil {
			return err
		}

		cnt++
		if cnt%threshold == 0 || cnt == 1 || h.IsEmpty() {
			err := w.WriteFirstIndex()
			if err != nil {
				return err
			}
		}

		if v.Count > 0 {
			// unique
			tmpCnt := 0
			for {
				data := datastructure.Data{}
				err := data.Read(files[v.FdIndex].DataFile)
				if err != nil {
					break
				}
				tmpCnt++
				if data.Timestamp != v.Timestamp {
					h.Push(TimestampHeap{
						Data:    data,
						FdIndex: v.FdIndex,
						Count:   v.Count - tmpCnt,
					})
					break
				}
			}
		}

	}
	//logrus.Traceln("unique:", cnt)
	err := w.WriteSecondIndex(int64(did))
	if err != nil {
		return err
	}
	return nil
}
