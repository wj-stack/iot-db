package compactor

import (
	"fmt"
	"github.com/chen3feng/stl4go"
	"github.com/sirupsen/logrus"
	"iot-db/internal/datastructure"
	"iot-db/internal/filemanager"
	"iot-db/internal/shardgroup"
	"iot-db/internal/writer"
	"os"
	"sort"
	"strconv"
	"strings"
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

func NewCompactor() *Compactor {
	return &Compactor{}
}

func (c *Compactor) Compact(task Task) error {

	// empty dir
	if len(task.CurrentShardGroupId) == 0 {
		return nil
	}

	firstIndex, secondIndex, dataFile, name, err := c.FileManager.CreateTempFile(task.NextShardGroupSize, task.NextShardGroupId)
	if err != nil {
		return err
	}

	w, err := writer.NewWriter(firstIndex, secondIndex, dataFile)
	if err != nil {
		return err
	}

	var fds []FileFd
	for _, i := range task.CurrentShardGroupId {

		// open file
		list := c.GetDataFileList(task.CurrentShardGroupSize, i)
		for _, name := range list {

			if strings.HasSuffix(name, ".data") {
				name := name[:len(name)-5]
				isExist := c.IsDataFileExist(int(task.CurrentShardGroupSize), i, name)
				if isExist {
					logrus.Traceln("open file:", path, int(task.CurrentShardGroupSize), i, name)
					firstIndex, secondIndex, dataFile, err := c.OpenDataFile(int(task.CurrentShardGroupSize), i, name)
					if err != nil {
						for _, i := range fds {
							_ = i.DataFile.Close()
							_ = i.FirstIndex.Close()
							_ = i.SecondIndex.Close()
						}
						return w.Close()
					}
					fds = append(fds, FileFd{
						FirstIndex:  firstIndex,
						SecondIndex: secondIndex,
						DataFile:    dataFile,
						Path:        fmt.Sprintf("%s/data/%010d/%s/%s", path, int(task.CurrentShardGroupSize), i, name),
					})
				}
			}
		}
	}

	err = merge(fds, w)
	if err != nil {
		return err
	}
	for _, i := range fds {
		_ = i.DataFile.Close()
		_ = i.FirstIndex.Close()
		_ = i.SecondIndex.Close()
	}

	return c.Rename(int(task.NextShardGroupSize), task.NextShardGroupId, task.NextShardGroupSize, task.NextShardGroupId, name)
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

func merge(files []FileFd, w *writer.Writer) error {
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
func mergeByTimeStamp(files []FileFd, items []DeviceHeap, did int, w *writer.Writer) error {

	logrus.Traceln("merge did:", did)

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
	logrus.Traceln("size:", size)

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
	logrus.Traceln("unique:", cnt)
	err := w.WriteSecondIndex(int64(did))
	if err != nil {
		return err
	}
	return nil
}

func (c *Compactor) GenerateTasks(cur, next int64, save int) []Task {
	dirs := c.GetDataDirList(cur)
	lastId := shardgroup.ShardGroupId(-1)
	var tasks []Task
	var args []string
	var id shardgroup.ShardGroupId
	// The last day's data are retained by default
	for _, i := range dirs {
		t, _ := strconv.Atoi(i)

		id = shardgroup.TimestampConvertShardId(int64(t*int(cur)), next)
		if id != lastId {
			// commit task
			lastId = id
			if len(args) > 0 {
				tasks = append(tasks, Task{
					CurrentShardGroupId:   args,
					NextShardGroupId:      int(id),
					CurrentShardGroupSize: cur,
					NextShardGroupSize:    int(next),
				})
			}
			args = []string{}
		}
		args = append(args, i)

	}
	if len(args) > 0 {
		tasks = append(tasks, Task{
			CurrentShardGroupId:   args,
			NextShardGroupId:      int(id),
			CurrentShardGroupSize: cur,
			NextShardGroupSize:    int(next),
		})
	}

	if len(tasks) > 0 {
		if len(tasks[len(tasks)-1].CurrentShardGroupId) >= save {
			tasks[len(tasks)-1].CurrentShardGroupId = tasks[len(tasks)-1].CurrentShardGroupId[:len(tasks[len(tasks)-1].CurrentShardGroupId)-save]
		} else {
			return nil
		}
	}

	return tasks
}
