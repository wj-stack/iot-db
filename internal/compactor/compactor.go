package compactor

import (
	"fmt"
	"github.com/chen3feng/stl4go"
	"github.com/sirupsen/logrus"
	"iot-db/internal/datastructure"
	"iot-db/internal/memorytable"
	"iot-db/internal/util"
	"iot-db/internal/writer"
	"os"
	"sort"
	"strconv"
	"strings"
	time "time"
)

var ShardGroupSize = []int64{int64(time.Hour),
	int64(time.Hour * 24),
	int64(time.Hour * 24 * 7),
	int64(time.Hour * 24 * 30),
	int64(time.Hour * 24 * 90),
	int64(time.Hour * 24 * 180)}

var path = "/home/wyatt/code/iot-db/data"

type Task struct {
	Args         []string
	ShardGroupId memorytable.ShardGroupId
}

type FileFd struct {
	FirstIndex, SecondIndex, DataFile *os.File
}

func Compact(cur, next int64, task Task) error {

	firstIndex, secondIndex, dataFile, _, err := util.CreateTempFile(path, int(next), 0)
	if err != nil {
		return err
	}

	w, err := writer.NewWriter(firstIndex, secondIndex, dataFile)
	if err != nil {
		return err
	}

	for _, i := range task.Args {
		var ret []FileFd

		// open file
		list := getFileList(cur, i)
		for _, name := range list {

			if strings.HasSuffix(name, ".data") {
				name := name[:len(name)-5]
				isExist := util.IsDataFileExist(path, int(cur), i, name)
				if isExist {
					fmt.Println("open file:", path, int(cur), i, name)
					firstIndex, secondIndex, dataFile, err := util.OpenDataFile(path, int(cur), i, name)
					if err != nil {
						for _, i := range ret {
							_ = i.DataFile.Close()
							_ = i.FirstIndex.Close()
							_ = i.SecondIndex.Close()
						}
						return nil
					}
					ret = append(ret, FileFd{
						FirstIndex:  firstIndex,
						SecondIndex: secondIndex,
						DataFile:    dataFile,
					})
				}
			}
		}
		fmt.Println(fmt.Sprintf("%s/data/%010d/%s", path, cur, i))
		err = merge(ret, cur, next, w)
		if err != nil {
			return err
		}
		for _, i := range ret {
			_ = i.DataFile.Close()
			_ = i.FirstIndex.Close()
			_ = i.SecondIndex.Close()
		}
	}

	return err
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

func merge(files []FileFd, curShardSize int64, nextShardSize int64, w *writer.Writer) error {
	fmt.Println("merge:", files)

	var items []DeviceHeap

	// 读取当前文件夹所有的二级索引
	for idx, fd := range files {

		// fd:  文件夹中的一个文件
		for {
			var second datastructure.SecondIndexMeta
			err := second.ReadSecondIndexMeta(fd.SecondIndex)
			if err != nil {
				break
			}
			// logrus.Infof("second:%#v\n", second)
			items = append(items, DeviceHeap{
				DeviceId: int(second.DeviceId),
				Count:    int(second.Size),
				FdIndex:  idx,
			})

		}

	}

	// 排序
	sort.Slice(items, func(i, j int) bool {
		return items[i].DeviceId < items[j].DeviceId
	})

	fmt.Println("items:", len(items))
	logrus.Infof("items:%#v\n", items)
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

	logrus.Infoln("did:", did)

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
	logrus.Infoln("size:", size)

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

		// write file , and write first second
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
	logrus.Infoln("cnt:", cnt)
	err := w.WriteSecondIndex(int64(did))
	if err != nil {
		return err
	}
	return nil
}

func GenerateTasks(cur, next int64, save int) []Task {
	dirs := getDirList(cur)
	fmt.Println(dirs)
	lastId := memorytable.ShardGroupId(-1)
	var tasks []Task
	var args []string
	var id memorytable.ShardGroupId
	// The last day's data are retained by default
	for _, i := range dirs {
		t, _ := strconv.Atoi(i)

		id = memorytable.TimestampConvertShardId(int64(t*int(cur)), next)
		fmt.Println(id, i)
		if id != lastId {
			// commit task
			lastId = id
			if len(args) > 0 {
				tasks = append(tasks, Task{
					Args:         args,
					ShardGroupId: id,
				})
			}
			args = []string{}
		}
		args = append(args, i)

	}
	if len(args) > 0 {
		tasks = append(tasks, Task{
			Args:         args,
			ShardGroupId: id,
		})
	}
	if len(tasks) < save {
		logrus.Infoln("the number of files is too small to merge")
		return nil
	}
	return tasks[:len(tasks)-save]
}

func getDirList(shardSize int64) []string {
	dir, err := os.ReadDir(fmt.Sprintf("%s/data/%010d", path, shardSize))
	if err != nil {
		return nil
	}
	var ret []string
	for _, i := range dir {
		ret = append(ret, i.Name())
	}
	return ret
}

func getFileList(shardSize int64, shardId string) []string {
	dir, err := os.ReadDir(fmt.Sprintf("%s/data/%010d/%s/", path, shardSize, shardId))
	if err != nil {
		return nil
	}
	var ret []string
	for _, i := range dir {
		ret = append(ret, i.Name())
	}
	return ret
}
