package compactor

import (
	"github.com/chen3feng/stl4go"
	"github.com/sirupsen/logrus"
	"io"
	"iot-db/internal/config"
	"iot-db/internal/datastructure"
	"iot-db/internal/filemanager"
	"iot-db/internal/shardgroup"
	"iot-db/internal/writer"
	"sort"
	"time"
)

type Task struct {
	CurrentShardGroupId   []string // directories waiting to be merged
	NextShardGroupId      int      // merged shard group id
	CurrentShardGroupSize int64
	NextShardGroupSize    int
}

type Compactor struct {
	Config *config.Config
	*filemanager.FileManager
}

func NewCompactor(f *filemanager.FileManager) *Compactor {
	return &Compactor{
		Config:      f.Config,
		FileManager: f,
	}
}

func (c *Compactor) createTempFile(index, id int) (string, *filemanager.File, *writer.Writer, error) {
	target := filemanager.NewFileName(int(c.Config.Core.ShardGroupSize[index+1]), id, 0, 0, int(time.Now().UnixNano()))
	file, err := c.FileManager.CreateTempFile(target)
	if err != nil {
		return "", nil, nil, err
	}
	w, err := writer.NewWriter(file)
	logrus.Infoln("write file:", target, w)
	if err != nil {
		return "", nil, nil, err
	}
	return target, file, w, nil
}

func (c *Compactor) Compact(index int) error {
	var x int64 = 1
	fileList := c.GetCompactFileList(c.Config.Core.ShardGroupSize[index], // shard size
		c.Config.Core.ShardGroupSize[index+1],
		int(shardgroup.TimestampConvertShardId(time.Now().UnixNano()-x*int64(c.Config.Core.ShardGroupSize[index]), int64(c.Config.Core.ShardGroupSize[index])))) // max shard id
	logrus.Infoln("maxId", int(shardgroup.TimestampConvertShardId(time.Now().UnixNano()-x*int64(c.Config.Core.ShardGroupSize[index]), int64(c.Config.Core.ShardGroupSize[index]))))
	// empty dir or too small
	if len(fileList) < 2 {
		logrus.Infoln("no compact...", len(fileList))
		return nil
	}

	logrus.Infoln("compact", fileList)

	for id, i := range fileList {

		var files []*filemanager.File
		// open files
		for _, name := range i {
			file, err := c.FileManager.OpenFile(name)
			if err != nil {
				return err
			}
			files = append(files, file)
		}
		logrus.Infoln("merge ->", id, "len:", len(i), len(files), files)

		var items []DeviceHeap

		for idx, fd := range files {
			for {
				var second datastructure.SecondIndexMeta
				err := second.ReadSecondIndexMeta(fd.SecondIndex)
				if err != nil {
					break
				}
				_, err = fd.FirstIndex.Seek(second.Offset, io.SeekStart)
				if err != nil {
					return err
				}
				header, err := writer.ReadFirstIndexHeader(fd.FirstIndex)
				if err != nil {
					return err
				}

				items = append(items, DeviceHeap{
					DeviceId: int(second.DeviceId),
					Count:    int(second.Size),
					FdIndex:  idx,
					Header:   header,
				})

			}
		}

		sort.Slice(items, func(i, j int) bool {
			return items[i].DeviceId < items[j].DeviceId
		})

		lIdx := 0
		rIdx := 0

		logrus.Infoln("second index len:", len(items))

		// create temp file
		target, file, w, err := c.createTempFile(index, id)
		if err != nil {
			return err
		}

		logrus.Infoln("target:", target)
		var args []DeviceHeap
		for rIdx < len(items) {
			if items[rIdx].DeviceId != items[lIdx].DeviceId {
				// commit
				err := mergeByTimeStamp(files, args, items[lIdx].DeviceId, w, items[lIdx].Header)
				if err != nil {
					return err
				}
				args = nil
				lIdx = rIdx
				if w.DataFileOffset > int64(c.Config.Compactor.FragmentSize[index+1]) {
					logrus.Infoln("too big... create new file...")
					err = c.SaveTempFile(file)
					if err != nil {
						return err
					}

					// create w
					target, file, w, err = c.createTempFile(index, id)
					if err != nil {
						return err
					}
					logrus.Infoln("new file:", target)
				}
			}
			args = append(args, items[rIdx])
			rIdx++

		}
		// commit
		if len(args) > 0 {
			err := mergeByTimeStamp(files, args, items[lIdx].DeviceId, w, items[lIdx].Header)
			if err != nil {
				return err
			}
		}

		// mv tmp -> data
		err = c.SaveTempFile(file)
		if err != nil {
			return err
		}

		// close data file
		for _, i := range i {
			err := c.FileManager.CloseFile(i)
			if err != nil {
				return err
			}
		}

		//remove
		for _, name := range i {
			func(name string) {
				for {
					err := c.FileManager.Remove(name)
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
	Header   []byte
}

type TimestampHeap struct {
	datastructure.Data
	FdIndex int
	Count   int
}

func mergeByTimeStamp(files []*filemanager.File, items []DeviceHeap, did int, w *writer.Writer, header []byte) error {

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

	err := w.WriteFirstIndexHeader(header)
	if err != nil {
		return err
	}

	// 计算阈值,用于划分first_index
	threshold := int(float64(size) * 0.1)
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

	err = w.WriteSecondIndex(int64(did))
	if err != nil {
		return err
	}
	return nil
}
