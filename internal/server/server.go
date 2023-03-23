package server

import (
	"encoding/binary"
	"fmt"
	wal "github.com/tidwall/wal"
	"iot-db/internal/data"
	"iot-db/internal/memorytable"
	"iot-db/internal/util"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"
)

type Query struct {
	EndTime   time.Time
	StartTime time.Time
	DeviceId  int64
}

type Engine interface {
	Write(data *data.BinaryData) error
	Read(Query) ([]*data.BinaryData, error)
}

type Cake struct {
	*memorytable.Table
	queue      map[*memorytable.Table]struct{}
	queueMutex sync.RWMutex
	size       int
	log        *wal.Log
	mutex      sync.RWMutex
	workspace  string
	fileList   *FileList
}

func NewEngine(workspace string) *Cake {
	log, err := wal.Open(fmt.Sprintf("%s/wal", workspace), wal.DefaultOptions)
	if err != nil {
		fmt.Println("err:", err)
		panic(err)
	}
	firstIndex, err := log.FirstIndex()
	if err != nil {
		fmt.Println("err:", err)
		panic(err)
	}
	lastIndex, err := log.LastIndex()
	if err != nil {
		fmt.Println("err:", err)
		panic(err)
	}
	fmt.Println("firstIndex:", firstIndex)
	fmt.Println("lastIndex:", lastIndex)
	for i := firstIndex; i < lastIndex; i++ {
		bytes, err := log.Read(i)
		if err != nil {
			fmt.Println("log.Read", err)
		}
		v := data.BinaryData{}
		err = v.Unmarshal(bytes)
		if err != nil {
			panic(err)
		}
		fmt.Printf("v:%#v\n", v)
	}

	for i := 0; i < MaxLevel; i++ {
		err := os.MkdirAll(fmt.Sprintf("%s/tmp/%02d/", workspace, i), 0755)
		if err != nil {
			panic(err)
		}
		err = os.MkdirAll(fmt.Sprintf("%s/data/%02d/", workspace, i), 0755)
		if err != nil {
			panic(err)
		}
	}

	rand.Seed(time.Now().UnixNano())
	files := ReadCurrentFileList(workspace)

	return &Cake{
		Table:     memorytable.NewTable(),
		queue:     map[*memorytable.Table]struct{}{},
		size:      0,
		log:       log,
		mutex:     sync.RWMutex{},
		workspace: workspace,
		fileList:  files,
	}
}

type FileStatus struct {
	IsCompact bool
	mutex     sync.Mutex
}

func (fs *FileStatus) SetCompactStatus(isCompact bool) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()
	fs.IsCompact = isCompact
}

type FileList struct {
	List  map[int]map[string]*FileStatus // level -> filename -> file status
	mutex sync.Mutex
}

func (fs *FileList) GetNotCompactFileList(level int) []string {
	var ret []string
	fs.mutex.Lock()
	defer fs.mutex.Lock()
	for filename, status := range fs.List[level] {
		if !status.IsCompact {
			ret = append(ret, filename)
			status.IsCompact = true
		}
	}
	return ret
}

func (c *Cake) Compactor(level int) {
	//if level == MaxLevel {
	//	return
	//}
	//list := c.fileList.GetNotCompactFileList(level)
	//nextLevel := level + 1
	//c.merge(list, level, nextLevel)
}

func ReadCurrentFileList(workspace string) *FileList {
	v := FileList{
		List:  map[int]map[string]*FileStatus{},
		mutex: sync.Mutex{},
	}
	for i := 0; i < MaxLevel; i++ {
		v.List[i] = make(map[string]*FileStatus)
		dir, err := os.ReadDir(fmt.Sprintf("%s/data/%02d", workspace, i))
		if err != nil {
			return &v
		}
		for _, j := range dir {
			name := strings.Split(j.Name(), ".")
			if len(name) == 2 && util.IsDataFileExist(workspace, i, name[1]) {
				v.List[i][name[1]] = &FileStatus{}
			}
			// TODO: remove other file
		}

	}
	return &v
}

const MaxLevel = 7

const TableMaxSize = 1024 * 1024 * 2 // 2MB

func (c *Cake) Write(data *data.BinaryData) error {

	//wal
	err := func() error {
		marshal, err := data.Marshal()
		if err != nil {
			return err
		}

		c.mutex.Lock()
		defer c.mutex.Unlock()

		index, err := c.log.LastIndex()
		if err != nil {
			return err
		}
		return c.log.Write(index+1, marshal)

	}()
	if err != nil {
		return err
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	//memory
	c.Insert(data.Timestamp, data.DeviceId, data.Body)
	c.size += len(data.Body)
	if c.size > 1024 {
		go func(table *memorytable.Table) {
			c.queueMutex.Lock()
			c.queue[table] = struct{}{}
			c.queueMutex.Unlock()

			err := c.Dump(table)
			if err != nil {
				panic(err)
				// TODO: log
				return
			}
			c.queueMutex.Lock()
			delete(c.queue, table)
			c.queueMutex.Unlock()
		}(c.Table)
		fmt.Println("c.Table.StartTime():", c.Table.StartTime())
		fmt.Println("c.Table.EndTime():", c.Table.EndTime())
		fmt.Println("c.Table.Cnt():", c.Table.Count())
		fmt.Println("size:", c.size)
		c.Table = memorytable.NewTable()
		c.size = 0
	}
	return nil
}

func (c *Cake) Dump(table *memorytable.Table) error {

	firstIndex, secondIndex, dataFile, fileName, err := util.CreateTempFile(c.workspace, 0)
	if err != nil {
		return err
	}

	fmt.Println("fileName:", fileName)

	//  device count ,max device id and bloom filter
	// [ device count ][ max device id ] [ 0 0 0 0 0 0 ...]
	/*
		err = binary.Write(secondIndex, binary.BigEndian, table.Count())
		if err != nil {
			return err
		}
		err = binary.Write(secondIndex, binary.BigEndian, table.GetMaxDeviceId())
		if err != nil {
			return err
		}

		bloomSize := table.GetMaxDeviceId()/8 + 1
		bloom := make([]byte, bloomSize)
	*/

	var offset int
	err = table.GetDeviceSkipList().Traversal(func(deviceId int64, timeList *memorytable.TimeSkipList) error {

		// bloom[deviceId/8] = bloom[deviceId/8] & (1 << (deviceId % 8))

		//  secondIndex
		//  [ start time, end time,device id,timestamp count]...[ start time, end time,device id,timestamp count]
		//  Device id grow from small to large, facilitating binary search

		err := binary.Write(secondIndex, binary.BigEndian, timeList.GetStart())
		if err != nil {
			return err
		}

		err = binary.Write(secondIndex, binary.BigEndian, timeList.GetEnd())
		if err != nil {
			return err
		}

		err = binary.Write(secondIndex, binary.BigEndian, deviceId)
		if err != nil {
			return err
		}

		err = binary.Write(secondIndex, binary.BigEndian, timeList.Size())
		if err != nil {
			return err
		}

		err = timeList.Traversal(func(timestamp int64, data memorytable.Data) error {

			//  firstIndex
			// [ timestamp , offset , len ]...[ timestamp , offset , len ]
			// Timestamp from small to large, convenient binary search, as well as range query
			err := binary.Write(firstIndex, binary.BigEndian, timestamp)
			if err != nil {
				return err
			}

			err = binary.Write(firstIndex, binary.BigEndian, int64(offset))
			if err != nil {
				return err
			}

			err = binary.Write(firstIndex, binary.BigEndian, int64(len(data)))
			if err != nil {
				return err
			}

			// data
			n, err := dataFile.Write(data)
			offset += n
			return err
		})
		return err
	})

	if err != nil {
		return err
	}

	// copy to data path
	// workspace data level start-time end-time

	err = util.ReTempName(c.workspace, 0, fileName, 0, fileName)
	if err != nil {
		return err
	}

	// add file list
	//c.fileList.mutex.Lock()
	//defer c.fileList.mutex.Unlock()
	//c.fileList.List[0][fileName] = &FileStatus{
	//	IsCompact: false,
	//	mutex:     sync.Mutex{},
	//}
	return nil

}

func (c *Cake) ReadFromCache(query Query) (*memorytable.TimeSkipList, error) {
	retList := memorytable.NewTimeSkipList()
	// cache
	c.mutex.RLock()
	defer c.mutex.Unlock()
	for table := range c.queue {
		list, err := table.GetDeviceSkipList().Get(query.DeviceId)
		if err != nil {
			return nil, err
		}
		between, err := list.Between(query.StartTime.UnixNano(), query.EndTime.UnixNano())
		if err != nil {
			return nil, err
		}
		for _, v := range between {
			retList.Insert(v.Timestamp, v.Data)
		}
	}
	return retList, nil
}

func (c *Cake) ReadFromDisk(query Query) (*memorytable.TimeSkipList, error) {
	retList := memorytable.NewTimeSkipList()
	// cache
	c.mutex.RLock()
	defer c.mutex.Unlock()
	for table := range c.queue {
		list, err := table.GetDeviceSkipList().Get(query.DeviceId)
		if err != nil {
			return nil, err
		}
		between, err := list.Between(query.StartTime.UnixNano(), query.EndTime.UnixNano())
		if err != nil {
			return nil, err
		}
		for _, v := range between {
			retList.Insert(v.Timestamp, v.Data)
		}
	}
	return retList, nil
}

func (c *Cake) Read(query Query) ([]*data.BinaryData, error) {

	// disk
	return nil, nil
}
