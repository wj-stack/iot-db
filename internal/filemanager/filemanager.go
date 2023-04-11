package filemanager

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"math/rand"
	"os"
	"sync"
	"time"
)

type File struct {
	FirstIndex  *os.File
	SecondIndex *os.File
	DataFile    *os.File
	Path        string // full path : xxx/data/shard_size/shard_id/timestamp-rand
	Dir         string // bbb
}

func (Fd *File) CloseFile() error {
	if Fd.DataFile != nil {
		err := Fd.DataFile.Close()
		if err != nil {
			return err
		}
	}
	if Fd.FirstIndex != nil {
		err := Fd.FirstIndex.Close()
		if err != nil {
			return err
		}
	}
	if Fd.SecondIndex != nil {
		err := Fd.SecondIndex.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

type Group struct {
	name      string
	workspace string
	Dir       map[string]map[string]*File
}

func NewGroup(workspace, name string) *Group {
	err := os.MkdirAll(workspace+"/"+name, 0777)
	if err != nil {
		logrus.Fatalln(err)
	}

	return &Group{
		name:      name,
		workspace: workspace + "/" + name,
		Dir:       make(map[string]map[string]*File),
	}
}

func (g Group) CreateFile(dir string, file string) (*File, error) {

	// mkdir
	dirPath := g.workspace + "/" + dir
	err := os.MkdirAll(dirPath, 0777)
	if err != nil {
		logrus.Fatalln(err)
	}

	// open file
	firstIndex, err := os.OpenFile(dirPath+"/"+file+".first_index", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		_ = firstIndex.Close()
		return nil, err
	}

	secondIndex, err := os.OpenFile(dirPath+"/"+file+".second_index", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		_ = firstIndex.Close()
		_ = secondIndex.Close()
		return nil, err
	}

	data, err := os.OpenFile(dirPath+"/"+file+".data", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		_ = firstIndex.Close()
		_ = secondIndex.Close()
		_ = data.Close()
		return nil, err
	}

	// add to map
	var fileList map[string]*File
	var ok bool
	if fileList, ok = g.Dir[file]; !ok {
		g.Dir[file] = make(map[string]*File)
		fileList = g.Dir[file]
	}

	f := &File{
		FirstIndex:  firstIndex,
		SecondIndex: secondIndex,
		DataFile:    data,
		Path:        dirPath + "/" + file,
	}
	fileList[file] = f

	return f, nil
}

func (g Group) Remove(dir, file string) error {
	err := os.Remove(file + ".first_index")
	if err != nil {
		return err
	}
	err = os.Remove(file + ".second_index")
	if err != nil {
		return err
	}
	err = os.Remove(file + ".data")
	if err != nil {
		return err
	}
	return nil
}

type FileManager struct {
	workspace        string
	dataDir, tempDir *Group
	mutex            sync.RWMutex
}

func NewFileManager(w string) *FileManager {
	return &FileManager{
		workspace: w,
		dataDir:   NewGroup(w, "data"),
		tempDir:   NewGroup(w, "tmp"),
		mutex:     sync.RWMutex{},
	}
}

func (fm *FileManager) AddFile(dir, name string) {

}

func (fm *FileManager) GetDataDirList(shardSize int64) []string {

	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	dirPath := fmt.Sprintf("%s/data/%010d", fm.workspace, shardSize)
	dir, err := os.ReadDir(dirPath)
	if err != nil {
		return nil
	}
	var ret []string
	for _, i := range dir {

		dir, err := os.ReadDir(dirPath + "/" + i.Name())
		if err != nil {
			continue
		}
		if len(dir) > 0 {
			ret = append(ret, i.Name())
		} else {
			// Empty folder
			os.Remove(dirPath)
		}
	}
	return ret
}

func (fm *FileManager) GetDataFileList(shardSize int64, shardId string) []string {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	fm.mutex.RLock()
	defer fm.mutex.RUnlock()

	dir, err := os.ReadDir(fmt.Sprintf("%s/data/%010d/%s/", fm.workspace, shardSize, shardId))
	if err != nil {
		return nil
	}
	var ret []string
	for _, i := range dir {
		ret = append(ret, i.Name())
	}
	return ret
}

func (fm *FileManager) CreateTempFile(shardGroup int, shardGroupId int) (*os.File, *os.File, *os.File, string, error) {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	err := os.MkdirAll(fmt.Sprintf("%s/tmp/%010d/%010d/", fm.workspace, shardGroup, shardGroupId), 0777)
	if err != nil {
		return nil, nil, nil, "", err
	}

	t := time.Now().UnixNano()
	random := rand.Int()

	firstIndex, err := os.OpenFile(fmt.Sprintf("%s/tmp/%010d/%010d/%d-%d.first_index", fm.workspace, shardGroup, shardGroupId, t, random), os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return nil, nil, nil, "", err
	}

	secondIndex, err := os.OpenFile(fmt.Sprintf("%s/tmp/%010d/%010d/%d-%d.second_index", fm.workspace, shardGroup, shardGroupId, t, random), os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		_ = firstIndex.Close()
		return nil, nil, nil, "", err
	}

	dataFile, err := os.OpenFile(fmt.Sprintf("%s/tmp/%010d/%010d/%d-%d.data", fm.workspace, shardGroup, shardGroupId, t, random), os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		_ = firstIndex.Close()
		_ = secondIndex.Close()
		return nil, nil, nil, "", err
	}
	return firstIndex, secondIndex, dataFile, fmt.Sprintf("%d-%d", t, random), nil
}

func (fm *FileManager) OpenDataFile(i int, j string, name string) (*os.File, *os.File, *os.File, error) {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	firstIndex, err := os.Open(fmt.Sprintf("%s/data/%010d/%s/%s.first_index", fm.workspace, i, j, name))
	if err != nil {
		return nil, nil, nil, err
	}
	secondIndex, err := os.Open(fmt.Sprintf("%s/data/%010d/%s/%s.second_index", fm.workspace, i, j, name))
	if err != nil {
		_ = firstIndex.Close()
		return nil, nil, nil, err
	}
	dataFile, err := os.Open(fmt.Sprintf("%s/data/%010d/%s/%s.data", fm.workspace, i, j, name))
	if err != nil {
		_ = firstIndex.Close()
		_ = secondIndex.Close()
		return nil, nil, nil, err
	}
	return firstIndex, secondIndex, dataFile, nil
}

func (fm *FileManager) IsDataFileExist(size int, shardId, name string) bool {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()

	_, err := os.Stat(fmt.Sprintf("%s/data/%010d/%s/%s.first_index", fm.workspace, size, shardId, name))
	if err != nil {
		return false
	}
	_, err = os.Stat(fmt.Sprintf("%s/data/%010d/%s/%s.second_index", fm.workspace, size, shardId, name))
	if err != nil {
		return false
	}
	_, err = os.Stat(fmt.Sprintf("%s/data/%010d/%s/%s.data", fm.workspace, size, shardId, name))
	if err != nil {
		return false
	}
	return true
}

func (fm *FileManager) ReTempName(shardGroup int, shardGroupId int, src string) error {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()
	err := os.MkdirAll(fmt.Sprintf("%s/data/%010d/%010d/", fm.workspace, shardGroup, shardGroupId), 0777)
	if err != nil {
		return err
	}

	err = os.Rename(fmt.Sprintf("%s/tmp/%010d/%010d/%s.data", fm.workspace, shardGroup, shardGroupId, src),
		fmt.Sprintf("%s/data/%010d/%010d/%s.data", fm.workspace, shardGroup, shardGroupId, src))
	if err != nil {
		return err
	}
	err = os.Rename(fmt.Sprintf("%s/tmp/%010d/%010d/%s.first_index", fm.workspace, shardGroup, shardGroupId, src),
		fmt.Sprintf("%s/data/%010d/%010d/%s.first_index", fm.workspace, shardGroup, shardGroupId, src))
	if err != nil {
		return err
	}
	err = os.Rename(fmt.Sprintf("%s/tmp/%010d/%010d/%s.second_index", fm.workspace, shardGroup, shardGroupId, src),
		fmt.Sprintf("%s/data/%010d/%010d/%s.second_index", fm.workspace, shardGroup, shardGroupId, src))
	if err != nil {
		return err
	}
	return nil
}

func (fm *FileManager) Rename(shardGroup int, shardGroupId int, targetShardGroup, targetShardGroupId int, src string) error {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()
	err := os.MkdirAll(fmt.Sprintf("%s/data/%010d/%010d/", fm.workspace, shardGroup, shardGroupId), 0777)
	if err != nil {
		return err
	}
	err = os.MkdirAll(fmt.Sprintf("%s/data/%010d/%010d/", fm.workspace, targetShardGroup, targetShardGroupId), 0777)
	if err != nil {
		return err
	}
	err = os.Rename(fmt.Sprintf("%s/tmp/%010d/%010d/%s.data", fm.workspace, shardGroup, shardGroupId, src),
		fmt.Sprintf("%s/data/%010d/%010d/%s.data", fm.workspace, targetShardGroup, targetShardGroupId, src))
	if err != nil {
		return err
	}
	err = os.Rename(fmt.Sprintf("%s/tmp/%010d/%010d/%s.first_index", fm.workspace, shardGroup, shardGroupId, src),
		fmt.Sprintf("%s/data/%010d/%010d/%s.first_index", fm.workspace, targetShardGroup, targetShardGroupId, src))
	if err != nil {
		return err
	}
	err = os.Rename(fmt.Sprintf("%s/tmp/%010d/%010d/%s.second_index", fm.workspace, shardGroup, shardGroupId, src),
		fmt.Sprintf("%s/data/%010d/%010d/%s.second_index", fm.workspace, targetShardGroup, targetShardGroupId, src))
	if err != nil {
		return err
	}
	return nil
}

func (fm *FileManager) Remove(path string) error {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()
	err := os.Remove(path + ".first_index")
	if err != nil {
		return err
	}
	err = os.Remove(path + ".second_index")
	if err != nil {
		return err
	}
	err = os.Remove(path + ".data")
	if err != nil {
		return err
	}
	return nil
}
