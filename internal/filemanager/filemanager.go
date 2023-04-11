package filemanager

import (
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
	"strings"
	"sync"
)

type Group struct {
	workspace string
	files     map[string]struct{} // map[file_path]*file
	opened    map[string]*File    // map[file_path]*file
	mutex     sync.Mutex
}

func NewGroup(workspace, name string) *Group {
	err := os.MkdirAll(workspace+"/"+name, 0777)
	if err != nil {
		logrus.Fatalln(err)
	}
	logrus.Traceln("mkdir", workspace+"/"+name)
	return &Group{
		workspace: workspace + "/" + name,
		files:     make(map[string]struct{}), // filename : file
		opened:    make(map[string]*File),
	}
}

func (g *Group) CreateFile(name string) (*File, error) {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	file, err := NewFile(g.workspace, name)
	if err != nil {
		return nil, err
	}

	logrus.Infoln("create file", name)
	g.opened[name] = file
	g.files[name] = struct{}{}

	return file, nil
}

func (g *Group) OpenFile(name string) (*File, error) {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	shardSize, shardId, _, _, _ := GetFileInfoByName(name)
	err := os.MkdirAll(fmt.Sprintf("%s/%010d/%010d/", g.workspace, shardSize, shardId), 0777)
	if err != nil {
		return nil, err
	}

	// open file
	firstIndex, err := os.Open(fmt.Sprintf("%s/%010d/%010d/%s.first_index", g.workspace, shardSize, shardId, name))
	if err != nil {
		_ = firstIndex.Close()
		return nil, err
	}

	secondIndex, err := os.Open(fmt.Sprintf("%s/%010d/%010d/%s.second_index", g.workspace, shardSize, shardId, name))
	if err != nil {
		_ = firstIndex.Close()
		_ = secondIndex.Close()
		return nil, err
	}

	data, err := os.Open(fmt.Sprintf("%s/%010d/%010d/%s.data", g.workspace, shardSize, shardId, name))
	if err != nil {
		_ = firstIndex.Close()
		_ = secondIndex.Close()
		_ = data.Close()
		return nil, err
	}

	file := &File{
		FirstIndex:  firstIndex,
		SecondIndex: secondIndex,
		DataFile:    data,
		shardId:     shardId,
		shardSize:   shardSize,
		name:        name,
	}

	logrus.Infoln("open file", name)
	g.opened[name] = file
	g.files[name] = struct{}{}

	return file, nil
}

func (g *Group) CloseFile(name string) error {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	err := g.opened[name].Close()
	if err != nil {
		return err
	}
	logrus.Infoln("close file", name)

	delete(g.opened, name)
	return nil
}

func (g *Group) Add(name string) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	g.files[name] = struct{}{}
	logrus.Infoln("add file:", name)
}

func (g *Group) Del(name string) error {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	if _, ok := g.opened[name]; ok {
		return errors.New(name + " file busy")
	}

	_ = removeFile(g.workspace, name)

	delete(g.files, name)
	delete(g.opened, name)
	logrus.Infoln("del file:", name)
	return nil
}

func (g *Group) GetCompactFileList(shardSize, nextShardSize int, maxId int) map[int][]string {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	ret := map[int][]string{}
	for i := range g.files {
		size, id, _, _, _ := GetFileInfoByName(i)
		if shardSize == size && id < maxId {
			id := id * shardSize / nextShardSize
			ret[id] = append(ret[id], i)
		}
	}
	return ret
}

func (g *Group) InitDataFile() error {
	dir, err := os.ReadDir(g.workspace) // shard_size
	if err != nil {
		return err
	}
	for _, i := range dir {
		dir, err := os.ReadDir(g.workspace + "/" + i.Name()) // shard_id
		if err != nil {
			return err
		}
		for _, j := range dir {
			dir, err := os.ReadDir(g.workspace + "/" + i.Name() + "/" + j.Name()) // shard_id
			if err != nil {
				return err
			}
			for _, k := range dir {
				name := k.Name()

				if strings.HasSuffix(name, ".data") {
					logrus.Traceln("InitDataFile", name)
					name = name[:len(name)-5]
					g.Add(name)
				}

			}

		}
	}
	return nil
}

type FileManager struct {
	workspace        string
	dataDir, tempDir *Group
	mutex            sync.RWMutex
}

func NewFileManager(w string) *FileManager {
	obj := &FileManager{
		workspace: w,
		dataDir:   NewGroup(w, "data"),
		tempDir:   NewGroup(w, "tmp"),
		mutex:     sync.RWMutex{},
	}
	err := obj.InitDataDir()
	if err != nil {
		logrus.Fatalln(err)
	}
	return obj
}

func (fm *FileManager) InitDataDir() error {
	return fm.dataDir.InitDataFile()
}

func (fm *FileManager) GetCompactFileList(shardSize, next, maxId int) map[int][]string {
	return fm.dataDir.GetCompactFileList(shardSize, next, maxId)
}

func (fm *FileManager) CreateTempFile(name string) (*File, error) {
	return fm.tempDir.CreateFile(name)
}

func (fm *FileManager) OpenDataFile(name string) (*File, error) {
	return fm.dataDir.OpenFile(name)
}
func (fm *FileManager) CloseDataFile(name string) error {
	return fm.dataDir.CloseFile(name)
}

func (fm *FileManager) Del(name string) error {
	return fm.dataDir.Del(name)
}

func (fm *FileManager) Rename(name string) error {
	size, id, _, _, _ := GetFileInfoByName(name)
	err := fm.tempDir.CloseFile(name)
	if err != nil {
		logrus.Errorln(err)
	}

	err = fm.reTempName(size, id, name)
	if err != nil {
		return err
	}
	_ = fm.tempDir.Del(name)
	fm.dataDir.Add(name)
	return nil
}

func (fm *FileManager) reTempName(shardGroup int, shardGroupId int, src string) error {
	_ = os.MkdirAll(fmt.Sprintf("%s/data/%010d/%010d/", fm.workspace, shardGroup, shardGroupId), 0777)
	err := os.Rename(fmt.Sprintf("%s/tmp/%010d/%010d/%s.data", fm.workspace, shardGroup, shardGroupId, src),
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

//func (fm *FileManager) CreateTempFile(shardGroup int, shardGroupId int) (*os.File, *os.File, *os.File, string, error) {
//	fm.mutex.Lock()
//	defer fm.mutex.Unlock()
//
//	err := os.MkdirAll(fmt.Sprintf("%s/tmp/%010d/%010d/", fm.workspace, shardGroup, shardGroupId), 0777)
//	if err != nil {
//		return nil, nil, nil, "", err
//	}
//
//	t := time.Now().UnixNano()
//	random := rand.Int()
//
//	FirstIndex, err := os.OpenFile(fmt.Sprintf("%s/tmp/%010d/%010d/%d-%d.first_index", fm.workspace, shardGroup, shardGroupId, t, random), os.O_CREATE|os.O_WRONLY, 0666)
//	if err != nil {
//		return nil, nil, nil, "", err
//	}
//
//	SecondIndex, err := os.OpenFile(fmt.Sprintf("%s/tmp/%010d/%010d/%d-%d.second_index", fm.workspace, shardGroup, shardGroupId, t, random), os.O_CREATE|os.O_WRONLY, 0666)
//	if err != nil {
//		_ = FirstIndex.Close()
//		return nil, nil, nil, "", err
//	}
//
//	DataFile, err := os.OpenFile(fmt.Sprintf("%s/tmp/%010d/%010d/%d-%d.data", fm.workspace, shardGroup, shardGroupId, t, random), os.O_CREATE|os.O_WRONLY, 0666)
//	if err != nil {
//		_ = FirstIndex.Close()
//		_ = SecondIndex.Close()
//		return nil, nil, nil, "", err
//	}
//	return FirstIndex, SecondIndex, DataFile, fmt.Sprintf("%d-%d", t, random), nil
//}

//func (fm *FileManager) OpenDataFile(i int, j string, name string) (*os.File, *os.File, *os.File, error) {
//	fm.mutex.Lock()
//	defer fm.mutex.Unlock()
//
//	FirstIndex, err := os.Open(fmt.Sprintf("%s/data/%010d/%s/%s.first_index", fm.workspace, i, j, name))
//	if err != nil {
//		return nil, nil, nil, err
//	}
//	SecondIndex, err := os.Open(fmt.Sprintf("%s/data/%010d/%s/%s.second_index", fm.workspace, i, j, name))
//	if err != nil {
//		_ = FirstIndex.Close()
//		return nil, nil, nil, err
//	}
//	DataFile, err := os.Open(fmt.Sprintf("%s/data/%010d/%s/%s.data", fm.workspace, i, j, name))
//	if err != nil {
//		_ = FirstIndex.Close()
//		_ = SecondIndex.Close()
//		return nil, nil, nil, err
//	}
//	return FirstIndex, SecondIndex, DataFile, nil
//}

//func (fm *FileManager) IsDataFileExist(size int, shardId, name string) bool {
//	fm.mutex.RLock()
//	defer fm.mutex.RUnlock()
//
//	_, err := os.Stat(fmt.Sprintf("%s/data/%010d/%s/%s.first_index", fm.workspace, size, shardId, name))
//	if err != nil {
//		return false
//	}
//	_, err = os.Stat(fmt.Sprintf("%s/data/%010d/%s/%s.second_index", fm.workspace, size, shardId, name))
//	if err != nil {
//		return false
//	}
//	_, err = os.Stat(fmt.Sprintf("%s/data/%010d/%s/%s.data", fm.workspace, size, shardId, name))
//	if err != nil {
//		return false
//	}
//	return true
//}

//func (fm *FileManager) Rename(shardGroup int, shardGroupId int, targetShardGroup, targetShardGroupId int, src string) error {
//	fm.mutex.Lock()
//	defer fm.mutex.Unlock()
//	err := os.MkdirAll(fmt.Sprintf("%s/data/%010d/%010d/", fm.workspace, shardGroup, shardGroupId), 0777)
//	if err != nil {
//		return err
//	}
//	err = os.MkdirAll(fmt.Sprintf("%s/data/%010d/%010d/", fm.workspace, targetShardGroup, targetShardGroupId), 0777)
//	if err != nil {
//		return err
//	}
//	err = os.Rename(fmt.Sprintf("%s/tmp/%010d/%010d/%s.data", fm.workspace, shardGroup, shardGroupId, src),
//		fmt.Sprintf("%s/data/%010d/%010d/%s.data", fm.workspace, targetShardGroup, targetShardGroupId, src))
//	if err != nil {
//		return err
//	}
//	err = os.Rename(fmt.Sprintf("%s/tmp/%010d/%010d/%s.first_index", fm.workspace, shardGroup, shardGroupId, src),
//		fmt.Sprintf("%s/data/%010d/%010d/%s.first_index", fm.workspace, targetShardGroup, targetShardGroupId, src))
//	if err != nil {
//		return err
//	}
//	err = os.Rename(fmt.Sprintf("%s/tmp/%010d/%010d/%s.second_index", fm.workspace, shardGroup, shardGroupId, src),
//		fmt.Sprintf("%s/data/%010d/%010d/%s.second_index", fm.workspace, targetShardGroup, targetShardGroupId, src))
//	if err != nil {
//		return err
//	}
//	return nil
//}
