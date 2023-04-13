package filemanager

import (
	"fmt"
	errors "github.com/juju/errors"
	"github.com/sirupsen/logrus"
	"iot-db/internal/config"
	"os"
	"strings"
	"sync"
)

type FileManager struct {
	Config *config.Config
	mutex  sync.RWMutex
	files  map[string]struct{}
	opened map[string]*File
}

func NewFileManager(Config *config.Config) *FileManager {
	obj := &FileManager{
		Config: Config,
		mutex:  sync.RWMutex{},
		files:  map[string]struct{}{},
		opened: map[string]*File{},
	}
	err := obj.InitDir()
	if err != nil {
		logrus.Fatalln(err)
	}
	return obj
}

func (fm *FileManager) InitDir() error {
	logrus.Infoln("init dir")
	// mkdir path
	err := os.MkdirAll(fm.Config.Core.Path.Data, 0777)
	if err != nil {
		return err
	}
	err = os.MkdirAll(fm.Config.Core.Path.Wal, 0777)
	if err != nil {
		return err
	}
	err = os.MkdirAll(fm.Config.Core.Path.Rubbish, 0777)
	if err != nil {
		return err
	}

	workspace := fm.Config.Core.Path.Data

	// add file to fm
	dir, err := os.ReadDir(workspace) // shard_size
	if err != nil {
		return err
	}

	for _, i := range dir {
		dir, err := os.ReadDir(workspace + "/" + i.Name()) // shard_id
		if err != nil {
			return err
		}
		for _, j := range dir {
			dir, err := os.ReadDir(workspace + "/" + i.Name() + "/" + j.Name()) // shard_id
			if err != nil {
				return err
			}

			// remove empty dir
			if len(dir) == 0 {
				err := os.Remove(workspace + "/" + i.Name() + "/" + j.Name())
				if err != nil {
					return err
				}
			}

			for _, k := range dir {
				name := k.Name()

				if strings.HasSuffix(name, ".data") {
					name = name[:len(name)-5]
					err := fm.AddFile(name)
					if err != nil {
						return err
					}
				}

			}

		}
	}

	return nil
}

func (fm *FileManager) AddFile(name string) error {
	logrus.Infoln("add file:", name)
	fm.mutex.Lock()
	defer fm.mutex.Unlock()
	if _, ok := fm.files[name]; ok {
		return errors.AlreadyExists
	}
	fm.files[name] = struct{}{}
	return nil
}

func (fm *FileManager) DelFile(name string) error {
	logrus.Infoln("del file:", name)
	fm.mutex.Lock()
	defer fm.mutex.Unlock()
	if _, ok := fm.opened[name]; ok {
		return errors.Annotate(errors.Forbidden, "file is opened!")
	}
	if _, ok := fm.files[name]; !ok {
		return errors.NotFound
	}
	delete(fm.files, name)
	return nil
}

func (fm *FileManager) OpenFile(name string) (*File, error) {
	logrus.Infoln("open file:", name)

	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	file, err := OpenFile(fm.Config.Core.Path.Data, name)
	if err != nil {
		return nil, err
	}

	fm.opened[name] = file
	fm.files[name] = struct{}{}

	return file, nil
}

func (fm *FileManager) CloseFile(name string) error {
	logrus.Infoln("close file:", name)

	fm.mutex.Lock()
	defer fm.mutex.Unlock()
	err := fm.opened[name].Close()
	if err != nil {
		return err
	}

	delete(fm.opened, name)
	return nil
}

func (fm *FileManager) CreateTempFile(name string) (*File, error) {
	file, err := OpenFile(fm.Config.Core.Path.Rubbish, name)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (fm *FileManager) SaveTempFile(f *File) error {
	err := f.Close()
	if err != nil {
		return err
	}
	err = fm.rename(f.name)
	if err != nil {
		return err
	}
	return fm.AddFile(f.name)
}

func (fm *FileManager) GetCompactFileList(shardSize, nextShardSize int, maxId int) map[int][]string {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()
	ret := map[int][]string{}
	for i := range fm.files {
		size, id, _, _, _ := GetFileInfoByName(i)
		if shardSize == size && id < maxId {
			id := id * shardSize / nextShardSize
			ret[id] = append(ret[id], i)
		}
	}
	return ret
}

func (fm *FileManager) Remove(name string) error {
	size, id, _, _, _ := GetFileInfoByName(name)
	err := fm.DelFile(name)
	if err != nil {
		return err
	}
	path := fmt.Sprintf("%s/%010d/%010d/%s.data", fm.Config.Core.Path.Data, size, id, name)
	return os.Remove(path)
}

func (fm *FileManager) rename(name string) error {
	size, id, _, _, _ := GetFileInfoByName(name)
	return fm.ReTempName(size, id, name)
}

func (fm *FileManager) ReTempName(shardGroup int, shardGroupId int, src string) error {
	err := os.MkdirAll(fmt.Sprintf("%s/%010d/%010d", fm.Config.Core.Path.Data, shardGroup, shardGroupId), 0777)
	if err != nil {
		return err
	}

	err = os.Rename(fmt.Sprintf("%s/%010d/%010d/%s.data", fm.Config.Core.Path.Rubbish, shardGroup, shardGroupId, src),
		fmt.Sprintf("%s/%010d/%010d/%s.data", fm.Config.Core.Path.Data, shardGroup, shardGroupId, src))
	if err != nil {
		return err
	}
	err = os.Rename(fmt.Sprintf("%s/%010d/%010d/%s.first_index", fm.Config.Core.Path.Rubbish, shardGroup, shardGroupId, src),
		fmt.Sprintf("%s/%010d/%010d/%s.first_index", fm.Config.Core.Path.Data, shardGroup, shardGroupId, src))
	if err != nil {
		return err
	}
	err = os.Rename(fmt.Sprintf("%s/%010d/%010d/%s.second_index", fm.Config.Core.Path.Rubbish, shardGroup, shardGroupId, src),
		fmt.Sprintf("%s/%010d/%010d/%s.second_index", fm.Config.Core.Path.Data, shardGroup, shardGroupId, src))
	if err != nil {
		return err
	}
	return nil
}
