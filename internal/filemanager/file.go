package filemanager

import (
	"fmt"
	"github.com/dablelv/go-huge-util/conv"
	"github.com/sirupsen/logrus"
	"os"
	"strings"
	"sync"
)

type File struct {
	FirstIndex  *os.File
	SecondIndex *os.File
	DataFile    *os.File
	shardId     int
	shardSize   int
	name        string
	mutex       sync.RWMutex
}

// name: data_(shard_size)_(shard_id)_(start)_(end)_(createdAt)

func NewFileName(shardSize, shardId, start, end, created int) string {
	return fmt.Sprintf("%d_%d_%d_%d_%d", shardSize, shardId, start, end, created)
}

func GetFileInfoByName(name string) (shardSize, shardId, start, end, created int) {
	split := strings.Split(name, "_")
	return conv.ToAny[int](split[0]), conv.ToAny[int](split[1]), conv.ToAny[int](split[2]), conv.ToAny[int](split[3]), conv.ToAny[int](split[4])
}

func NewFile(workspace string, name string) (*File, error) {

	shardSize, shardId, _, _, _ := GetFileInfoByName(name)
	err := os.MkdirAll(fmt.Sprintf("%s/%010d/%010d/", workspace, shardSize, shardId), 0777)
	if err != nil {
		return nil, err
	}

	// open file
	firstIndex, err := os.OpenFile(fmt.Sprintf("%s/%010d/%010d/%s.first_index", workspace, shardSize, shardId, name), os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		_ = firstIndex.Close()
		return nil, err
	}

	secondIndex, err := os.OpenFile(fmt.Sprintf("%s/%010d/%010d/%s.second_index", workspace, shardSize, shardId, name), os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		_ = firstIndex.Close()
		_ = secondIndex.Close()
		return nil, err
	}

	data, err := os.OpenFile(fmt.Sprintf("%s/%010d/%010d/%s.data", workspace, shardSize, shardId, name), os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		_ = firstIndex.Close()
		_ = secondIndex.Close()
		_ = data.Close()
		return nil, err
	}

	return &File{
		FirstIndex:  firstIndex,
		SecondIndex: secondIndex,
		DataFile:    data,
		shardId:     shardId,
		shardSize:   shardSize,
		name:        name,
	}, nil
}

func (Fd *File) Close() error {
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

func removeFile(workspace string, name string) error {

	logrus.Infoln("remove", name)

	shardSize, shardId, _, _, _ := GetFileInfoByName(name)
	err := os.Remove(fmt.Sprintf("%s/%010d/%010d/%s.first_index", workspace, shardSize, shardId, name))
	if err != nil {
		return err
	}
	err = os.Remove(fmt.Sprintf("%s/%010d/%010d/%s.second_index", workspace, shardSize, shardId, name))
	if err != nil {
		return err
	}
	err = os.Remove(fmt.Sprintf("%s/%010d/%010d/%s.data", workspace, shardSize, shardId, name))
	if err != nil {
		return err
	}
	return nil
}
