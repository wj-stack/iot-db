package engine

import (
	"fmt"
	"github.com/dablelv/go-huge-util/conv"
	"github.com/juju/errors"
	"github.com/sirupsen/logrus"
	"math"
	"os"
	"strings"
)

type File struct {
	FirstIndexFile  *os.File
	SecondIndexFile *os.File
	DataFile        *os.File
	Name            string
}

func (Fd *File) close() error {
	if Fd.DataFile != nil {
		err := Fd.DataFile.Close()
		if err != nil {
			return err
		}
	}
	if Fd.FirstIndexFile != nil {
		err := Fd.FirstIndexFile.Close()
		if err != nil {
			return err
		}
	}
	if Fd.SecondIndexFile != nil {
		err := Fd.SecondIndexFile.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func OpenFile(workspace string, name string) (*File, error) {

	// open file
	firstIndex, err := os.OpenFile(fmt.Sprintf("%s/%s.index", workspace, name), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	data, err := os.OpenFile(fmt.Sprintf("%s/%s.data", workspace, name), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		_ = firstIndex.Close()
		return nil, err
	}

	second, err := os.OpenFile(fmt.Sprintf("%s/%s.s_index", workspace, name), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		_ = firstIndex.Close()
		_ = data.Close()
		return nil, err
	}

	return &File{
		FirstIndexFile:  firstIndex,
		SecondIndexFile: second,
		DataFile:        data,
		Name:            name,
	}, nil
}

func (c *Cake) createTempFile(name string) (*File, error) {
	file, err := OpenFile(c.Optional.TempFilePath, name)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (c *Cake) saveTempFile(f *File) error {

	// rename
	err := os.Rename(fmt.Sprintf("%s/%s.data", c.Optional.TempFilePath, f.Name),
		fmt.Sprintf("%s/%s.data", c.Optional.DataFilePath, f.Name))
	if err != nil {
		return err
	}
	err = os.Rename(fmt.Sprintf("%s/%s.index", c.Optional.TempFilePath, f.Name),
		fmt.Sprintf("%s/%s.index", c.Optional.DataFilePath, f.Name))
	if err != nil {
		return err
	}
	err = os.Rename(fmt.Sprintf("%s/%s.s_index", c.Optional.TempFilePath, f.Name),
		fmt.Sprintf("%s/%s.s_index", c.Optional.DataFilePath, f.Name))
	if err != nil {
		return err
	}

	c.fileMutex.Lock()
	defer c.fileMutex.Unlock()
	_, ok := c.files[f.Name]
	if ok {
		return errors.AlreadyExists
	}
	c.files[f.Name] = 0
	return nil
}

func (c *Cake) openFiles(start, end int64) []string {
	c.fileMutex.Lock()
	defer c.fileMutex.Unlock()

	//fileName := fmt.Sprintf("%d_%d_%d", start, end, time.Now().UnixNano())

	var ret []string
	for name := range c.files {

		arr := strings.Split(name, "_")
		a := conv.ToAny[int64](arr[0])
		b := conv.ToAny[int64](arr[1])
		if a > end || b < start {
			continue
		}
		c.files[name]++
		ret = append(ret, name)
	}

	return ret
}

func (c *Cake) getNoiseFile(size int64) ([]string, int64, int64) {
	c.fileMutex.Lock()
	defer c.fileMutex.Unlock()

	//fileName := fmt.Sprintf("%d_%d_%d", start, end, time.Now().UnixNano())
	var start = int64(math.MaxInt64)
	var end = int64(math.MinInt64)
	var ret []string
	for name := range c.files {

		stat, err := os.Stat(fmt.Sprintf("%s/%s.data", c.Optional.DataFilePath, name))
		if err != nil {
			return nil, 0, 0
		}
		logrus.Info(stat.Size(), size, name)
		if stat.Size() < size {
			logrus.Infoln("noise file:", name)
			ret = append(ret, name)
			arr := strings.Split(name, "_")
			a := conv.ToAny[int64](arr[0])
			b := conv.ToAny[int64](arr[1])
			if a < start {
				start = a
			}
			if b > end {
				end = b
			}
			c.files[name]++

		}

	}

	return ret, start, end
}

func (c *Cake) closeFiles(files []string) {
	c.fileMutex.Lock()
	defer c.fileMutex.Unlock()

	for _, f := range files {
		c.files[f]--
	}

}

func (c *Cake) tryRemoveFiles(f string) bool {
	c.fileMutex.Lock()
	defer c.fileMutex.Unlock()
	logrus.Infoln(c.files[f], f, "opened")
	for c.files[f] == 0 {
		logrus.Infoln("remove file", f)
		os.Remove(fmt.Sprintf("%s/%s.index", c.Optional.DataFilePath, f))
		os.Remove(fmt.Sprintf("%s/%s.data", c.Optional.DataFilePath, f))
		os.Remove(fmt.Sprintf("%s/%s.s_index", c.Optional.DataFilePath, f))
		delete(c.files, f)
		return true
	}

	return false

}
