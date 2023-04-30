package engine

import (
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/mmap"
	"io"
	"os"
	"strings"
	"time"
)

func (c *Cake) InitKey() {
	c.mu.Lock()
	defer c.mu.Unlock()
	cancel := make(chan struct{})
	t := time.Now()
	keys := c.dataDisk.Keys(cancel)
	for i := range keys {
		if strings.HasSuffix(i, "index") {
			c.Keys[i] = struct{}{}
			_ = c.addCache(i)
			_ = c.addCache(i[:len(i)-5] + "data")
		}
	}
	c.Cache.OnEvicted(func(s string, i interface{}) {
		at := i.(*mmap.ReaderAt)
		if at != nil {
			if err := at.Close(); err != nil {
				panic(err)
			}
		}
	})
	logrus.Infoln(c.Keys)
	logrus.Infoln("init key:", len(c.Keys), time.Now().UnixMilli()-t.UnixMilli(), "ms")
}

func (c *Cake) DelFile(indexKey, dataKey string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	err := c.dataDisk.Erase(indexKey)
	if err != nil {
		return err
	}
	delete(c.Keys, indexKey)
	c.Cache.Delete(indexKey)
	c.Cache.Delete(dataKey)
	return nil
}

func (c *Cake) AddFile(indexKey, dataKey string, cb func(dataFile, indexFile io.Writer)) {
	indexFile, err := os.CreateTemp("", indexKey)
	if err != nil {
		panic(err)
	}
	defer func() {
		indexFile.Close()
		os.Remove(indexFile.Name())
	}()

	dataFile, err := os.CreateTemp("", dataKey)
	if err != nil {
		panic(err)
	}
	defer func() {
		dataFile.Close()
		os.Remove(dataFile.Name())
	}()

	cb(dataFile, indexFile)

	err = c.dataDisk.Import(dataFile.Name(), dataKey, true)
	if err != nil {
		panic(err)
	}
	err = c.dataDisk.Import(indexFile.Name(), indexKey, true)
	if err != nil {
		panic(err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.Keys[indexKey] = struct{}{}
}

func (c *Cake) ReadFile(indexKey string, cb func(at *mmap.ReaderAt)) {

	v, ok := c.Cache.Get(indexKey)
	if !ok {
		v = c.addCache(indexKey)
	}
	at := v.(*mmap.ReaderAt)
	c.mu.RLock()
	defer c.mu.RUnlock()

	cb(at)
}

func (c *Cake) addCache(indexKey string) *mmap.ReaderAt {
	path := c.Optional.DataCachePath + "/" + strings.Join(Transform(indexKey), "/") + "/" + indexKey
	at, err := mmap.Open(path)
	if err != nil {
		return nil
	}
	_ = c.Cache.Add(indexKey, at, time.Hour)
	return at
}
