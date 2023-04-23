package engine

import (
	"fmt"
	"github.com/dablelv/go-huge-util/conv"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)

func (c *Cake) compact(shardId int64, keys []string) {
	dataStreams := map[string]io.ReadCloser{}

	var lastDid uint32
	var lastTimestamp uint64
	created := time.Now().UnixNano()
	dataKey := fmt.Sprintf("%d_%d_%d_data", ShardGroupSize, shardId, created)
	indexKey := fmt.Sprintf("%d_%d_%d_index", ShardGroupSize, shardId, created)

	indexFile, err := os.CreateTemp("", indexKey)
	if err != nil {
		panic(err)
	}
	defer func() {
		os.Remove(indexFile.Name())
	}()

	dataFile, err := os.CreateTemp("", dataKey)
	if err != nil {
		panic(err)
	}
	defer func() {
		os.Remove(dataFile.Name())
	}()

	var pipelines []chan IndexAndKey
	for _, key := range keys {
		dataKey := key[:len(key)-5] + "data"
		_, ok := dataStreams[dataKey]
		if !ok {
			var err error
			dataStreams[dataKey], err = c.dataDisk.ReadStream(dataKey, false)
			if err != nil {
				panic(err)
			}
		}
		pipelines = append(pipelines, c.openIndexPipeline(key))
	}

	indexChan := MergeN(pipelines...)
	logrus.Infoln("len(pipelines):", len(pipelines))
	cnt := 0
	//defer pipeWriter.Close()
	defer indexFile.Close()
	defer dataFile.Close()
	defer func() {
		for _, v := range dataStreams {
			_ = v.Close()
		}
	}()
	size := 0
	t := time.Now()

	buf := make([]byte, 1024*5)
	for index := range indexChan {
		cnt++
		if int(index.Length) > len(buf) {
			buf = make([]byte, index.Length*2)
		}
		n, err := dataStreams[index.DataKey].Read(buf[:index.Length])
		if err != nil {
			panic(err)
		}

		if index.Timestamp != lastTimestamp || index.Did != lastDid {
			lastTimestamp = index.Timestamp
			lastDid = index.Did
			_, err := dataFile.Write(buf[:index.Length])
			if err != nil {
				panic(err)
			}
			index := &Index{
				Did:       index.Did,
				Offset:    uint64(size),
				Length:    uint32(n),
				Timestamp: index.Timestamp,
				Flag:      index.Flag,
			}
			_, err = index.Write(indexFile)
			if err != nil {
				panic(err)
			}
			size += int(index.Length)
		}

	}
	logrus.Infoln("write ok..", shardId, time.Now().UnixMilli()-t.UnixMilli())

	logrus.Infoln("close index chan...")
	logrus.Infoln("start compact..", shardId)
	t = time.Now()
	err = c.dataDisk.Import(dataFile.Name(), dataKey, true)
	if err != nil {
		panic(err)
	}
	// write index
	err = c.dataDisk.Import(indexFile.Name(), indexKey, true)
	if err != nil {
		panic(err)
	}

	logrus.Infoln("compact ok..", shardId, time.Now().UnixMilli()-t.UnixMilli())

	for _, indexKey := range keys {
		dataKey := indexKey[:len(indexKey)-5] + "data"
		logrus.Infoln("erase", dataKey)
		err := c.dataDisk.Erase(dataKey)
		if err != nil {
			panic(err)
		}
		err = c.dataDisk.Erase(indexKey)
		if err != nil {
			panic(err)
		}

	}

}

func (c *Cake) Compact() {
	cancel := make(chan struct{})
	keys := c.dataDisk.Keys(cancel)

	shardKey := map[int64][]string{}
	for key := range keys {
		if strings.HasSuffix(key, "index") {
			meta := strings.Split(key, "_")
			shardId := conv.ToAny[int64](meta[1])
			shardKey[shardId] = append(shardKey[shardId], key)
		}
	}

	for shardId, keys := range shardKey {
		type KeyAndSize struct {
			Key  string
			Size int64
		}
		var keyAndSize []KeyAndSize
		for _, key := range keys {
			path := c.Optional.DataCachePath + "/" + strings.Join(Transform(key), "/") + "/" + key
			stat, err := os.Stat(path)
			if err != nil {
				continue
			}

			// meta := strings.Split(key, "_")
			// created := conv.ToAny[int64](meta[2])

			// index 100MB
			if stat.Size() < 100*1e6 {
				keyAndSize = append(keyAndSize, KeyAndSize{Size: stat.Size(), Key: key})
			}
		}
		sort.Slice(keyAndSize, func(i, j int) bool {
			return keyAndSize[i].Size < keyAndSize[j].Size
		})
		var sortKeys []string
		for _, i := range keyAndSize {
			sortKeys = append(sortKeys, i.Key)
		}
		if len(sortKeys) > 10 {
			logrus.Infoln("compact:", sortKeys)
			c.compact(shardId, sortKeys)
		}
	}

}
