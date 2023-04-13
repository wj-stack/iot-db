package reader

import (
	"github.com/gammazero/workerpool"
	"github.com/sirupsen/logrus"
	"github.com/zeromicro/go-zero/core/collection"
	"io"
	"iot-db/internal/datastructure"
	"iot-db/internal/filemanager"
	"iot-db/internal/shardgroup"
	"os"
	"sync"
	"time"
)

type IndexCache struct {
	FirstIndex  []*datastructure.FirstIndexMeta
	SecondIndex map[int64]*datastructure.SecondIndexMeta
}

type Reader struct {
	*filemanager.FileManager
	Cache *collection.Cache // save second index
}

func NewReader(f *filemanager.FileManager) (*Reader, error) {
	c, err := collection.NewCache(time.Minute*10, collection.WithLimit(200))
	if err != nil {
		panic(err)
	}

	return &Reader{
		FileManager: f,
		Cache:       c,
	}, nil
}

func (r *Reader) Query(did, start, end int64) ([]*datastructure.Data, error) {

	var ret []*datastructure.Data
	var mutex sync.Mutex
	wp := workerpool.New(len(r.Config.Core.ShardGroupSize))

	for _, shardSize := range r.Config.Core.ShardGroupSize {
		shardSize := shardSize
		wp.Submit(func() {
			startId := shardgroup.TimestampConvertShardId(start, int64(shardSize))
			endId := shardgroup.TimestampConvertShardId(end, int64(shardSize))

			files, err := r.FileManager.GetFile(shardSize, int(startId), int(endId))
			if err != nil {
				logrus.Errorln(err)
				return
			}

			wp := workerpool.New(len(files))

			for _, file := range files {
				file := file
				wp.Submit(func() {
					// get second index
					//logrus.Infoln("query", file)
					c, ok := r.Cache.Get(file.Name)
					if !ok {
						// get second
						secondIndex := r.ReadSecondIndex(file.SecondIndex)
						v := map[int64]*datastructure.SecondIndexMeta{}

						for _, idx := range secondIndex {
							v[idx.DeviceId] = idx
						}
						firstIndex := r.ReadFirstIndex(file.FirstIndex)

						index := &IndexCache{
							FirstIndex:  firstIndex,
							SecondIndex: v,
						}

						r.Cache.Set(file.Name, index)
						// get first index
						c = index
					}

					index := c.(*IndexCache)

					secondIndex, ok := index.SecondIndex[did]
					if !ok {
						return
					}

					//logrus.Infoln("secondIndex:", secondIndex)

					if secondIndex.End < start || secondIndex.Start > end {
						return
					}

					begin := int(secondIndex.Offset / datastructure.FirstIndexMetaSize)

					var l = -1
					for i := 0; i < int(secondIndex.FirstIndexSize); i++ {
						if index.FirstIndex[begin+i].Timestamp >= start {
							l = begin + i
							break
						}
					}

					if l == -1 {
						return
					}

					logrus.Infoln("first index", index.FirstIndex[l].Offset)

					_, err := file.DataFile.Seek(index.FirstIndex[l].Offset, io.SeekStart)
					if err != nil {
						return
					}

					for {
						data := datastructure.Data{}
						err := data.Read(file.DataFile)
						if err != nil {
							break
						}

						if data.DeviceId == did && data.Timestamp >= start && data.Timestamp <= end {
							mutex.Lock()
							ret = append(ret, &data)
							mutex.Unlock()
						} else {
							break
						}

					}
				})

			}
			wp.StopWait()

		})

	}

	wp.StopWait()

	return ret, nil
}

func (r *Reader) ReadSecondIndex(secondIndex *os.File) (ret []*datastructure.SecondIndexMeta) {
	for {
		secondIndexMeta := datastructure.SecondIndexMeta{}
		err := secondIndexMeta.ReadSecondIndexMeta(secondIndex)
		if err != nil {
			break
		}
		ret = append(ret, &secondIndexMeta)
	}
	return
}

func (r *Reader) ReadFirstIndex(firstIndex *os.File) (ret []*datastructure.FirstIndexMeta) {
	for {
		firstIndexMeta := datastructure.FirstIndexMeta{}
		err := firstIndexMeta.ReadFirstIndexMeta(firstIndex)
		if err != nil {
			break
		}
		ret = append(ret, &firstIndexMeta)
	}
	return
}
