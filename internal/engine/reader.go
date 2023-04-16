package engine

import (
	"github.com/dablelv/go-huge-util/conv"
	"github.com/gammazero/workerpool"
	"github.com/sirupsen/logrus"
	"github.com/zeromicro/go-zero/core/collection"
	"io"
	"iot-db/internal/datastructure"
	"iot-db/internal/filemanager"
	"iot-db/internal/shardgroup"
	"iot-db/internal/writer"
	"os"
	"sync"
	"time"
)

type SecondIndexCache struct {
	SecondIndex map[int64]*datastructure.SecondIndexMeta
}

type FirstIndexCache struct {
	FirstIndex []*datastructure.FirstIndexMeta
	Field      []byte
}

type Reader struct {
	*filemanager.FileManager
	SecondCache *collection.Cache // save second index
	FirstCache  *collection.Cache // save second index
}

func NewReader(f *filemanager.FileManager) (*Reader, error) {
	sc, err := collection.NewCache(time.Minute*10, collection.WithLimit(200))
	if err != nil {
		panic(err)
	}
	fc, err := collection.NewCache(time.Minute*10, collection.WithLimit(200))
	if err != nil {
		panic(err)
	}

	return &Reader{
		FileManager: f,
		SecondCache: sc,
		FirstCache:  fc,
	}, nil
}

func (r *Reader) Query(did, start, end int64) ([]*datastructure.Data, []byte, error) {

	var ret []*datastructure.Data
	var mutex sync.Mutex
	wp := workerpool.New(len(r.Config.Core.ShardGroupSize))

	var retHeader []byte

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
					c, ok := r.SecondCache.Get(file.Name)
					if !ok {
						// get second
						secondIndex := r.ReadSecondIndex(file.SecondIndex)
						v := map[int64]*datastructure.SecondIndexMeta{}

						for _, idx := range secondIndex {
							v[idx.DeviceId] = idx
						}

						index := &SecondIndexCache{
							SecondIndex: v,
						}

						r.SecondCache.Set(file.Name, index)
						c = index
					}

					index := c.(*SecondIndexCache)

					secondIndex, ok := index.SecondIndex[did]
					if !ok {
						return
					}

					if secondIndex.End < start || secondIndex.Start > end {
						return
					}

					// get first index
					cacheName := file.Name + "_" + conv.ToAny[string](did)
					fc, ok := r.FirstCache.Get(cacheName)
					if !ok {
						_, err := file.FirstIndex.Seek(secondIndex.Offset, io.SeekStart)
						if err != nil {
							return
						}

						header, err := writer.ReadFirstIndexHeader(file.FirstIndex)
						if err != nil {
							return
						}
						//
						//field := pb.Array{}
						//err = proto.Unmarshal(header, &field)
						//if err != nil {
						//	return
						//}

						var v []*datastructure.FirstIndexMeta
						for i := 0; i < int(secondIndex.FirstIndexSize); i++ {
							firstIndex := datastructure.FirstIndexMeta{}
							err := firstIndex.ReadFirstIndexMeta(file.FirstIndex)
							if err != nil {
								return
							}
							v = append(v, &firstIndex)
						}

						index := &FirstIndexCache{
							FirstIndex: v,
							Field:      header,
						}

						r.FirstCache.Set(cacheName, index)
						fc = index
					}

					firstIndex := fc.(*FirstIndexCache)

					var l = -1
					for i := 0; i < int(secondIndex.FirstIndexSize); i++ {
						if firstIndex.FirstIndex[i].Timestamp >= start {
							l = i
							break
						}
					}

					if l == -1 {
						return
					}

					_, err := file.DataFile.Seek(int64(firstIndex.FirstIndex[l].Offset), io.SeekStart)
					if err != nil {
						return
					}

					mutex.Lock()
					retHeader = firstIndex.Field
					mutex.Unlock()

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

	return ret, retHeader, nil
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
