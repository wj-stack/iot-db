package engine

import (
	"bytes"
	"github.com/dablelv/go-huge-util/conv"
	"github.com/gammazero/workerpool"
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/mmap"
	"io"
	"iot-db/internal/pb"
	"sort"
	"strings"
	"time"
)

func (c *Cake) Query(did int64, start, end int64) (chan *pb.Data, error) {
	startShardId := toShardId(start)
	endShardId := toShardId(end)
	cancel := make(chan struct{})
	keys := c.dataDisk.Keys(cancel)
	dataChan := make(chan *pb.Data, 1000)
	wp := workerpool.New(200)
	for key := range keys {
		if strings.HasSuffix(key, "index") {
			key := key
			wp.Submit(func() {
				//logrus.Infoln("query", key)
				meta := strings.Split(key, "_")
				shardId := conv.ToAny[int64](meta[1])
				if shardId < startShardId || shardId > endShardId {
					return
				}

				t := time.Now()

				//readStream, err := c.dataDisk.Read(key)
				//if err != nil {
				//	return
				//}
				path := c.Optional.DataCachePath + "/" + strings.Join(Transform(key), "/") + "/" + key
				at, err := mmap.Open(path)
				if err != nil {
					//logrus.Infoln(err)
					return
				}
				defer at.Close()
				//_ = at.Close()
				//at.Len()

				//logrus.Infoln("read index ", time.Now().UnixMilli()-t.UnixMilli())
				//logrus.Infoln("open index file", path, "len:", at.Len())

				t = time.Now()

				var indexs []*Index

				l := sort.Search(at.Len()/IndexSize, func(i int) bool {

					buff := make([]byte, IndexSize)
					_, err := at.ReadAt(buff, int64(i*IndexSize))
					if err != nil {
						logrus.Errorln(err)
						return false
					}
					reader := bytes.NewReader(buff)

					index := &Index{}
					err = index.Read(reader)
					if err != nil {
						return false
					}
					//logrus.Infoln("index:", index)
					if int64(index.Did) != did {
						return int64(index.Did) >= did
					}
					return int64(index.Timestamp) >= start
				})
				r := sort.Search(at.Len()/IndexSize, func(i int) bool {
					buff := make([]byte, IndexSize)
					_, err := at.ReadAt(buff, int64(i*IndexSize))
					if err != nil {
						logrus.Errorln(err)
						return false
					}
					reader := bytes.NewReader(buff)
					index := &Index{}
					err = index.Read(reader)
					if err != nil {
						return false
					}

					if int64(index.Did) != did {
						return int64(index.Did) > did
					}
					return int64(index.Timestamp) > end
				})
				buff := make([]byte, int64((r-l)*IndexSize))
				_, err = at.ReadAt(buff, int64((r-l)*IndexSize))
				if err != nil {
					logrus.Errorln(err, int64((r-l)*IndexSize))
					return
				}
				reader := bytes.NewReader(buff)
				for i := l; i < r; i++ {
					index := &Index{}
					err = index.Read(reader)
					if err != nil {
						break
					}
					indexs = append(indexs, index)
				}

				//logrus.Infoln("search index", time.Now().UnixMilli()-t.UnixMilli(), "index cnt:", r-l)
				t = time.Now()

				stream, err := c.dataDisk.ReadStream(key[:len(key)-5]+"data", false)
				if err != nil {
					logrus.Errorln(err)
					return
				}
				//logrus.Infoln(key, len(indexs))
				func() {
					defer stream.Close()
					if len(indexs) > 0 {
						_, err = io.CopyN(io.Discard, stream, int64(indexs[0].Offset))
						if err != nil {
							return
						}
						for i := 0; i < len(indexs); i++ {
							body := make([]byte, indexs[i].Length)
							_, err := stream.Read(body)
							if err != nil {
								return
							}
							var v pb.Data
							err = proto.Unmarshal(body, &v)
							if err != nil {
								return
							}
							dataChan <- &v
						}
					}
				}()

				logrus.Infoln("read data", time.Now().UnixMilli()-t.UnixMilli())

			})
		}
	}
	go func() {
		wp.StopWait()
		close(dataChan)
	}()
	return dataChan, nil
}
