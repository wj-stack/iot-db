package engine

import (
	"bytes"
	"github.com/dablelv/go-huge-util/conv"
	"github.com/gammazero/workerpool"
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/mmap"
	"iot-db/internal/pb"
	"sort"
	"strings"
)

func (c *Cake) Query(did int64, start, end int64) (chan *pb.Data, error) {
	startShardId := toShardId(start)
	endShardId := toShardId(end)
	dataChan := make(chan *pb.Data, 1000)
	wp := workerpool.New(1)
	c.mu.RLock()
	defer c.mu.RUnlock()
	for key := range c.Keys {
		if strings.HasSuffix(key, "index") {
			key := key
			wp.Submit(func() {
				meta := strings.Split(key, "_")
				shardId := conv.ToAny[int64](meta[1])
				if shardId < startShardId || shardId > endShardId {
					return
				}
				//logrus.Infoln("key:", key)

				//readStream, err := c.dataDisk.Read(key)
				//if err != nil {
				//	return
				//}
				//t := time.Now()

				//c.BigCache.Get
				indexCacheKey := key + conv.ToAny[string](did)
				//logrus.Infoln("read indexCacheKey:", indexCacheKey)
				//var indexs []Index
				buf, err := c.BigCache.Get(indexCacheKey)
				if err != nil {
					// read did index
					c.ReadFile(key, func(at *mmap.ReaderAt) {

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
							//if int64(index.Did) != did {
							//	return int64(index.Did) >= did
							//}
							//return int64(index.Timestamp) >= start
							return int64(index.Did) >= did
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

							//if int64(index.Did) != did {
							//	return int64(index.Did) > did
							//}
							//return int64(index.Timestamp) > end
							return int64(index.Did) > did
						})
						//logrus.Infoln("l,r,size:", l, r, int64((r-l)*IndexSize), "offset:", int64(l*IndexSize))
						buf = make([]byte, int64((r-l)*IndexSize))
						_, err := at.ReadAt(buf, int64(l*IndexSize))
						if err != nil {
							panic(err)
						}
						err = c.BigCache.Set(indexCacheKey, buf)
						if err != nil {
							panic(err)
						}
					})
				}

				//for {
				//	index := Index{}
				//	err = index.Read(bytes.NewReader(buf))
				//	if err != nil {
				//		break
				//	}
				//	indexs = append(indexs, index)
				//}

				//if len(indexs) == 0 {
				//	return
				//}

				//logrus.Infoln("indexs:", len(indexs), indexs[0], indexs[len(indexs)-1])
				//return

				// get l,r
				if len(buf) == 0 {
					return
				}

				var l, r Index
				err = l.Read(bytes.NewReader(buf[:IndexSize]))
				if err != nil {
					panic(err)
				}
				err = r.Read(bytes.NewReader(buf[len(buf)-IndexSize:]))
				if err != nil {
					panic(err)
				}

				//logrus.Infoln(l, r)

				// read data from cache
				//dataSize := indexs[len(indexs)-1].Offset + uint64(indexs[len(indexs)-1].Length) - indexs[0].Offset
				dataSize := r.Offset + uint64(r.Length) - l.Offset
				//logrus.Infoln("IndexSize", dataSize/IndexSize)
				dataKey := key[:len(key)-5] + "data"
				dataCacheKey := dataKey + "_" + conv.ToAny[string](did)
				dataBuf, err := c.BigCache.Get(dataCacheKey)
				if err != nil || len(dataBuf) != int(dataSize) {
					c.ReadFile(dataKey, func(at *mmap.ReaderAt) {
						dataBuf = make([]byte, dataSize)
						_, err := at.ReadAt(dataBuf, int64(l.Offset))
						//_, err := at.ReadAt(dataBuf, int64(indexs[0].Offset))
						if err != nil {
							panic(err)
						}
					})
					err := c.BigCache.Set(dataCacheKey, dataBuf)
					if err != nil {
						panic(err)
					}
				}

				// read data
				reader := bytes.NewReader(buf)
				var i Index
				for {
					err := i.Read(reader)
					if err != nil {
						break
					}
					//logrus.Infoln(indexCacheKey, i)
					var v pb.Data
					offset := i.Offset - l.Offset
					length := uint64(i.Length)
					err = proto.Unmarshal(dataBuf[offset:offset+length], &v)
					if err != nil {
						panic(err)
					}
					dataChan <- &v
					//logrus.Infoln(v.Did, v.Timestamp)
				}

				//for i := 0; i < len(indexs); i++ {
				//	var v pb.Data
				//	offset := indexs[i].Offset - indexs[0].Offset
				//	length := uint64(indexs[i].Length)
				//	err = proto.Unmarshal(dataBuf[offset:offset+length], &v)
				//	if err != nil {
				//		panic(err)
				//	}
				//	dataChan <- &v
				//}

			})
		}
	}
	go func() {
		wp.StopWait()
		close(dataChan)
	}()
	return dataChan, nil
}
