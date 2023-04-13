package reader

import (
	"github.com/sirupsen/logrus"
	"github.com/zeromicro/go-zero/core/collection"
	"io"
	"iot-db/internal/datastructure"
	"iot-db/internal/filemanager"
	"sort"
	"time"
)

type Reader struct {
	*filemanager.FileManager
	Cache *collection.Cache
}

func NewReader(f *filemanager.FileManager) (*Reader, error) {
	c, err := collection.NewCache(time.Minute, collection.WithLimit(10000))
	if err != nil {
		panic(err)
	}

	return &Reader{
		FileManager: f,
		Cache:       c,
	}, nil
}

func (r *Reader) Query1(did, start, end int64) ([]*datastructure.Data, error) {

	//

	return nil, nil
}

func (r *Reader) ReadSecondIndex() (ret []datastructure.SecondIndexMeta) {
	for {
		secondIndexMeta := datastructure.SecondIndexMeta{}
		err := secondIndexMeta.ReadSecondIndexMeta(r.secondIndex)
		if err != nil {
			break
		}
		ret = append(ret, secondIndexMeta)
	}
	return
}

func (r *Reader) ReadFirstIndex() (ret []datastructure.FirstIndexMeta) {
	for {
		firstIndexMeta := datastructure.FirstIndexMeta{}
		err := firstIndexMeta.ReadFirstIndexMeta(r.firstIndex)
		if err != nil {
			break
		}
		ret = append(ret, firstIndexMeta)
	}
	return
}

func (r *Reader) ReadFirstIndexN(n int) (ret []datastructure.FirstIndexMeta) {
	for i := 0; i < n; i++ {
		firstIndexMeta := datastructure.FirstIndexMeta{}
		err := firstIndexMeta.ReadFirstIndexMeta(r.firstIndex)
		if err != nil {
			break
		}
		ret = append(ret, firstIndexMeta)

	}
	return
}

func (r *Reader) ReadAllData() (ret []datastructure.Data) {
	for {
		data := datastructure.Data{}
		err := data.Read(r.dataFile)
		if err != nil {
			break
		}
		ret = append(ret, data)
	}
	return
}

func (r *Reader) ReadData() (*datastructure.Data, error) {
	data := datastructure.Data{}
	err := data.Read(r.dataFile)
	if err != nil {
		return nil, err
	}
	return &data, nil
}

func (r *Reader) SeekDataFile(offset int64) error {
	_, err := r.dataFile.Seek(offset, io.SeekStart)
	return err
}

func (r *Reader) SeekFirstIndexFile(offset int64) error {
	_, err := r.firstIndex.Seek(offset, io.SeekStart)
	return err
}

func (r *Reader) Query(did int64, start, end int64) ([]*datastructure.Data, error) {

	secondIndex := r.ReadSecondIndex()

	search := sort.Search(len(secondIndex), func(i int) bool {
		return secondIndex[i].DeviceId >= did
	})

	logrus.Infoln("search:", search, len(secondIndex))

	if search == len(secondIndex) {
		return nil, nil
	}

	if secondIndex[search].DeviceId != did {
		return nil, nil
	}

	//logrus.Infof("%#v\n", secondIndex[search])

	err := r.SeekFirstIndexFile(secondIndex[search].Offset)
	if err != nil {
		return nil, err
	}

	firstIndexN := r.ReadFirstIndexN(int(secondIndex[search].FirstIndexSize))

	logrus.Infof("first index", firstIndexN)
	var l int = -1
	for i := 0; i < len(firstIndexN)-1; i++ {
		logrus.Infoln(firstIndexN[i].Timestamp, firstIndexN[i+1].Timestamp, start)
		if firstIndexN[i].Timestamp >= start {
			l = i
			break
		}
	}

	if l == -1 {
		logrus.Errorln("no found")
		return nil, nil
	}

	// find last index
	if l != 0 {
		l -= 1
	}
	logrus.Infof("%#v\n", firstIndexN[l])
	err = r.SeekDataFile(firstIndexN[l].Offset)
	if err != nil {
		return nil, err
	}
	var ret []*datastructure.Data
	for {

		data, err := r.ReadData()
		if err != nil {
			break
		}

		if data.DeviceId == did && data.Timestamp >= start && data.Timestamp <= end {
			ret = append(ret, data)
		} else {
			break
		}

	}

	return ret, nil
}
