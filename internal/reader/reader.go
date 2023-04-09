package reader

import (
	"bytes"
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"iot-db/internal/datastructure"
	"os"
)

type Reader struct {
	firstIndex, secondIndex, dataFile *os.File
}

func OpenDataFile(path, name string) (*os.File, *os.File, *os.File, error) {
	firstIndex, err := os.Open(fmt.Sprintf("%s/%s.first_index", path, name))
	if err != nil {
		return nil, nil, nil, err
	}
	secondIndex, err := os.Open(fmt.Sprintf("%s/%s.second_index", path, name))
	if err != nil {
		_ = firstIndex.Close()
		return nil, nil, nil, err
	}
	dataFile, err := os.Open(fmt.Sprintf("%s/%s.data", path, name))
	if err != nil {
		_ = firstIndex.Close()
		_ = secondIndex.Close()
		return nil, nil, nil, err
	}
	return firstIndex, secondIndex, dataFile, nil
}

func NewReader(path string, name string) (*Reader, error) {
	firstIndex, secondIndex, dataFile, err := OpenDataFile(path, name)
	if err != nil {
		return nil, err
	}
	return &Reader{
		firstIndex:  firstIndex,
		secondIndex: secondIndex,
		dataFile:    dataFile,
	}, nil
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

func Query(did int64, start, end int64, firstIndex, secondIndex, dataFile *os.File) ([]datastructure.Data, error) {
	var ret []datastructure.Data
	all, err := io.ReadAll(secondIndex)
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(all)
	for {
		secondIndexMeta := datastructure.SecondIndexMeta{}
		err := secondIndexMeta.ReadSecondIndexMeta(r)
		if err != nil {
			break
		}
		if secondIndexMeta.DeviceId == did {
			logrus.Infof("secondIndexMeta:%#v\n", secondIndexMeta)

			firstIndexBuf := make([]byte, secondIndexMeta.FirstIndexSize*datastructure.FirstIndexMetaSize)
			_, err := firstIndex.ReadAt(firstIndexBuf, secondIndexMeta.Offset)
			if err != nil {
				return nil, err
			}

			r := bytes.NewReader(firstIndexBuf)
			firstIndexMeta := datastructure.FirstIndexMeta{}
			var lastOffset int64 = 0
			for {
				err := firstIndexMeta.ReadFirstIndexMeta(r)
				if err != nil {
					break
				}

				logrus.Infof("firstIndexMeta:%#v\n", firstIndexMeta)

				if firstIndexMeta.Timestamp > start {
					_, err := dataFile.Seek(lastOffset, io.SeekStart)
					if err != nil {
						return nil, err
					}
					for {
						data := datastructure.Data{}
						err := data.Read(dataFile)
						if err != nil {
							break
						}

						if data.DeviceId != did {
							break
						}

						if data.Timestamp <= end && data.Timestamp >= start {
							ret = append(ret, data)
							logrus.Infof("data:%#v\n", data)

						} else if data.Timestamp < start {
							continue
						} else {
							break
						}
					}
					break
				}

				lastOffset = firstIndexMeta.Offset
			}
		}
	}
	return ret, nil
}
