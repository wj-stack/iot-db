package index

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

type FirstIndexMeta struct {
	Timestamp int64
	Offset    int64
	Len       int64
}

const FirstIndexMetaSize = 24

func WriteFirstIndex(firstIndex *os.File, meta *FirstIndexMeta) error {
	// write first index
	err := binary.Write(firstIndex, binary.BigEndian, meta.Timestamp)
	if err != nil {
		return err
	}

	err = binary.Write(firstIndex, binary.BigEndian, meta.Offset)
	if err != nil {
		return err
	}

	err = binary.Write(firstIndex, binary.BigEndian, meta.Len)
	if err != nil {
		return err
	}
	return nil
}

func ReadFirstIndexMeta(firstIndex *os.File) (*FirstIndexMeta, error) {
	v := FirstIndexMeta{}
	err := binary.Read(firstIndex, binary.BigEndian, &v.Timestamp)
	if err != nil {
		return nil, err
	}

	err = binary.Read(firstIndex, binary.BigEndian, &v.Offset)
	if err != nil {
		return nil, err
	}

	err = binary.Read(firstIndex, binary.BigEndian, &v.Len)
	if err != nil {
		return nil, err
	}

	return &v, nil
}

type SecondIndexMeta struct {
	Start, End int64
	DeviceId   int64
	Count      int64
}

const SecondIndexMetaSize = 32

func ReadSecondIndexMeta(secondIndex *os.File) (*SecondIndexMeta, error) {
	v := SecondIndexMeta{}
	err := binary.Read(secondIndex, binary.BigEndian, &v.Start)
	if err != nil {
		return nil, err
	}

	err = binary.Read(secondIndex, binary.BigEndian, &v.End)
	if err != nil {
		return nil, err
	}

	err = binary.Read(secondIndex, binary.BigEndian, &v.DeviceId)
	if err != nil {
		return nil, err
	}

	err = binary.Read(secondIndex, binary.BigEndian, &v.Count)
	if err != nil {
		return nil, err
	}
	return &v, nil
}

func WriteSecondIndex(secondIndex *os.File, meta *SecondIndexMeta) error {
	err := binary.Write(secondIndex, binary.BigEndian, meta.Start)
	if err != nil {
		return err
	}

	err = binary.Write(secondIndex, binary.BigEndian, meta.End)
	if err != nil {
		return err
	}

	err = binary.Write(secondIndex, binary.BigEndian, meta.DeviceId)
	if err != nil {
		return err
	}

	err = binary.Write(secondIndex, binary.BigEndian, meta.Count)
	if err != nil {
		return err
	}
	return nil
}

type File struct {
	*SecondIndexMeta
	*FirstIndexMeta

	FirstIndex  *os.File
	SecondIndex *os.File
	DataFile    *os.File
}

func (f *File) ReadData(len int64) ([]byte, error) {
	dataFile := f.DataFile
	bytes := make([]byte, len)
	_, err := dataFile.Read(bytes)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func OpenFile(workspace string, i int, name string) (*File, error) {
	firstIndex, err := os.Open(fmt.Sprintf("%s/data/%02d/%s.first_index", workspace, i, name))
	if err != nil {
		return nil, err
	}
	secondIndex, err := os.Open(fmt.Sprintf("%s/data/%02d/%s.second_index", workspace, i, name))
	if err != nil {
		_ = firstIndex.Close()
		return nil, err
	}
	dataFile, err := os.Open(fmt.Sprintf("%s/data/%02d/%s.data", workspace, i, name))
	if err != nil {
		_ = firstIndex.Close()
		_ = secondIndex.Close()
		return nil, err
	}

	return &File{
		SecondIndexMeta: nil,
		FirstIndexMeta:  nil,
		FirstIndex:      firstIndex,
		SecondIndex:     secondIndex,
		DataFile:        dataFile,
	}, nil
}

func merge(list []*File, newFile *File) error {

	table := NewTable()

	for _, file := range list {

		for {
			secondIndexMeta, err := ReadSecondIndexMeta(file.SecondIndex)
			if err != nil {
				break
			}

			var i int64
			for i = 0; i < secondIndexMeta.Count; i++ {
				firstIndexMeta, err := ReadFirstIndexMeta(file.FirstIndex)
				if err != nil {
					return err
				}

				//fmt.Printf("firstIndexMeta:%#v\n", firstIndexMeta)
				table.Insert(firstIndexMeta.Timestamp, secondIndexMeta.DeviceId, &FileInfo{
					FirstIndexMeta: firstIndexMeta,
					File:           file.DataFile,
				})
			}

		}

	}
	fmt.Println("table.deviceList.Size():", table.deviceList.Size())
	return table.Save(newFile)
}

//func merge(list []*File, newFile *File) error {
//
//	// deviceId
//	devicePQ := stl4go.NewPriorityQueueFunc[*File](func(a, b *File) bool {
//		return a.SecondIndexMeta.DeviceId < b.SecondIndexMeta.DeviceId
//	})
//
//	// timestamp
//	timestampPQ := stl4go.NewPriorityQueueFunc[*File](func(a, b *File) bool {
//		return a.FirstIndexMeta.Timestamp < b.FirstIndexMeta.Timestamp
//	})
//
//	for _, file := range list {
//
//		if file.FirstIndexMeta == nil {
//			var err error
//			file.FirstIndexMeta, err = ReadFirstIndexMeta(file.FirstIndex)
//			if err != nil {
//				return err
//			}
//		}
//
//		if file.SecondIndexMeta == nil {
//			var err error
//			file.SecondIndexMeta, err = ReadSecondIndexMeta(file.SecondIndex)
//			if err != nil {
//				return err
//			}
//		}
//
//		devicePQ.Push(file)
//	}
//
//	var offset int64
//	for {
//
//		if devicePQ.IsEmpty() {
//			break
//		}
//
//		// device id should not same
//		// add f to timestamp heap
//		f := devicePQ.Top()
//		devicePQ.Pop()
//
//		timestampPQ.Push(f)
//
//		// fetch all node of same of device id
//		for {
//			if devicePQ.IsEmpty() {
//				break
//			}
//			// Check whether the next device has the same id
//			// push it to timestampPQ
//			v := devicePQ.Top()
//			if v.SecondIndexMeta.DeviceId == f.SecondIndexMeta.DeviceId {
//				// add v to timestamp heap
//				devicePQ.Pop()
//				timestampPQ.Push(v)
//
//			} else {
//				break
//			}
//		}
//
//		startTime := int64(math.MaxInt64)
//		endTime := int64(math.MinInt64)
//		count := int64(0)
//
//		for {
//			if timestampPQ.IsEmpty() {
//				break
//			}
//
//			v := timestampPQ.Top()
//			timestampPQ.Pop()
//
//			//  read old data
//			readData, err := v.ReadData(v.Len)
//			if err != nil {
//				return err
//			}
//			//  write new data
//			_, err = newFile.DataFile.Write(readData)
//			if err != nil {
//				return err
//			}
//
//			// save meta info
//			if startTime > v.Timestamp {
//				startTime = v.Timestamp
//			}
//
//			if endTime < v.Timestamp {
//				endTime = v.Timestamp
//			}
//
//			fmt.Println("first index:", FirstIndexMeta{
//				Timestamp: v.Timestamp,
//				Offset:    offset,
//				Len:       v.Len,
//			})
//
//			count++
//			err = WriteFirstIndex(newFile.FirstIndex, &FirstIndexMeta{
//				Timestamp: v.Timestamp,
//				Offset:    offset,
//				Len:       v.Len,
//			})
//			if err != nil {
//				return err
//			}
//			fmt.Println("readData:", string(readData))
//			offset += int64(len(readData))
//			v.SecondIndexMeta.Count--
//			if v.SecondIndexMeta.Count > 0 {
//				next, err := ReadFirstIndexMeta(v.FirstIndex)
//				if err != nil {
//					continue
//				}
//				v.FirstIndexMeta = next
//				timestampPQ.Push(v)
//			}
//
//		}
//
//		fmt.Println(SecondIndexMeta{
//			Start:    startTime,
//			End:      endTime,
//			DeviceId: f.DeviceId,
//			Count:    count,
//		})
//
//		// write second index
//		err := WriteSecondIndex(newFile.SecondIndex, &SecondIndexMeta{
//			Start:    startTime,
//			End:      endTime,
//			DeviceId: f.DeviceId,
//			Count:    count,
//		})
//		if err != nil {
//			return err
//		}
//
//		// read next device
//		// read second index to next
//		next, err := ReadSecondIndexMeta(f.SecondIndex)
//		if err != nil {
//			continue
//		}
//		f.SecondIndexMeta = next
//
//		// read first index
//		if f.SecondIndexMeta.Count > 0 {
//			meta, err := ReadFirstIndexMeta(f.FirstIndex)
//			if err != nil {
//				return err
//			}
//			f.FirstIndexMeta = meta
//			devicePQ.Push(f)
//		}
//
//	}
//
//	return nil
//}

var errNoFound = errors.New("no found meta")

func GetSecondIndexMeta(secondIndex *os.File, deviceId int64) (*SecondIndexMeta, error) {

	stat, err := secondIndex.Stat()
	if err != nil {
		return nil, err
	}
	size := stat.Size()

	count := int(size) / SecondIndexMetaSize

	r := count
	for l := 0; l <= r; {
		mid := (l + r) / 2
		fmt.Println("mid:", mid)
		_, err := secondIndex.Seek(int64((mid-1)*SecondIndexMetaSize), io.SeekStart)
		if err != nil {
			return nil, err
		}
		meta, err := ReadSecondIndexMeta(secondIndex)
		if err != nil {
			return nil, err
		}
		fmt.Println("meta:", meta, l, r, mid)

		if meta.DeviceId > deviceId {
			r = mid
		} else if meta.DeviceId < deviceId {
			l = mid + 1
		} else {
			return meta, nil
		}

	}
	return nil, errNoFound
}

func GetFirstIndexMeta(firstIndex *os.File, timestamp int64) (*FirstIndexMeta, error) {

	stat, err := firstIndex.Stat()
	if err != nil {
		return nil, err
	}
	size := stat.Size()

	count := int(size) / FirstIndexMetaSize

	r := count
	for l := 0; l <= r; {
		mid := (l + r) / 2

		_, err := firstIndex.Seek(int64((mid-1)*FirstIndexMetaSize), io.SeekStart)
		if err != nil {
			return nil, err
		}

		meta, err := ReadFirstIndexMeta(firstIndex)
		if err != nil {
			return nil, err
		}

		if meta.Timestamp > timestamp {
			r = mid
		} else if meta.Timestamp < timestamp {
			l = mid + 1
		} else {
			return meta, nil
		}

	}
	return nil, errNoFound
}

func GetData(dataFile *os.File, offset int64, length int64) ([]byte, error) {
	ret := make([]byte, length)
	_, err := dataFile.ReadAt(ret, offset)
	if err != nil {
		return nil, err
	}
	return ret, nil
}
