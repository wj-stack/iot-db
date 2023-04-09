package writer

import (
	"iot-db/internal/datastructure"
	"math"
	"os"
)

type Writer struct {
	FirstIndex, SecondIndex, DataFile *os.File
	name                              string
	size                              int
	cnt                               int
	lastTimestamp                     int64
	dataFileOffset                    int64
	lastDataFileOffset                int64
	firstFileOffset                   int64
	lastFirstFileOffset               int64
	firstIndexCnt                     int64
	start, end                        int64
}

func NewWriter(firstIndex, secondFile, dataFile *os.File) (*Writer, error) {
	//firstIndex, secondFile, dataFile, name, err := util.CreateTempFile(workspace, int(compactor.ShardGroupSize[0]), 0)
	//if err != nil {
	//	return nil, err
	//}

	return &Writer{
		FirstIndex:      firstIndex,
		SecondIndex:     secondFile,
		DataFile:        dataFile,
		size:            0,
		cnt:             0,
		dataFileOffset:  0,
		firstFileOffset: 0,
		firstIndexCnt:   0,
		start:           int64(math.MaxInt64),
		end:             int64(math.MinInt64),
	}, nil
}

// WriteData  upper ensure sequential writes
func (w *Writer) WriteData(data *datastructure.Data) error {

	// write data
	err := data.Write(w.DataFile)
	if err != nil {
		return err
	}

	// update info

	w.cnt++
	w.size++
	w.lastTimestamp = data.Timestamp
	w.lastDataFileOffset = w.dataFileOffset
	w.dataFileOffset += data.GetSize()

	if w.start > data.Timestamp {
		w.start = data.Timestamp
	}

	if w.end < data.Timestamp {
		w.end = data.Timestamp
	}

	return nil
}

func (w *Writer) WriteFirstIndex() error {
	// write first index
	firstIndexMeta := datastructure.FirstIndexMeta{
		Timestamp: w.lastTimestamp,
		Offset:    w.lastDataFileOffset,
	}
	//logrus.Infof("firstIndex:%#v\n", firstIndexMeta)
	err := firstIndexMeta.WriteFirstIndex(w.FirstIndex)
	if err != nil {
		return err
	}
	w.firstIndexCnt++
	w.firstFileOffset += datastructure.FirstIndexMetaSize
	//logrus.Infoln("w.dataFileOffset:", w.dataFileOffset, "w.lastDataFileOffset:", w.lastDataFileOffset)
	w.lastDataFileOffset = w.dataFileOffset
	return nil
}

func (w *Writer) WriteSecondIndex(did int64) error {
	secondIndexMeta := datastructure.SecondIndexMeta{
		Start:          w.start,
		End:            w.end,
		DeviceId:       did,
		Size:           int64(w.cnt), // point size
		FirstIndexSize: w.firstIndexCnt,
		Offset:         w.lastFirstFileOffset,
		Flag:           0,
	}
	//logrus.Infof("secondIndexMeta:%#v\n", secondIndexMeta)

	err := secondIndexMeta.WriteSecondIndex(w.SecondIndex)
	if err != nil {
		return err
	}
	w.cnt = 0
	w.firstIndexCnt = 0
	w.start = int64(math.MaxInt64)
	w.end = int64(math.MinInt64)
	w.lastFirstFileOffset = w.firstFileOffset
	return nil
}
