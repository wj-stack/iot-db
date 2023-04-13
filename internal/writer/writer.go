package writer

import (
	"iot-db/internal/datastructure"
	"iot-db/internal/filemanager"
	"math"
	"os"
)

type Writer struct {
	FirstIndex, SecondIndex, DataFile *os.File
	name                              string
	size                              int
	cnt                               int
	lastTimestamp                     int64
	DataFileOffset                    int64
	lastDataFileOffset                int64
	firstFileOffset                   int64
	lastFirstFileOffset               int64
	firstIndexCnt                     int64
	start, end                        int64
}

func NewWriter(file *filemanager.File) (*Writer, error) {
	return &Writer{
		FirstIndex:      file.FirstIndex,
		SecondIndex:     file.SecondIndex,
		DataFile:        file.DataFile,
		size:            0,
		cnt:             0,
		DataFileOffset:  0,
		firstFileOffset: 0,
		firstIndexCnt:   0,
		start:           int64(math.MaxInt64),
		end:             int64(math.MinInt64),
	}, nil
}

// WriteData  upper ensure sequential writes
func (w *Writer) WriteData(data *datastructure.Data) error {

	// writer data
	n, err := data.Write(w.DataFile)
	if err != nil {
		return err
	}

	// update info

	w.cnt++
	w.size++
	w.lastTimestamp = data.Timestamp
	w.lastDataFileOffset = w.DataFileOffset
	w.DataFileOffset += n

	if w.start > data.Timestamp {
		w.start = data.Timestamp
	}

	if w.end < data.Timestamp {
		w.end = data.Timestamp
	}

	return nil
}

func (w *Writer) WriteFirstIndex() error {
	// writer first index
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

func (w *Writer) Close() error {
	err := w.FirstIndex.Close()
	if err != nil {
		return err
	}
	err = w.SecondIndex.Close()
	if err != nil {
		return err
	}
	err = w.DataFile.Close()
	if err != nil {
		return err
	}
	return nil
}
