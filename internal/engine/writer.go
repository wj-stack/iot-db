package engine

import (
	"encoding/binary"
	"github.com/golang/protobuf/proto"
	"iot-db/internal/datastructure"
	"iot-db/internal/pb"
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

func NewWriter(file *File) (*Writer, error) {
	return &Writer{
		FirstIndex:      file.FirstIndexFile,
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
func (w *Writer) WriteData(data *pb.Data) error {

	marshal, err := proto.Marshal(data)
	if err != nil {
		return err
	}

	// writer data
	err = binary.Write(w.DataFile, binary.BigEndian, uint32(len(marshal)))
	if err != nil {
		return err
	}

	n, err := w.DataFile.Write(marshal)
	if err != nil {
		return err
	}

	// update info

	w.lastTimestamp = data.Timestamp
	w.lastDataFileOffset = w.DataFileOffset
	w.DataFileOffset += int64(n) + 4

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
		Offset:    uint32(w.lastDataFileOffset),
	}
	err := firstIndexMeta.WriteFirstIndex(w.FirstIndex)
	if err != nil {
		return err
	}
	return nil
}
