package datastructure

import (
	"bytes"
	"encoding/binary"
	"io"
	"iot-db/internal/util"
	"os"
)

// SampleSizePerShardGroup limit first index fileSize
const SampleSizePerShardGroup = 1024

// Data point data
type Data struct {
	DeviceId  int64 // 8
	Length    int32 // 4
	Flag      Flag  // 1
	Timestamp int64 // 8
	CreatedAt int64 // 8
	Body      []byte
}

//func (d *Data) GetSize() int64 {
//	return 8 + 4 + 1 + 8 + 8 + int64(len(d.Body))
//}

type Flag byte

const (
	FlagZip     = 1 << 0
	FlagCompact = 1 << 1
)

func (f *Flag) IsZip() bool {
	return (*f)&FlagZip > 0
}

func (f *Flag) IsCompact() bool {
	return (*f)&FlagCompact > 0
}

func zip(body []byte) ([]byte, error) {
	r := bytes.NewReader(body)
	w := bytes.NewBuffer([]byte{})
	err := util.Compress(r, w)
	if err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}
func unzip(body []byte) ([]byte, error) {
	r := bytes.NewReader(body)
	w := bytes.NewBuffer([]byte{})
	err := util.DeCompress(r, w)
	if err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func (d *Data) Write(writer io.Writer) (int64, error) {
	err := binary.Write(writer, binary.BigEndian, d.DeviceId)
	if err != nil {
		return 0, err
	}
	err = binary.Write(writer, binary.BigEndian, d.Length)
	if err != nil {
		return 0, err
	}
	err = binary.Write(writer, binary.BigEndian, d.Flag)
	if err != nil {
		return 0, err
	}
	err = binary.Write(writer, binary.BigEndian, d.Timestamp)
	if err != nil {
		return 0, err
	}
	err = binary.Write(writer, binary.BigEndian, d.CreatedAt)
	if err != nil {
		return 0, err
	}

	n, err := writer.Write(d.Body)
	if err != nil {
		return 0, err
	}
	return int64(n + 8 + 4 + 1 + 8 + 8), nil
}

func (d *Data) Read(r io.Reader) error {
	err := binary.Read(r, binary.BigEndian, &d.DeviceId)
	if err != nil {
		return err
	}
	err = binary.Read(r, binary.BigEndian, &d.Length)
	if err != nil {
		return err
	}
	err = binary.Read(r, binary.BigEndian, &d.Flag)
	if err != nil {
		return err
	}
	err = binary.Read(r, binary.BigEndian, &d.Timestamp)
	if err != nil {
		return err
	}
	err = binary.Read(r, binary.BigEndian, &d.CreatedAt)
	if err != nil {
		return err
	}
	d.Body = make([]byte, d.Length)
	_, err = r.Read(d.Body)
	if err != nil {
		return err
	}

	return nil
}

const FirstIndexMetaSize = 12

type FirstIndexMeta struct {
	Timestamp int64
	Offset    uint32
}

func (v *FirstIndexMeta) WriteFirstIndex(firstIndex *os.File) error {
	// writer first index
	err := binary.Write(firstIndex, binary.BigEndian, v.Timestamp)
	if err != nil {
		return err
	}

	err = binary.Write(firstIndex, binary.BigEndian, v.Offset)
	if err != nil {
		return err
	}

	return nil
}

func (v *FirstIndexMeta) ReadFirstIndexMeta(firstIndex io.Reader) error {

	err := binary.Read(firstIndex, binary.BigEndian, &v.Timestamp)
	if err != nil {
		return err
	}

	err = binary.Read(firstIndex, binary.BigEndian, &v.Offset)
	if err != nil {
		return err
	}

	return nil
}

const SecondIndexMetaSize = 8 + 8 + 8 + 8 + 8 + 8 + 1

type SecondIndexMeta struct {
	Start, End     int64
	DeviceId       int64
	Size           int64
	FirstIndexSize int64
	Offset         int64
	Flag           byte
}

func (meta *SecondIndexMeta) ReadSecondIndexMeta(secondIndex io.Reader) error {
	err := binary.Read(secondIndex, binary.BigEndian, &meta.Start)
	if err != nil {
		return err
	}

	err = binary.Read(secondIndex, binary.BigEndian, &meta.End)
	if err != nil {
		return err
	}

	err = binary.Read(secondIndex, binary.BigEndian, &meta.DeviceId)
	if err != nil {
		return err
	}

	err = binary.Read(secondIndex, binary.BigEndian, &meta.Size)
	if err != nil {
		return err
	}

	err = binary.Read(secondIndex, binary.BigEndian, &meta.Offset)
	if err != nil {
		return err
	}

	err = binary.Read(secondIndex, binary.BigEndian, &meta.Flag)
	if err != nil {
		return err
	}
	err = binary.Read(secondIndex, binary.BigEndian, &meta.FirstIndexSize)
	if err != nil {
		return err
	}

	return nil
}

func (meta *SecondIndexMeta) WriteSecondIndex(secondIndex *os.File) error {
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

	err = binary.Write(secondIndex, binary.BigEndian, meta.Size)
	if err != nil {
		return err
	}

	err = binary.Write(secondIndex, binary.BigEndian, meta.Offset)
	if err != nil {
		return err
	}

	err = binary.Write(secondIndex, binary.BigEndian, meta.Flag)
	if err != nil {
		return err
	}
	err = binary.Write(secondIndex, binary.BigEndian, meta.FirstIndexSize)
	if err != nil {
		return err
	}
	return nil
}
