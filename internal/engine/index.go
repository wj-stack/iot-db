package engine

import (
	"encoding/binary"
	"io"
)

type Index struct {
	Did       uint32 // 4
	Offset    uint64 // 8
	Length    uint32 // 4
	Timestamp uint64 // 8
	Flag      byte   // 1
}

const IndexSize = 4 + 8 + 4 + 8 + 1

func (d *Index) Write(writer io.Writer) (int64, error) {
	err := binary.Write(writer, binary.BigEndian, d.Did)
	if err != nil {
		return 0, err
	}
	err = binary.Write(writer, binary.BigEndian, d.Offset)
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
	return int64(4 + 8 + 8 + 1 + 4), nil
}

func (d *Index) Read(r io.Reader) error {
	err := binary.Read(r, binary.BigEndian, &d.Did)
	if err != nil {
		return err
	}
	err = binary.Read(r, binary.BigEndian, &d.Offset)
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
	return nil
}
