package memorytable

import (
	"encoding/binary"
	"io"
	"os"
	"unsafe"
)

const FirstIndexMetaSize = unsafe.Sizeof(new(FirstIndexMeta))
const SecondIndexMetaSize = unsafe.Sizeof(new(SecondIndexMeta))

// Data point data
type Data struct {
	Length    int32
	Flag      byte
	Timestamp int64
	CreatedAt int64
	Body      []byte
}

func (d *Data) Write(writer io.Writer) error {
	err := binary.Write(writer, binary.BigEndian, d.Length)
	if err != nil {
		return err
	}
	err = binary.Write(writer, binary.BigEndian, d.Flag)
	if err != nil {
		return err
	}
	err = binary.Write(writer, binary.BigEndian, d.Timestamp)
	if err != nil {
		return err
	}
	err = binary.Write(writer, binary.BigEndian, d.CreatedAt)
	if err != nil {
		return err
	}
	_, err = writer.Write(d.Body)
	if err != nil {
		return err
	}
	return nil
}

func (d *Data) Read(r io.Reader) error {
	err := binary.Read(r, binary.BigEndian, &d.Length)
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

type FirstIndexMeta struct {
	Timestamp int64
	Offset    int64
}

func (v *FirstIndexMeta) WriteFirstIndex(firstIndex *os.File) error {
	// write first index
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

func (v *FirstIndexMeta) ReadFirstIndexMeta(firstIndex *os.File) error {

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

type SecondIndexMeta struct {
	Start, End int64
	DeviceId   int64
	Size       int64
	Offset     int64
	Flag       byte
}

func (meta *SecondIndexMeta) ReadSecondIndexMeta(secondIndex *os.File) error {
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
	return nil
}
