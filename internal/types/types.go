package types

import (
	"bytes"
	"encoding/binary"
)

type Data []int64

type Index struct {
	DeviceId  uint32
	Offset    uint64
	StartTime uint64
	EndTime   uint64
	Flag      byte
}

type DataPoint struct {
	Timestamp uint64
	DeviceId  uint32
	Data      Data
}

const MagicWord = int32(0x12345678)

func WriteBlock(dataPoint []DataPoint) ([]byte, error) {
	// magic
	buf := bytes.NewBuffer([]byte{})

	for _, i := range dataPoint {
		err := binary.Write(buf, binary.BigEndian, i.Data)
		if err != nil {
			return nil, err
		}
		err = binary.Write(buf, binary.BigEndian, i.Timestamp)
		if err != nil {
			return nil, err
		}
	}

	// zip

	return buf.Bytes(), nil
}

func Write(dataPoint []DataPoint) ([]byte, error) {
	// magic
	buf := bytes.NewBuffer([]byte{})

	for _, i := range dataPoint {
		err := binary.Write(buf, binary.BigEndian, i.Data)
		if err != nil {
			return nil, err
		}
		err = binary.Write(buf, binary.BigEndian, i.Timestamp)
		if err != nil {
			return nil, err
		}

	}

	// zip

	return buf.Bytes(), nil
}

// 一个设备只会在一个block当中,一个block可以有多个设备
//  [Block][Block][Block][Block][Block] []
// Index = [deviceId,blockOffset,]
// Block = [[.寄存器信息.][.时间戳.][.寄存器信息.][.时间戳.]...[.寄存器信息.][.时间戳.]]
