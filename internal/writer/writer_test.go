package writer

import (
	"testing"
)

func TestWriter_Write(t *testing.T) {
	//writer, err := NewWriter("/home/wyatt/code/iot-db/data", 100*10, 10)
	//if err != nil {
	//	panic(err)
	//}
	//var cnt int64
	//for i := 0; i < 100; i++ {
	//	for j := 0; j < 10; j++ {
	//		buf := bytes.NewBuffer(make([]byte, 0))
	//		err := binary.Write(buf, binary.BigEndian, &cnt)
	//		if err != nil {
	//			return
	//		}
	//		err = writer.WriteData(memorytable.Data{
	//			DeviceId:  int64(i),
	//			Length:    8,
	//			Flag:      0,
	//			Timestamp: int64(j),
	//			CreatedAt: int64(j),
	//			Body:      buf.Bytes(),
	//		})
	//		if err != nil {
	//			panic(err)
	//		}
	//		cnt++
	//		if j%2 == 0 || j == 0 {
	//			err := writer.WriteFirstIndex()
	//			if err != nil {
	//				panic(err)
	//			}
	//
	//		}
	//	}
	//	err := writer.WriteSecondIndex(int64(i))
	//	if err != nil {
	//		panic(err)
	//	}
	//}
	//
}
