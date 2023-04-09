package memorytable

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

func TestShardGroup_Insert(t *testing.T) {
	shardGroup := NewShardGroup("/home/wyatt/code/iot-db/data")

	var cnt int64 = 0x000ff00000000000

	for i := 0; i < 100; i++ {
		for j := 0; j < 10; j++ {
			buf := bytes.NewBuffer(make([]byte, 0))
			err := binary.Write(buf, binary.BigEndian, &cnt)
			if err != nil {
				return
			}
			shardGroup.Insert(int64(i), int64(j), buf.Bytes())
			cnt++
		}
	}
	shardGroup.clean()
	time.Sleep(time.Second * 3)

}
