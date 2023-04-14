package engine

import (
	"github.com/golang/protobuf/proto"
	"github.com/tidwall/wal"
	"iot-db/internal/pb"
	"sync"
)

type Engine interface {
	Insert(did int64, timestamp int64, created int, data map[int64]int64) error
	Query(did int64, start, end int64) (map[int64]int64, error)
	Delete(did int64, start, end int64) error
}

type Cake struct {
	wal.Log
	mutex sync.RWMutex
}

func (c *Cake) Init() {

}

func (c *Cake) Insert(data *pb.Data) error {
	// wal
	marshal, err := proto.Marshal(data)
	if err != nil {
		return err
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()

	lastIndex, err := c.Log.LastIndex()
	if err != nil {
		return err
	}

	err = c.Log.Write(lastIndex+1, marshal)
	if err != nil {
		return err
	}

	// memory -> wal index

	return nil
}

func (c *Cake) Query(did int64, start, end int64) (map[int64]int64, error) {
	// query memory

	// query desk
	return nil, nil
}

func (c *Cake) Delete(did int64, start, end int64) error {
	// write wal del command

	// del memory
	return nil
}
