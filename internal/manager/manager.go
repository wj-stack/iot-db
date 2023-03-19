package manager

import (
	"encoding/binary"
	"fmt"
	"iot-db/internal/memorytable"
	"os"
	"sync"
	"time"
)

const QueueMaxSize = 32
const TableMaxSize = 2e20 * 2 // 2MB

type Manager struct {
	queue chan *memorytable.Table
	*memorytable.Table
	mutex sync.RWMutex
}

func OnInsert(t *memorytable.Table) {
	if t.Size() > TableMaxSize {

	}
}

func NewManager() *Manager {
	return &Manager{
		queue: make(chan *memorytable.Table, QueueMaxSize),
		Table: memorytable.NewTable(OnInsert),
	}
}

type Meta struct {
	MaxTime, MinTime int64
	KeyCount         int64
	Index            map[int64]int64 // deviceId - offset
	// Bloom
}

// filepath:  workspace/data/${level}/${now}.index
// filepath:  workspace/data/${level}/${now}.data

type FileManager struct {
	workspace string
}

func NewFileManager(workspace string) (*FileManager, error) {
	for i := 0; i < 7; i++ {
		err := os.MkdirAll(fmt.Sprintf("%s/tmp/%2d/", workspace, i), 0666)
		if err != nil {
			return nil, err
		}
		err = os.MkdirAll(fmt.Sprintf("%s/data/%2d/", workspace, i), 0666)
		if err != nil {
			return nil, err
		}
	}
	return &FileManager{
		workspace: workspace,
	}, nil
}

func (f *FileManager) Dump(table *memorytable.Table) error {
	now := time.Now().UnixNano()

	firstIndexPath := fmt.Sprintf("%s/tmp/%2d/%d.first_index", f.workspace, 0, now)
	secondIndexPath := fmt.Sprintf("%s/tmp/%2d/%d.second_index", f.workspace, 0, now)
	dataPath := fmt.Sprintf("%s/tmp/%2d/%d.data", f.workspace, 0, now)

	err := func() error {
		firstIndex, err := os.Create(firstIndexPath)
		if err != nil {
			return err
		}
		secondIndex, err := os.Create(secondIndexPath)
		if err != nil {
			return err
		}
		dataFile, err := os.Create(dataPath)
		if err != nil {
			return err
		}

		// TODO: second index add maxDeviceId and bloom

		var offset int
		err = table.List.Traversal(func(deviceId int64, timeList *memorytable.TimeSkipList) error {

			err := binary.Write(secondIndex, binary.BigEndian, timeList.GetStart())
			if err != nil {
				return err
			}

			err = binary.Write(secondIndex, binary.BigEndian, timeList.GetEnd())
			if err != nil {
				return err
			}

			err = binary.Write(secondIndex, binary.BigEndian, deviceId)
			if err != nil {
				return err
			}

			// count
			err = binary.Write(secondIndex, binary.BigEndian, timeList.Size())
			if err != nil {
				return err
			}

			err = timeList.Traversal(func(timestamp int64, data memorytable.Data) error {

				err := binary.Write(firstIndex, binary.BigEndian, timestamp)
				if err != nil {
					return err
				}

				err = binary.Write(firstIndex, binary.BigEndian, int64(offset))
				if err != nil {
					return err
				}

				n, err := dataFile.Write(data)
				offset += n
				return err
			})
			return err
		})

		return err
	}()
	if err != nil {
		os.Remove(firstIndexPath)
		os.Remove(secondIndexPath)
		os.Remove(dataPath)
	}
	return err
}
