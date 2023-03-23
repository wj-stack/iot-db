package main

import (
	"iot-db/internal/data"
	"iot-db/internal/server"
	"sync"
	"time"
)

func main() {
	engine := server.NewEngine(".")
	wp := sync.WaitGroup{}
	wp.Add(1000)
	for i := 0; i < 1000; i++ {
		i := i
		go func() {
			err := engine.Write(&data.BinaryData{
				DeviceId:  int64(i),
				Timestamp: int64(i),
				Body:      []byte("1111"),
			})
			if err != nil {
				panic(err)
			}
			wp.Done()
		}()
	}
	wp.Wait()
	time.Sleep(time.Second * 10)
}
