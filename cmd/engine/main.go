package main

import (
	"iot-db/internal/engine"
	"time"
)

func main() {
	e := engine.NewDefaultEngine()
	e.Query(26754, 0, time.Now().UnixNano())
}
