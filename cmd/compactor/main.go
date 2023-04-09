package main

import (
	"github.com/sirupsen/logrus"
	"iot-db/internal/compactor"
)

func main() {
	tasks := compactor.GenerateTasks(compactor.ShardGroupSize[0], compactor.ShardGroupSize[1], 0)
	logrus.Infoln(tasks)
	err := compactor.Compact(compactor.ShardGroupSize[0], compactor.ShardGroupSize[1], tasks[0])
	if err != nil {
		logrus.Fatalln(err)
		return
	}
	return
}
