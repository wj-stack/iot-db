package main

import (
	"github.com/sirupsen/logrus"
	"iot-db/internal/compactor"
	"iot-db/internal/shardgroup"
)

func main() {

	compactor.SetWorkspace("/home/delta/iot-db/data")
	tasks := compactor.GenerateTasks(shardgroup.ShardGroupSize[0], shardgroup.ShardGroupSize[1], 6)
	logrus.Infoln(tasks)
	err := compactor.Compact(shardgroup.ShardGroupSize[0], shardgroup.ShardGroupSize[1], tasks[0])
	if err != nil {
		logrus.Fatalln(err)
		return
	}
	return
}
