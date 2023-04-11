package main

import (
	"github.com/sirupsen/logrus"
	"iot-db/internal/compactor"
	"iot-db/internal/config"
	"iot-db/internal/shardgroup"
	"time"
)

func main() {

	// start shard group
	//var shardGroup = shardgroup.NewShardGroup(config.Default.workspace)

	// start wal

	// start compactor
	go func() {
		logrus.Infoln("start compactor", config.Default.Workspace)
		compactor.SetWorkspace(config.Default.Workspace)
		for {
			for i := 0; i < len(shardgroup.ShardGroupSize)-1; i++ {
				tasks := compactor.GenerateTasks(shardgroup.ShardGroupSize[i], shardgroup.ShardGroupSize[i+1], config.Default.SaveFile)
				for _, task := range tasks {
					if len(task.dirs) == 0 {
						continue
					}

					logrus.Infoln("start compact task", task.ShardGroupId, task.dirs)

					err := compactor.Compact(shardgroup.ShardGroupSize[i], shardgroup.ShardGroupSize[i+1], task)
					if err != nil {
						logrus.Fatalln(err)
					}
					time.Sleep(time.Second * 10)
				}
			}
		}
	}()

	// start web-server

}
