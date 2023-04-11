package svc

import (
	"iot-db/api/internal/config"
	"iot-db/internal/shardgroup"
)

type ServiceContext struct {
	Config     config.Config
	ShardGroup *shardgroup.ShardGroup
}

func NewServiceContext(c config.Config) *ServiceContext {

	var shardGroup = shardgroup.NewShardGroup(c.Workspace)
	// read wal

	// start compactor
	//go func() {
	//	logrus.Infoln("start compactor", c.workspace)
	//	compactor.SetWorkspace(c.workspace)
	//	for {
	//		for i := 0; i < len(shardgroup.ShardGroupSize)-1; i++ {
	//			tasks := compactor.GenerateTasks(shardgroup.ShardGroupSize[i], shardgroup.ShardGroupSize[i+1], c.SaveFile)
	//			for _, task := range tasks {
	//				if len(task.dirs) == 0 {
	//					continue
	//				}
	//
	//				logrus.Infoln("start compact task", task.ShardGroupId, task.dirs)
	//
	//				err := compactor.Compact(shardgroup.ShardGroupSize[i], shardgroup.ShardGroupSize[i+1], task)
	//				if err != nil {
	//					logrus.Fatalln(err)
	//				}
	//				time.Sleep(time.Second * 10)
	//			}
	//		}
	//	}
	//}()

	return &ServiceContext{
		Config:     c,
		ShardGroup: shardGroup,
	}
}
