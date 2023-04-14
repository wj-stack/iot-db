package svc

import (
	"github.com/golang/protobuf/proto"
	"github.com/tidwall/wal"
	"iot-db/api/internal/config"
	conf "iot-db/internal/config"
	"iot-db/internal/filemanager"
	"iot-db/internal/pb"
	"iot-db/internal/shardgroup"
)

type ServiceContext struct {
	Config     config.Config
	ShardGroup *shardgroup.ShardGroup
	log        *wal.Log
}

func NewServiceContext(c config.Config) *ServiceContext {

	manager := filemanager.NewFileManager(&conf.Default)

	var shardGroup = shardgroup.NewShardGroup(manager)

	log, err := wal.Open(conf.Default.Core.Path.Wal, nil)
	if err != nil {
		panic(err)
	}

	firstIndex, err := log.FirstIndex()
	if err != nil {
		return nil
	}

	lastIndex, err := log.LastIndex()
	if err != nil {
		return nil
	}

	for i := firstIndex; i < lastIndex; i++ {
		read, err := log.Read(i)
		if err != nil {
			panic(err)
		}

		v := pb.Data{}
		err = proto.Unmarshal(read, &v)
		if err != nil {
			panic(err)
		}

		shardGroup.Insert(v.Did, v.Timestamp, v.CreatedAt, v.Body, v.PrivateData)
	}

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
		log:        log,
	}
}
