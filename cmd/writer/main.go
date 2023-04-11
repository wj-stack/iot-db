package main

import (
	"context"
	"github.com/sirupsen/logrus"
	dumpservice "gitlab.vidagrid.com/wyatt/dump-reader"
	"iot-db/internal/compactor"
	"iot-db/internal/filemanager"
	"iot-db/internal/shardgroup"
	"time"
)

var workspace = "/data/iot-db/data"

func main() {
	service, err := dumpservice.NewService(context.Background(), &dumpservice.SolarDumpService{
		Srv: "wj-test-0329",
	}, "postgres://delta:Delta123@127.0.0.1:5432/meta")
	if err != nil {
		logrus.Fatal(err)
	}
	manager := filemanager.NewFileManager(workspace)
	var shardGroup = shardgroup.NewShardGroup(manager)

	c := compactor.NewCompactor(manager)
	go func() {
		for {
			for i := 0; i < len(shardgroup.ShardGroupSize)-1; i++ {
				logrus.Infoln("compact", shardgroup.ShardGroupSize[i])
				err = c.Compact(i)
				if err != nil {
					logrus.Fatalln(err)
				}
			}
			time.Sleep(time.Second * 10)
		}

	}()
	logrus.SetLevel(logrus.TraceLevel)

	for {
		message, err := service.FetchMessage()
		if err != nil {
			logrus.Fatal(err)
		}

		for _, msg := range message {
			// ms -> ns
			shardGroup.Insert(int64(msg.DeviceId), int64(msg.Time*1e6), msg.Data)
		}

		err = service.Commit(context.Background())
		if err != nil {
			return
		}
	}

}
