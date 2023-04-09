package main

import (
	"context"
	"github.com/sirupsen/logrus"
	dumpservice "gitlab.vidagrid.com/wyatt/dump-reader"
	"iot-db/internal/memorytable"
)

var shardGroup = memorytable.NewShardGroup("/home/delta/iot-db/data")

func main() {
	service, err := dumpservice.NewService(context.Background(), &dumpservice.SolarDumpService{
		Srv: "wj-test-0329",
	}, "postgres://delta:Delta123@127.0.0.1:5432/meta")
	if err != nil {
		logrus.Fatal(err)
	}

	for {
		message, err := service.FetchMessage()
		if err != nil {
			logrus.Fatal(err)
		}

		for _, msg := range message {
			//logrus.Infoln(msg)
			// ms -> ns
			shardGroup.Insert(int64(msg.DeviceId), int64(msg.Time*1e6), msg.Data)
		}

	}

}
