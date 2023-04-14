package main

import (
	"context"
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	dumpservice "gitlab.vidagrid.com/wyatt/dump-reader"
	"iot-db/internal/compactor"
	"iot-db/internal/config"
	"iot-db/internal/filemanager"
	"iot-db/internal/pb"
	"iot-db/internal/shardgroup"
	"time"
)

func main() {
	service, err := dumpservice.NewService(context.Background(), &dumpservice.SolarDumpService{
		Srv: "wj-test-0329",
	}, "postgres://delta:Delta123@127.0.0.1:5432/meta")
	if err != nil {
		logrus.Fatal(err)
	}
	manager := filemanager.NewFileManager(&config.Default)
	var shardGroup = shardgroup.NewShardGroup(manager)

	c := compactor.NewCompactor(manager)

	go func() {
		for {
			for i := 0; i < len(config.Default.Core.ShardGroupSize)-1; i++ {
				logrus.Infoln("compact:", config.Default.Core.ShardGroupSize[i], config.Default.Core.ShardGroupSize[i+1])
				err := c.Compact(i)
				if err != nil {
					logrus.Fatalln(err)
				}
				time.Sleep(time.Second * 10)
			}
		}
	}()

	logrus.SetLevel(logrus.TraceLevel)

	var solarAnalyzer SolarAnalyzer
	for i := 0; i < 100; i++ {
		message, err := service.FetchMessage()
		if err != nil {
			logrus.Fatal(err)
		}

		logrus.Infoln("message:", len(message), service.LastCommit().File, service.LastCommit().Cnt)
		for _, msg := range message {
			analyze, err := solarAnalyzer.Analyze(Message{
				Offset:    msg.Offset,
				CreatedAt: time.UnixMilli(int64(msg.Time)),
				DeviceId:  msg.DeviceId,
				Bytes:     msg.Data,
				Length:    msg.Len,
			})
			if err != nil {
				continue
			}

			for _, j := range analyze {
				if j.MsgType != 80 {
					continue
				}
				data := j.Data.(SolarMessage)
				solarData := data.GetSolarData()
				if solarData == nil {
					continue
				}

				k := []int64{}
				v := []int64{}
				for _, i := range solarData.Regs {
					v = append(v, int64(i[1]))
					k = append(k, int64(i[0]))
				}

				p := &pb.Array{Arr: v}
				marshal, err := proto.Marshal(p)
				if err != nil {
					logrus.Errorln(err)
					continue
				}
				field := &pb.Array{Arr: k}
				fieldMarshal, err := proto.Marshal(field)
				if err != nil {
					logrus.Errorln(err)
					continue
				}
				shardGroup.Insert(int64(j.DeviceId), j.UpdatedAt.UnixNano(), marshal, fieldMarshal)
			}

		}
		logrus.Infoln("CurrentSize ", shardGroup.CurrentSize)

		err = service.Commit(context.Background())
		if err != nil {
			return
		}

	}

	shardGroup.Clean()
}
