package main

import (
	"context"
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	dumpservice "gitlab.vidagrid.com/wyatt/dump-reader"
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

	//c := compactor.NewCompactor(manager)
	//go func() {
	//	for i := 0; i < len(config.Default.Core.ShardGroupSize)-1; i++ {
	//		logrus.Infoln("compact:", config.Default.Core.ShardGroupSize[i], config.Default.Core.ShardGroupSize[i+1])
	//		err := c.Compact(i)
	//		if err != nil {
	//			logrus.Fatalln(err)
	//		}
	//		time.Sleep(time.Second * 10)
	//	}
	//
	//}()
	logrus.SetLevel(logrus.TraceLevel)

	var solarAnalyzer SolarAnalyzer
	for {
		message, err := service.FetchMessage()
		if err != nil {
			logrus.Fatal(err)
		}

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
				v := map[int32]int64{}
				for _, i := range solarData.Regs {
					v[int32(i[0])] = int64(i[1])
				}
				p := &pb.Data{Kv: v}
				marshal, err := proto.Marshal(p)
				if err != nil {
					logrus.Errorln(err)
					continue
				}
				logrus.Infoln("marshal len: ", len(marshal), len(v)*(8+2))
				shardGroup.Insert(int64(j.DeviceId), j.UpdatedAt.UnixNano(), marshal)

			}

			// ms -> ns
			shardGroup.Insert(int64(msg.DeviceId), int64(msg.Time*1e6), msg.Data)
		}

		err = service.Commit(context.Background())
		if err != nil {
			return
		}
	}

}
