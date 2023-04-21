package main

import (
	"context"
	"github.com/sirupsen/logrus"
	dumpservice "gitlab.vidagrid.com/wyatt/dump-reader"
	"iot-db/internal/engine"
	"iot-db/internal/pb"
	"time"
)

func main() {

	logrus.SetReportCaller(true)
	service, err := dumpservice.NewService(context.Background(), &dumpservice.SolarDumpService{
		Srv: "solar_cake",
	}, "postgres://delta:Delta123@127.0.0.1:5432/meta")
	if err != nil {
		logrus.Fatal("psql", err)
	}

	e := engine.NewDefaultEngine()

	var solarAnalyzer SolarAnalyzer
	for {
		message, err := service.FetchMessage()
		if err != nil {
			logrus.Fatal("fetch", err)
		}
		var pbData []*pb.FullData
		logrus.Infoln("message:", len(message), service.LastCommit().File, service.LastCommit().Cnt, time.Now())
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

				var k []int32
				var v []int64
				for _, i := range solarData.Regs {
					v = append(v, int64(i[1]))
					k = append(k, int32(i[0]))
				}
				pbData = append(pbData, &pb.FullData{
					Did:       int64(j.DeviceId),
					Timestamp: j.UpdatedAt.UnixNano(),
					CreatedAt: time.Now().UnixNano(),
					Key:       k,
					Value:     v,
				})
				//if int64(j.DeviceId) == 35008 {
				//	logrus.Infoln(j, j.CreatedAt, j.UpdatedAt, string(msg.Data))
				//}
			}

		}
		t := time.Now()
		err = e.Insert(pbData)
		if err != nil {
			logrus.Fatalln(err)
		}
		logrus.Infoln("speed:", time.Now().UnixMilli()-t.UnixMilli(), "ms", len(pbData), "Size:", e.Size())

		err = service.Commit(context.Background())
		if err != nil {
			return
		}

	}

}
