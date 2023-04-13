package main

import (
	"github.com/sirupsen/logrus"
	"iot-db/internal/config"
	"iot-db/internal/filemanager"
	"iot-db/internal/reader"
	"time"
)

var workspace = "/data/iot-db/data"

func main() {

	logrus.SetLevel(logrus.TraceLevel)
	manager := filemanager.NewFileManager(&config.Default)

	file, err := manager.OpenFile("86400000000000_19317_0_0_1681371447986751129")
	if err != nil {
		logrus.Fatalln(err)
	}

	r, err := reader.NewReader(file)
	if err != nil {
		logrus.Fatalln(err)
	}

	t := time.Now().UnixMilli()
	query, err := r.Query(46612, 0, time.Now().UnixNano())
	if err != nil {
		logrus.Fatalln(err)
	}
	logrus.Infoln(time.Now().UnixMilli() - t)
	//logrus.Infoln(time.UnixMilli(query[0].Timestamp/1e6), time.UnixMilli(query[len(query)-1].Timestamp/1e6))

	for _, i := range query {
		logrus.Infoln(i)
	}
	////logrus.Infoln(len(query))

	//i := r.ReadSecondIndex()
	//for _, i := range i {
	//	logrus.Infoln(i.DeviceId, i.Size, i.FirstIndexSize, time.UnixMilli(i.Start/1e6), time.UnixMilli(i.End/1e6))
	//}
}
