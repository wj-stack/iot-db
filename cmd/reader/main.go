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

	r, err := reader.NewReader(manager)
	if err != nil {
		logrus.Fatalln(err)
	}

	t := time.Now().UnixMilli()
	query, err := r.Query(46612, 0, time.Now().UnixNano())
	if err != nil {
		logrus.Fatalln(err)
	}
	logrus.Infoln(time.Now().UnixMilli() - t)
	logrus.Infoln(len(query))

	t = time.Now().UnixMilli()
	query, err = r.Query(46612, 0, time.Now().UnixNano())
	if err != nil {
		logrus.Fatalln(err)
	}
	logrus.Infoln(time.Now().UnixMilli() - t)
	logrus.Infoln(len(query))

	//for _, i := range query {
	//	logrus.Infoln(i)
	//}
	////logrus.Infoln(len(query))

	//i := r.ReadSecondIndex()
	//for _, i := range i {
	//	logrus.Infoln(i.DeviceId, i.Size, i.FirstIndexSize, time.UnixMilli(i.Start/1e6), time.UnixMilli(i.End/1e6))
	//}
}
