package main

import (
	"github.com/sirupsen/logrus"
	"iot-db/internal/engine"
	"time"
)

func main() {
	logrus.SetReportCaller(true)
	defaultEngine := engine.NewDefaultEngine()
	for i := 30000; i < 50000; i++ {
		t := time.Now()
		key, values, err := defaultEngine.Query(int64(i), 0, time.Now().UnixNano())
		if err != nil {
			logrus.Fatalln(err)
		}
		logrus.Infoln(time.Now().UnixMilli()-t.UnixMilli(), len(values), len(key))

		t = time.Now()
		key, values, err = defaultEngine.Query(int64(i), 0, time.Now().UnixNano())
		if err != nil {
			logrus.Fatalln(err)

		}
		logrus.Infoln(time.Now().UnixMilli()-t.UnixMilli(), len(values), len(key))
	}

}
