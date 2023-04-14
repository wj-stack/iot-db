package main

import (
	"github.com/sirupsen/logrus"
	"iot-db/internal/compactor"
	"iot-db/internal/config"
	"iot-db/internal/filemanager"
)

func main() {
	manager := filemanager.NewFileManager(&config.Default)
	c := compactor.NewCompactor(manager)
	logrus.SetReportCaller(true)
	err := c.Compact(0)
	if err != nil {
		logrus.Fatalln(err)
	}

	return
}
