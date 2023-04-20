package main

import (
	"github.com/sirupsen/logrus"
	"iot-db/internal/compactor"
	"iot-db/internal/filemanager"
)

var workspace = "/home/delta/iot-db/data"

func main() {
	logrus.SetLevel(logrus.TraceLevel)
	manager := filemanager.NewFileManager(workspace)

	c := compactor.NewCompactor(manager)
	err := c.Compact(0)
	if err != nil {
		return
	}
}
