package main

import (
	"github.com/sirupsen/logrus"
	"iot-db/internal/compactor"
	"iot-db/internal/filemanager"
)

func main() {
	var workspace = "/data/iot-db/data"

	manager := filemanager.NewFileManager(workspace)
	c := compactor.NewCompactor(manager)

	err := c.Compact(0)
	if err != nil {
		logrus.Fatalln(err)
	}

	return
}
