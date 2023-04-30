package main

import (
	"github.com/sirupsen/logrus"
	"iot-db/internal/engine"
	"os"
)

func main() {

	file, err := os.Open("/data/iot-db/data/data/604800000000000/2759/604800000000000_2759_1682228682901132262_index")
	if err != nil {
		return
	}
	for {
		i := engine.Index{}
		err := i.Read(file)
		if err != nil {
			break
		}
		logrus.Infoln(i)
	}
}
