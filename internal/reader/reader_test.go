package reader

import (
	"github.com/sirupsen/logrus"
	"testing"
)

func TestReader_ReadSecondIndex(t *testing.T) {
	reader, err := NewReader("/home/wyatt/code/iot-db/data/tmp/86400000000000/0000000000", "1681037126865045781-5577006791947779410")
	if err != nil {
		panic(err)
	}
	secondIndex := reader.ReadSecondIndex()
	for _, s := range secondIndex {
		logrus.Infof("secondIndex:%#v\n", s)
	}
	firstIndex := reader.ReadFirstIndex()
	for _, s := range firstIndex {
		logrus.Infof("firstIndex:%#v\n", s)
	}
	err = reader.SeekDataFile(36963)
	if err != nil {
		panic(err)
	}
	data, err := reader.ReadData()
	if err != nil {
		panic(err)
	}
	logrus.Infof("data:%#v\n", data)
}
