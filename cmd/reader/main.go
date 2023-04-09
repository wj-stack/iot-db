package main

import (
	"iot-db/internal/compactor"
	"iot-db/internal/reader"
	"iot-db/internal/util"
)

func main() {
	// ("%s/data/%010d/%s/%s.first_index

	firstIndex, secondIndex, dataFile, err := util.OpenDataFile("/home/wyatt/code/iot-db/data",
		int(compactor.ShardGroupSize[0]), "0000000000", "1681025100175677208-5361817119047301016")
	if err != nil {
		panic(err)
	}

	_, err = reader.Query(0, 0, 9, firstIndex, secondIndex, dataFile)
	if err != nil {
		return
	}

	//for _, i := range query {
	//	logrus.Infoln(i)
	//}

	//secondIndexMeta := memorytable.SecondIndexMeta{
	//	Start:    0,
	//	End:      0,
	//	DeviceId: 0,
	//	Size:     0,
	//	Offset:   0,
	//	Flag:     0,
	//}
	//
	//file, err := os.Open("/home/wyatt/code/iot-db/data/tmp/86400000000000/0000000000/1681022345267431356-5577006791947779410.second_index")
	//if err != nil {
	//	return
	//}
	//
	//for {
	//	err := secondIndexMeta.ReadSecondIndexMeta(file)
	//	if err != nil {
	//		break
	//	}
	//	logrus.Infof("%#v\n", secondIndexMeta)
	//}
	//
	//file, err := os.Open("/home/wyatt/code/iot-db/data/tmp/86400000000000/0000000000/1681022345267431356-5577006791947779410.first_index")
	//if err != nil {
	//	return
	//}
	//firstIndexMeta := memorytable.FirstIndexMeta{}
	//
	//for {
	//	err := firstIndexMeta.ReadFirstIndexMeta(file)
	//	if err != nil {
	//		break
	//	}
	//	logrus.Infof("%#v\n", firstIndexMeta)
	//}

	//data := memorytable.Data{}
	//file, err := os.Open("/home/wyatt/code/iot-db/data/tmp/86400000000000/0000000000/1681022345267431356-5577006791947779410.data")
	//if err != nil {
	//	return
	//}
	//
	//for {
	//	err := data.Read(file)
	//	if err != nil {
	//		break
	//	}
	//	logrus.Infof("%#v\n", data)
	//}

}
