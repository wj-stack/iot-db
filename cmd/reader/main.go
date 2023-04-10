package main

import (
	"github.com/sirupsen/logrus"
	"iot-db/internal/reader"
	"time"
)

func main() {
	// ("%s/data/%010d/%s/%s.first_index

	//firstIndex, secondIndex, dataFile, err := util.OpenDataFile("/home/delta/iot-db/data/data",
	//	int(shardgroup.ShardGroupSize[1]), "0000019454", "1681096366763968877-5577006791947779410")
	//if err != nil {
	//	panic(err)
	//}
	// /home/delta/iot-db/data/data/3600000000000/0000466912/
	//r, err := reader.NewReader("/home/delta/iot-db/data/data/3600000000000/0000466912", "1681096289466204035-2150422575057484888")
	//if err != nil {
	//	return
	//}

	r, err := reader.NewReader("/home/delta/iot-db/data/data/86400000000000/0000019454", "1681106645504239211-5577006791947779410")
	if err != nil {
		return
	}

	secondIndex := r.ReadSecondIndex()
	for _, i := range secondIndex {
		logrus.Infof("%#v\n", i)
		//firstIndex := r.ReadFirstIndexN(int(secondIndex[i].FirstIndexSize))
		//for _, s := range firstIndex {
		//	logrus.Infof("firstIndex:%#v\n", s)
		//}
	}

	//logrus.Infof("%#v\n", secondIndex[0])

	r.Query(25464, 0, time.Now().UnixNano())

	//firstIndex := r.ReadFirstIndex()
	//for _, s := range firstIndex {
	//	logrus.Infof("firstIndex:%#v\n", s)
	//}

	//_, err = reader.Query(0, 0, 9, firstIndex, secondIndex, dataFile)
	//if err != nil {
	//	return
	//}

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
