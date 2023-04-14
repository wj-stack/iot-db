package main

import (
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"io"
	"iot-db/internal/config"
	"iot-db/internal/datastructure"
	"iot-db/internal/filemanager"
	"iot-db/internal/pb"
	"iot-db/internal/reader"
	"iot-db/internal/writer"
	"time"
)

var workspace = "/data/iot-db/data/data"

func main() {

	file, err := filemanager.OpenFile(workspace, "604800000000000_2672_0_0_1681455680442843615")
	if err != nil {
		return

	}

	logrus.SetLevel(logrus.TraceLevel)
	manager := filemanager.NewFileManager(&config.Default)

	r, err := reader.NewReader(manager)
	if err != nil {
		logrus.Fatalln(err)
	}

	secondIndex := r.ReadSecondIndex(file.SecondIndex)
	logrus.Infoln(len(secondIndex), "len(secondIndex)")
	for _, i := range secondIndex {
		logrus.Infof("%#v\n", i, time.UnixMilli(i.Start/1e6), time.UnixMilli(i.End/1e6))
	}
	logrus.Infoln(secondIndex)

	for _, i := range secondIndex {
		_, err := file.FirstIndex.Seek(i.Offset, io.SeekStart)
		if err != nil {
			return
		}
		body, _ := writer.ReadFirstIndexHeader(file.FirstIndex)
		//logrus.Infoln(body)
		field := pb.Array{}
		err = proto.Unmarshal(body, &field)
		if err != nil {
			logrus.Errorln(err)
		}
		logrus.Infoln(field.Arr)
		for j := 0; j < int(i.FirstIndexSize); j++ {
			f := datastructure.FirstIndexMeta{}
			err := f.ReadFirstIndexMeta(file.FirstIndex)
			if err != nil {
				return
			}
			logrus.Infoln(f, time.UnixMilli(f.Timestamp/1e6))
		}
	}
	//logrus.Infoln(secondIndex)
	//
	//for {
	//	data := datastructure.Data{}
	//	err := data.Read(file.DataFile)
	//	if err != nil {
	//		break
	//	}
	//	logrus.Infof("%#v\n", data)
	//	value := pb.Array{}
	//	err = proto.Unmarshal(data.Body, &value)
	//	logrus.Infoln(value.Arr)
	//}

	//
	//t := time.Now().UnixMilli()
	//query, header, err := r.Query(46612, 0, time.Now().UnixNano())
	//if err != nil {
	//	logrus.Fatalln(err)
	//}
	//logrus.Infoln(time.Now().UnixMilli() - t)
	//logrus.Infoln(len(query), header)
	//
	//t = time.Now().UnixMilli()
	//query, header, err = r.Query(46612, 0, time.Now().UnixNano())
	//if err != nil {
	//	logrus.Fatalln(err)
	//}
	//logrus.Infoln(time.Now().UnixMilli() - t)
	//logrus.Infoln(len(query), header)

	//for _, i := range query {
	//	logrus.Infoln(i)
	//}
	////logrus.Infoln(len(query))

	//i := r.ReadSecondIndex()
	//for _, i := range i {
	//	logrus.Infoln(i.DeviceId, i.Size, i.FirstIndexSize, time.UnixMilli(i.Start/1e6), time.UnixMilli(i.End/1e6))
	//}
}
