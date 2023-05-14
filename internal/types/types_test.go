package types

import (
	"encoding/json"
	"fmt"
	"github.com/dablelv/go-huge-util/conv"
	"github.com/golang/protobuf/proto"
	"iot-db/internal/pb"
	"math/rand"
	"testing"
)

func Test_flag(t *testing.T) {
	//logrus.Infoln(RemoveFlag)
	//logrus.Infoln(ZipFlag)
}

func TestEngine_GenJson(t *testing.T) {
	rand.Seed(20230423)
	s := ""
	s1 := ""
	s2 := ""
	for i := 0; i < 100; i++ {
		v := map[uint16]int64{}
		var arr []int64
		for i := 0; i < 120; i++ {
			v[uint16(rand.Int()%65535)] = int64(rand.Int())
		}
		marshal, err := json.Marshal(v)
		if err != nil {
			return
		}

		for k, v := range v {
			arr = append(arr, int64(k))
			arr = append(arr, v)
		}

		array := pb.Array{Arr: arr}
		bytes, err := proto.Marshal(&array)
		if err != nil {
			return
		}

		//fmt.Println("json:", len(marshal), "binary:", len(v)*10, "protobuf:", len(bytes))
		s += conv.ToAny[string](len(marshal)) + ","
		s1 += conv.ToAny[string](len(v)*10) + ","
		s2 += conv.ToAny[string](len(bytes)) + ","
	}
	fmt.Println(s)
	fmt.Println(s1)
	fmt.Println(s2)
}
