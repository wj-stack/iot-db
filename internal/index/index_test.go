package index

import (
	"iot-db/internal/util"
	"testing"
)

func TestMergeFile(t *testing.T) {

}

func Test_merge(t *testing.T) {
	file, err := OpenFile("/home/wyatt/code/iot-db/internal/server", 0, "1679577439776976852-945379800829250161")
	if err != nil {
		return
	}
	tempFile, o, o2, _, err := util.CreateTempFile("/home/wyatt/code/iot-db/internal/server", 0)
	if err != nil {
		return
	}

	//1679583871400839946-3343019718063367058
	newFile := &File{
		SecondIndexMeta: nil,
		FirstIndexMeta:  nil,
		FirstIndex:      tempFile,
		SecondIndex:     o,
		DataFile:        o2,
	}
	merge([]*File{file}, newFile)
}

func Test_merge2(t *testing.T) {
	file, err := OpenFile("/home/wyatt/code/iot-db/internal/server", 0, "1679577439776976852-945379800829250161")
	if err != nil {
		return
	}
	file2, err := OpenFile("/home/wyatt/code/iot-db/internal/server", 0, "1679583871400839946-3343019718063367058")
	if err != nil {
		return
	}
	tempFile, o, o2, _, err := util.CreateTempFile("/home/wyatt/code/iot-db/internal/server", 0)
	if err != nil {
		return
	}

	newFile := &File{
		SecondIndexMeta: nil,
		FirstIndexMeta:  nil,
		FirstIndex:      tempFile,
		SecondIndex:     o,
		DataFile:        o2,
	}

	err = merge([]*File{file, file2}, newFile)
	if err != nil {
		return
	}
}
