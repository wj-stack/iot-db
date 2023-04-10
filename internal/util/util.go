package util

import (
	"bufio"
	"fmt"
	"github.com/pierrec/lz4/v4"
	"io"
	"math/rand"
	"os"
	"time"
)

func IsDataFileExist(workspace string, size int, shardId, name string) bool {
	_, err := os.Stat(fmt.Sprintf("%s/data/%010d/%s/%s.first_index", workspace, size, shardId, name))
	if err != nil {
		return false
	}
	_, err = os.Stat(fmt.Sprintf("%s/data/%010d/%s/%s.second_index", workspace, size, shardId, name))
	if err != nil {
		return false
	}
	_, err = os.Stat(fmt.Sprintf("%s/data/%010d/%s/%s.data", workspace, size, shardId, name))
	if err != nil {
		return false
	}
	return true
}

func CreateTempFile(workspace string, shardGroup int, shardGroupId int) (*os.File, *os.File, *os.File, string, error) {
	err := os.MkdirAll(fmt.Sprintf("%s/tmp/%010d/%010d/", workspace, shardGroup, shardGroupId), 0777)
	if err != nil {
		return nil, nil, nil, "", err
	}

	t := time.Now().UnixNano()
	random := rand.Int()

	firstIndex, err := os.OpenFile(fmt.Sprintf("%s/tmp/%010d/%010d/%d-%d.first_index", workspace, shardGroup, shardGroupId, t, random), os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return nil, nil, nil, "", err
	}
	secondIndex, err := os.OpenFile(fmt.Sprintf("%s/tmp/%010d/%010d/%d-%d.second_index", workspace, shardGroup, shardGroupId, t, random), os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		_ = firstIndex.Close()
		return nil, nil, nil, "", err
	}
	dataFile, err := os.OpenFile(fmt.Sprintf("%s/tmp/%010d/%010d/%d-%d.data", workspace, shardGroup, shardGroupId, t, random), os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		_ = firstIndex.Close()
		_ = secondIndex.Close()
		return nil, nil, nil, "", err
	}
	return firstIndex, secondIndex, dataFile, fmt.Sprintf("%d-%d", t, random), nil
}

func OpenDataFile(workspace string, i int, j string, name string) (*os.File, *os.File, *os.File, error) {
	firstIndex, err := os.Open(fmt.Sprintf("%s/data/%010d/%s/%s.first_index", workspace, i, j, name))
	if err != nil {
		return nil, nil, nil, err
	}
	secondIndex, err := os.Open(fmt.Sprintf("%s/data/%010d/%s/%s.second_index", workspace, i, j, name))
	if err != nil {
		_ = firstIndex.Close()
		return nil, nil, nil, err
	}
	dataFile, err := os.Open(fmt.Sprintf("%s/data/%010d/%s/%s.data", workspace, i, j, name))
	if err != nil {
		_ = firstIndex.Close()
		_ = secondIndex.Close()
		return nil, nil, nil, err
	}
	return firstIndex, secondIndex, dataFile, nil
}

func ReTempName(workspace string, shardGroup int, shardGroupId int, src string) error {
	err := os.MkdirAll(fmt.Sprintf("%s/data/%010d/%010d/", workspace, shardGroup, shardGroupId), 0777)
	if err != nil {
		return err
	}

	err = os.Rename(fmt.Sprintf("%s/tmp/%010d/%010d/%s.data", workspace, shardGroup, shardGroupId, src),
		fmt.Sprintf("%s/data/%010d/%010d/%s.data", workspace, shardGroup, shardGroupId, src))
	if err != nil {
		return err
	}
	err = os.Rename(fmt.Sprintf("%s/tmp/%010d/%010d/%s.first_index", workspace, shardGroup, shardGroupId, src),
		fmt.Sprintf("%s/data/%010d/%010d/%s.first_index", workspace, shardGroup, shardGroupId, src))
	if err != nil {
		return err
	}
	err = os.Rename(fmt.Sprintf("%s/tmp/%010d/%010d/%s.second_index", workspace, shardGroup, shardGroupId, src),
		fmt.Sprintf("%s/data/%010d/%010d/%s.second_index", workspace, shardGroup, shardGroupId, src))
	if err != nil {
		return err
	}
	return nil
}

func Compress(reader io.Reader, writer io.Writer) error {
	// open input file
	// make a read buffer
	r := bufio.NewReader(reader)

	// make an lz4 write buffer
	w := lz4.NewWriter(writer)

	// make a buffer to keep chunks that are read
	buf := make([]byte, 1024)
	for {
		// read a chunk
		n, err := r.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}

		// write a chunk
		if _, err := w.Write(buf[:n]); err != nil {
			panic(err)
		}
	}

	if err := w.Flush(); err != nil {
		return err
	}
	return nil
}

func DeCompress(reader io.Reader, writer io.Writer) error {
	// open input file

	// make an lz4 read buffer
	r := lz4.NewReader(reader)

	// make a write buffer
	w := bufio.NewWriter(writer)

	// make a buffer to keep chunks that are read
	buf := make([]byte, 1024)
	for {
		// read a chunk
		n, err := r.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}

		// write a chunk
		if _, err := w.Write(buf[:n]); err != nil {
			return err
		}
	}

	if err := w.Flush(); err != nil {
		return err
	}
	return nil
}
