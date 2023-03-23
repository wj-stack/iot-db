package util

import (
	"fmt"
	"math/rand"
	"os"
	"time"
)

func IsDataFileExist(workspace string, level int, name string) bool {
	_, err := os.Stat(fmt.Sprintf("%s/data/%02d/%s.first_index", workspace, level, name))
	if err != nil {
		return false
	}
	_, err = os.Stat(fmt.Sprintf("%s/data/%02d/%s.second_index", workspace, level, name))
	if err != nil {
		return false
	}
	_, err = os.Stat(fmt.Sprintf("%s/data/%02d/%s.data", workspace, level, name))
	if err != nil {
		return false
	}
	return true
}

func CreateTempFile(workspace string, i int) (*os.File, *os.File, *os.File, string, error) {
	t := time.Now().UnixNano()
	random := rand.Int()

	firstIndex, err := os.Create(fmt.Sprintf("%s/tmp/%02d/%d-%d.first_index", workspace, i, t, random))
	if err != nil {
		return nil, nil, nil, "", err
	}
	secondIndex, err := os.Create(fmt.Sprintf("%s/tmp/%02d/%d-%d.second_index", workspace, i, t, random))
	if err != nil {
		_ = firstIndex.Close()
		return nil, nil, nil, "", err
	}
	dataFile, err := os.Create(fmt.Sprintf("%s/tmp/%02d/%d-%d.data", workspace, i, t, random))
	if err != nil {
		_ = firstIndex.Close()
		_ = secondIndex.Close()
		return nil, nil, nil, "", err
	}
	return firstIndex, secondIndex, dataFile, fmt.Sprintf("%d-%d", t, random), nil
}

func OpenDataFile(workspace string, i int, name string) (*os.File, *os.File, *os.File, error) {
	firstIndex, err := os.Open(fmt.Sprintf("%s/data/%02d/%s.first_index", workspace, i, name))
	if err != nil {
		return nil, nil, nil, err
	}
	secondIndex, err := os.Open(fmt.Sprintf("%s/data/%02d/%s.second_index", workspace, i, name))
	if err != nil {
		_ = firstIndex.Close()
		return nil, nil, nil, err
	}
	dataFile, err := os.Open(fmt.Sprintf("%s/data/%02d/%s.data", workspace, i, name))
	if err != nil {
		_ = firstIndex.Close()
		_ = secondIndex.Close()
		return nil, nil, nil, err
	}
	return firstIndex, secondIndex, dataFile, nil
}

func ReTempName(workspace string, i int, src string, j int, desc string) error {
	err := os.Rename(fmt.Sprintf("%s/tmp/%02d/%s.data", workspace, i, src), fmt.Sprintf("%s/data/%02d/%s.data", workspace, j, desc))
	if err != nil {
		return err
	}
	err = os.Rename(fmt.Sprintf("%s/tmp/%02d/%s.first_index", workspace, i, src), fmt.Sprintf("%s/data/%02d/%s.first_index", workspace, j, desc))
	if err != nil {
		return err
	}
	err = os.Rename(fmt.Sprintf("%s/tmp/%02d/%s.second_index", workspace, i, src), fmt.Sprintf("%s/data/%02d/%s.second_index", workspace, j, desc))
	if err != nil {
		return err
	}
	return nil
}
