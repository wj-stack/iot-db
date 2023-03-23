package server

import (
	"fmt"
	"iot-db/internal/index"
	"strconv"
	"strings"
	"testing"
)

func TestCake_Write(t *testing.T) {

	c := NewEngine(".")
	fmt.Println(c)

	for i := 0; i < 10000; i++ {
		c.Insert(int64(i), int64(i+1), []byte(strconv.Itoa(i+2)))
		c.Insert(int64(i+1), int64(i+1), []byte(strconv.Itoa(i+2)))
		c.Insert(int64(i+2), int64(i+1), []byte(strconv.Itoa(i+2)))
		c.Insert(int64(i+3), int64(i+1), []byte(strconv.Itoa(i+2)))
	}
	err := c.Dump(c.Table)
	if err != nil {
		panic(err)
	}
}

func TestCake_Write2(t *testing.T) {

	c := NewEngine(".")
	fmt.Println(c)

	bytes := []byte(strings.Repeat("A", 2048))
	for i := 0; i < 20000; i++ {
		c.Insert(int64(i), int64(i+1), bytes)
		c.Insert(int64(i+1), int64(i+1), bytes)
		c.Insert(int64(i+2), int64(i+1), bytes)
		c.Insert(int64(i+3), int64(i+1), bytes)
	}
	err := c.Dump(c.Table)
	if err != nil {
		panic(err)
	}
}

func TestCake_GetSecondIndexMeta0(t *testing.T) {
	c := NewEngine(".")
	fmt.Println(c)
	file, err := index.OpenFile(".", 0, "1679577439776976852-945379800829250161")
	if err != nil {
		return
	}
	meta, err := index.GetSecondIndexMeta(file.SecondIndex, 0)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(meta, err)
}

func TestCake_GetSecondIndexMeta(t *testing.T) {
	c := NewEngine(".")
	fmt.Println(c)
	file, err := index.OpenFile(".", 0, "1679577439776976852-945379800829250161")
	if err != nil {
		return
	}
	meta, err := index.GetSecondIndexMeta(file.SecondIndex, 1)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(meta, err)
}

func TestCake_GetSecondIndexMeta2(t *testing.T) {
	c := NewEngine(".")
	fmt.Println(c)
	file, err := index.OpenFile(".", 0, "1679577439776976852-945379800829250161")
	if err != nil {
		return
	}
	meta, err := index.GetSecondIndexMeta(file.SecondIndex, 10000)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(meta, err)
}

func TestCake_GetSecondIndexMeta3(t *testing.T) {
	c := NewEngine(".")
	fmt.Println(c)
	file, err := index.OpenFile(".", 0, "1679577439776976852-945379800829250161")
	if err != nil {
		return
	}
	meta, err := index.GetSecondIndexMeta(file.SecondIndex, 10001)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(meta, err)
}

func TestCake_GetFirstSecondMeta(t *testing.T) {
	c := NewEngine(".")
	fmt.Println(c)
	file, err := index.OpenFile(".", 0, "1679577439776976852-945379800829250161")
	if err != nil {
		return
	}
	meta, err := index.GetFirstIndexMeta(file.FirstIndex, 10001)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(meta, err)
}
