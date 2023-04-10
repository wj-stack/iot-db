package datastructure

import (
	"bufio"
	"github.com/pierrec/lz4/v4"
	"github.com/sirupsen/logrus"
	"io"
	"strings"
	"testing"
)

func Test_lz(t *testing.T) {

	s := "hello world"
	r := strings.NewReader(s)

	// The pipe will uncompress the data from the writer.
	pr, pw := io.Pipe()
	zw := lz4.NewWriter(pw)
	zr := bufio.NewReader(pr)
	go func() {
		// Compress the input string.
		_, _ = io.Copy(zw, r)
		_ = zw.Close() // Make sure the writer is closed
		_ = pw.Close() // Terminate the pipe
		logrus.Infoln("Terminate the pipe")
	}()

	all, err := io.ReadAll(zr)
	if err != nil {
		return
	}
	logrus.Info(all)

	//repeat := strings.Repeat("A", 1000)
	//v, err := lz([]byte(repeat))
	//logrus.Infoln(v, err)

}
