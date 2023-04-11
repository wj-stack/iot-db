package util

import (
	"bufio"
	"github.com/pierrec/lz4/v4"
	"io"
)

func Compress(reader io.Reader, writer io.Writer) error {
	// open input file
	// make a read buffer
	r := bufio.NewReader(reader)

	// make an lz4 writer buffer
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

		// writer a chunk
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

	// make a writer buffer
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

		// writer a chunk
		if _, err := w.Write(buf[:n]); err != nil {
			return err
		}
	}

	if err := w.Flush(); err != nil {
		return err
	}
	return nil
}
