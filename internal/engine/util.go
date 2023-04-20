package engine

import (
	"os"
)

type File struct {
	FirstIndexFile  *os.File
	SecondIndexFile *os.File
	DataFile        *os.File
	Name            string
}

func (Fd *File) close() error {
	if Fd.DataFile != nil {
		err := Fd.DataFile.Close()
		if err != nil {
			return err
		}
	}
	if Fd.FirstIndexFile != nil {
		err := Fd.FirstIndexFile.Close()
		if err != nil {
			return err
		}
	}
	if Fd.SecondIndexFile != nil {
		err := Fd.SecondIndexFile.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
