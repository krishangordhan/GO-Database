package db

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"os"
)

const (
	fileMode = 0664
)

func SaveData(path string, data []byte) error {
	randomNum, err := rand.Int(rand.Reader, big.NewInt(0))
	if err != nil {
		return err
	}
	tmp := fmt.Sprintf("%s.tmp.%d", path, randomNum)
	fp, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, fileMode)
	if err != nil {
		return err
	}

	defer func() {
		fp.Close()
		if err != nil {
			os.Remove(tmp)
		}
	}()

	if _, err = fp.Write(data); err != nil {
		return err
	}
	if err = fp.Sync(); err != nil {
		return err
	}
	err = os.Rename(tmp, path)
	return err
}
