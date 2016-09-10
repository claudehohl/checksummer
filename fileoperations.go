package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
)

// FileInspector is the WalkFn, passes path into the insert channel
func FileInspector(path string, info os.FileInfo, err error) error {
	file, err := os.Open(path)
	if err != nil {
		fmt.Printf("File not found: %s", path)
	}
	defer file.Close()

	// spew.Dump(info)

	if info.IsDir() {
		return nil
	}

	// wait for clear
	<-clear

	insert <- path

	return nil
}

// HashFile takes a path and returns a hash
func HashFile(path string) (hash string, err error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
		// fmt.Printf("File not found: %s", path)
	}
	defer file.Close()

	// spew.Dump(info)

	hasher := sha256.New()
	_, err = io.Copy(hasher, file)
	if err != nil {
		return "", err
	}

	hash = hex.EncodeToString(hasher.Sum(nil))
	fmt.Printf(" %v\n", hash)

	return hash, nil
}

// InsertWorker runs the statement, commits every 10k inserts
func InsertWorker(c *Conn) {
	err := c.Begin()
	checkErr(err)

	// Precompile SQL statement
	stmt, err := c.PrepareInsert()

	clear <- true

	i := 0
	for {
		select {
		case filename := <-insert:
			err := c.InsertFilename(&File{Name: filename}, stmt)
			if err != nil {
				// unique constraint failed, just skip.
			}
			i++

			// commit every 10k inserts
			if i%10000 == 0 {
				fmt.Println(i)
				c.Commit()
				err = c.Begin()
				checkErr(err)
			}

			clear <- true

		case <-commit:
			c.Commit()
			commitDone <- true

		case <-exit:
			return
		}
	}
}
