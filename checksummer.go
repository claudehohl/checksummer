package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// channels
var insert = make(chan string)
var clear = make(chan bool)
var commit = make(chan bool)
var commitDone = make(chan bool)

// File is the struct for a file holding attributes
type File struct {
	Name string
}

func main() {
	flag.Parse()
	root := flag.Arg(0)
	if root == "" {
		panic("please provide rootpath!")
	}

	// initialize database
	db, err := Open("foo.db")
	db.Init()

	// fire up insert worker
	go InsertWorker(db)

	// walk through files
	err = filepath.Walk(root, FileInspector)
	checkErr(err)

	// wait for clear
	<-clear

	// final commit
	commit <- true

	// exit when commit is done
	<-commitDone
}

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
		fmt.Printf("File not found: %s", path)
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
			checkErr(err)
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
		}
	}
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
