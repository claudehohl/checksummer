package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// channels
var insert = make(chan File)
var clear = make(chan bool)
var commit = make(chan bool)
var commitDone = make(chan bool)
var exit = make(chan bool)

// CollectFiles starts insert worker and walks through files
func CollectFiles(c *Conn) {

	// get basepath
	basepath, err := c.GetOption("basepath")
	checkErr(err)

	// fire up insert worker
	go InsertWorker(c)

	// walk through files
	err = filepath.Walk(basepath, FileInspector)
	checkErr(err)

	// wait for clear
	<-clear

	// final commit
	commit <- true

	// wait for commit
	<-commitDone

	// terminate InsertWorker
	exit <- true
}

// InsertWorker runs the statement, commits every 10k inserts
func InsertWorker(c *Conn) {

	// get basepath
	basepath, err := c.GetOption("basepath")
	checkErr(err)

	err = c.Begin()
	checkErr(err)

	// Precompile SQL statement
	stmt, err := c.Prepare("INSERT INTO files(filename, filesize, mtime) VALUES(?, ?, ?)")

	clear <- true

	i := 0
	for {
		select {
		case file := <-insert:
			// strip basepath
			file.Name = strings.Replace(file.Name, basepath, "", 1)

			err := stmt.Exec(file.Name, file.Size, file.Mtime)
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

// FileInspector is the WalkFn, passes path into the insert channel
func FileInspector(path string, info os.FileInfo, err error) error {

	// skip nonregular files
	if info.Mode().IsRegular() == false {
		return nil
	}

	file, err := os.Open(path)
	if err != nil {
		fmt.Printf("File not found: %s", path)
	}
	defer file.Close()

	// wait for clear
	<-clear

	insert <- File{Name: path, Size: info.Size(), Mtime: info.ModTime()}

	return nil
}

// CheckFilesDB collects stats for all files in database
func CheckFilesDB(c *Conn) {

	// get basepath
	basepath, err := c.GetOption("basepath")
	checkErr(err)

	// walk through files
	ustmt, err := c.Prepare("UPDATE files SET filesize = ?, mtime = ?, file_found = 1 WHERE id = ?")
	checkErr(err)

	c.Begin()
	i := 0
	for stmt, err := c.Query("SELECT id, filename FROM files"); err == nil; err = stmt.Next() {
		var id int
		var filename string
		stmt.Scan(&id, &filename)

		// TODO
		path := basepath + filename
		fmt.Println(path)

		err = ustmt.Exec(33, 34, id)
		checkErr(err)
		i++

		if i%10000 == 0 {
			fmt.Println(i)
			c.Commit()
			err = c.Begin()
			checkErr(err)
		}
	}
	c.Commit()

	return
}

// HashFile takes a path and returns a hash
func HashFile(path string) (hash string, err error) {

	file, err := os.Open(path)
	if err != nil {
		return "", err
		// fmt.Printf("File not found: %s", path)
	}
	defer file.Close()

	hasher := sha256.New()
	_, err = io.Copy(hasher, file)
	if err != nil {
		return "", err
	}

	hash = hex.EncodeToString(hasher.Sum(nil))
	fmt.Printf(" %v\n", hash)

	return hash, nil
}
