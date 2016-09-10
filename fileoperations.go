package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// channels
var insert = make(chan File)
var clear = make(chan bool)
var commit = make(chan bool)
var commitDone = make(chan bool)
var exit = make(chan bool)

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

	// spew.Dump(info)

	// wait for clear
	<-clear

	insert <- File{Name: path, Size: info.Size(), Mtime: info.ModTime()}

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
			err := c.InsertFilename(&filename, stmt)
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

// CollectFiles starts insert worker and walks through files
func CollectFiles(db *Conn) {

	// get basepath
	basepath, err := db.GetOption("basepath")
	checkErr(err)

	// fire up insert worker
	go InsertWorker(db)

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

// CheckFilesDB collects stats for all files in database
func CheckFilesDB(db *Conn) {

	// // get basepath
	// basepath, err := db.GetOption("basepath")
	// checkErr(err)

	// // fire up insert worker
	// go InsertWorker(db)

	// walk through files
	ustmt, err := db.Prepare("UPDATE files SET filesize = ?, mtime = ?, file_found = 1 WHERE id = ?")
	checkErr(err)

	db.Begin()
	i := 0
	for stmt, err := db.GetFilenames(); err == nil; err = stmt.Next() {
		var id int
		var filename string
		stmt.Scan(&id, &filename)
		err = ustmt.Exec(33, 34, id)
		checkErr(err)
		i++

		if i%10000 == 0 {
			fmt.Println(i)
			db.Commit()
			err = db.Begin()
			checkErr(err)
		}
	}
	db.Commit()

	// // wait for clear
	// <-clear

	// // final commit
	// commit <- true

	// // wait for commit
	// <-commitDone

	// // terminate InsertWorker
	// exit <- true

	return
}
