package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/mxk/go-sqlite/sqlite3"
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
	err = filepath.Walk(basepath, InsertFileInspector)
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

// InsertFileInspector is the WalkFn, passes path into the insert channel
func InsertFileInspector(path string, info os.FileInfo, err error) error {

	// skip nonregular files
	if info.Mode().IsRegular() == false {
		return nil
	}

	// wait for clear
	<-clear

	// pass fileinfo to the insert channel
	insert <- File{Name: path, Size: info.Size(), Mtime: info.ModTime()}

	return nil
}

// CheckFilesDB collects stats for all files in database
func CheckFilesDB(c *Conn) {

	// get basepath
	basepath, err := c.GetOption("basepath")
	checkErr(err)

	fileCount, err := c.GetCount()
	checkErr(err)

	// sqlite dies with "unable to open database [14]" when I run two stmts concurrently
	// therefore, we process by fetching blocks of 10000 files
	for i := 0; i < fileCount; i = i + 10000 {
		if i >= 10000 {
			fmt.Println(i)
		}

		rowmap := make(sqlite3.RowMap)
		var rows []File
		var stmt *sqlite3.Stmt

		for stmt, err = c.Query("SELECT id, filename FROM files LIMIT ?, 10000", i); err == nil; err = stmt.Next() {
			stmt.Scan(rowmap)
			rows = append(rows, File{ID: rowmap["id"].(int64), Name: rowmap["filename"].(string)})
		}
		stmt.Close()

		// prepare update statement
		stmt, err := c.Prepare("UPDATE files SET filesize = ?, mtime = ?, file_found = ? WHERE id = ?")
		checkErr(err)

		c.Begin()
		for _, file := range rows {
			path := basepath + file.Name

			f, err := os.Open(path)
			if err != nil {
				// file not found
				err = stmt.Exec(nil, nil, 0, file.ID)
				checkErr(err)
			} else {
				GetStats(f, &file)
				stmt.Exec(file.Size, file.Mtime, 1, file.ID)
			}
			f.Close()
		}

		stmt.Close()
		c.Commit()
	}

	return
}

// GetStats populates File with os.FileInfo stats
func GetStats(f *os.File, file *File) {
	fi, err := f.Stat()
	checkErr(err)
	file.Size = fi.Size()
	file.Mtime = fi.ModTime()
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
