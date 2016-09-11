package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
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
func CollectFiles(db *DB) {

	// get basepath
	basepath, err := db.GetOption("basepath")
	checkErr(err)

	// fire up insert worker
	go InsertWorker(db)

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
func InsertWorker(db *DB) {

	// get basepath
	basepath, err := db.GetOption("basepath")
	checkErr(err)

	tx, err := db.Begin()
	checkErr(err)

	// Precompile SQL statement
	stmt, err := tx.Prepare("INSERT INTO files(filename, filesize, mtime) VALUES(?, ?, ?)")

	clear <- true

	i := 0
	for {
		select {
		case file := <-insert:
			// strip basepath
			file.Name = strings.Replace(file.Name, basepath, "", 1)

			_, err := stmt.Exec(file.Name, file.Size, file.Mtime)
			if err != nil {
				// unique constraint failed, just skip.
			}
			i++

			// commit every 10k inserts
			if i%10000 == 0 {
				fmt.Println(i)
				tx.Commit()
				tx, err = db.Begin()
				checkErr(err)
			}

			clear <- true

		case <-commit:
			tx.Commit()
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
func CheckFilesDB(db *DB) {

	// get basepath
	basepath, err := db.GetOption("basepath")
	checkErr(err)

	fileCount, err := db.GetCount("SELECT count(id) FROM files")
	checkErr(err)

	// sqlite dies with "unable to open database [14]" when I run two stmts concurrently
	// therefore, we process by fetching blocks of 10000 files
	for i := 0; i < fileCount; i = i + 10000 {
		if i >= 10000 {
			fmt.Println(i)
		}

		var files []File
		var rows *sql.Rows

		for rows, err = db.Query("SELECT id, filename FROM files LIMIT ?, 10000", i); err == nil; rows.Next() {
			var id int64
			var filename string
			rows.Scan(&id, &filename)
			files = append(files, File{ID: id, Name: filename})
		}
		rows.Close()

		tx, err := db.Begin()

		// prepare update statement
		stmt, err := tx.Prepare("UPDATE files SET filesize = ?, mtime = ?, file_found = ? WHERE id = ?")
		checkErr(err)

		for _, file := range files {
			path := basepath + file.Name

			f, err := os.Open(path)
			if err != nil {
				// file not found
				_, err = stmt.Exec(nil, nil, 0, file.ID)
				checkErr(err)
			} else {
				err = GetStats(f, &file)
				checkErr(err)
				_, err = stmt.Exec(file.Size, file.Mtime, 1, file.ID)
				checkErr(err)
			}
			f.Close()
		}

		stmt.Close()
		tx.Commit()
	}

	return
}

// GetStats populates File with os.FileInfo stats
func GetStats(f *os.File, file *File) error {
	fi, err := f.Stat()
	file.Size = fi.Size()
	file.Mtime = fi.ModTime()
	return err
}

// MakeChecksums makes checksums of all files
func MakeChecksums(db *DB) {

	// get basepath
	basepath, err := db.GetOption("basepath")
	checkErr(err)

	fileCount, err := db.GetCount("SELECT count(id) FROM files WHERE checksum_sha256 IS NULL AND file_found = '1'")
	checkErr(err)

	// sqlite dies with "unable to open database [14]" when I run two stmts concurrently
	// therefore, we process by fetching blocks of 1000 files
	blockSize := 100
	for i := fileCount; i > 0; i = i - blockSize {
		rowmap := make(sqlite3.RowMap)
		var rows []File
		var stmt *sqlite3.Stmt
		offset := fileCount - i + blockSize
		if offset > fileCount {
			offset = fileCount
		}
		fmt.Println("offset:", offset)
		remaining := i

		for stmt, err = db.Query("SELECT id, filename, filesize FROM files WHERE checksum_sha256 IS NULL AND file_found = '1' LIMIT ?, ?", offset, 100); err == nil; err = stmt.Next() {
			stmt.Scan(rowmap)
			rows = append(rows, File{ID: rowmap["id"].(int64), Name: rowmap["filename"].(string), Size: rowmap["filesize"].(int64)})
		}
		stmt.Close()

		// prepare update statement
		stmtUpdate, err := db.Prepare("UPDATE files SET checksum_sha256 = ? WHERE id = ?")
		checkErr(err)
		stmtNotFound, err := db.Prepare("UPDATE files SET file_found = 0 WHERE id = ?")
		checkErr(err)

		db.Begin()
		for _, file := range rows {
			path := basepath + file.Name

			fmt.Printf("(%d) making checksum: %s (%d)... ", remaining, path, file.Size)

			f, err := os.Open(path)
			if err != nil {
				// file not found
				err = stmtNotFound.Exec(file.ID)
				checkErr(err)
			} else {
				hash, err := HashFile(path)
				checkErr(err)
				err = stmtUpdate.Exec(hash, file.ID)
				checkErr(err)
			}
			f.Close()

			fmt.Println("OK")
			remaining--
		}

		stmtUpdate.Close()
		stmtNotFound.Close()
		db.Commit()
	}

	return
}

// HashFile takes a path and returns a hash
func HashFile(path string) (hash string, err error) {

	file, err := os.Open(path)
	if err != nil {
		fmt.Printf("File not found: %s", path)
		return "", err
	}
	defer file.Close()

	hasher := sha256.New()
	_, err = io.Copy(hasher, file)
	if err != nil {
		return "", err
	}

	hash = hex.EncodeToString(hasher.Sum(nil))

	return hash, nil
}
