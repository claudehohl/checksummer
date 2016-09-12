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

// ByteSize displays bytes in human-readable format
type ByteSize float64

func (b ByteSize) String() string {
	const (
		_           = iota // ignore first value by assigning to blank identifier
		KB ByteSize = 1 << (10 * iota)
		MB
		GB
		TB
		PB
		EB
		ZB
		YB
	)

	switch {
	case b >= YB:
		return fmt.Sprintf("%.2fYB", b/YB)
	case b >= ZB:
		return fmt.Sprintf("%.2fZB", b/ZB)
	case b >= EB:
		return fmt.Sprintf("%.2fEB", b/EB)
	case b >= PB:
		return fmt.Sprintf("%.2fPB", b/PB)
	case b >= TB:
		return fmt.Sprintf("%.2fTB", b/TB)
	case b >= GB:
		return fmt.Sprintf("%.2fGB", b/GB)
	case b >= MB:
		return fmt.Sprintf("%.2fMB", b/MB)
	case b >= KB:
		return fmt.Sprintf("%.2fKB", b/KB)
	}
	return fmt.Sprintf("%.2fB", b)
}

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

	var tx *sql.Tx
	var stmt *sql.Stmt

	tx, err = db.Begin()
	checkErr(err)

	// Precompile SQL statement
	stmt, err = tx.Prepare("INSERT INTO files(filename, filesize, mtime, file_found) VALUES(?, ?, ?, 1)")
	checkErr(err)

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
				stmt.Close()
				tx.Commit()

				// well. sql closes the connection after Commit(), unlike in python.
				// so we have to reopen it again.
				// what a special hack'n'bang, all because WalkFn doesn't accept parameters... -.-

				tx, err = db.Begin()
				checkErr(err)

				// Precompile SQL statement
				stmt, err = tx.Prepare("INSERT INTO files(filename, filesize, mtime, file_found) VALUES(?, ?, ?, 1)")
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

	if err != nil {
		fmt.Println(err)
		return nil // actually not true, but we just wanna skip the file.
	}

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

		rows, err := db.Query("SELECT id, filename FROM files LIMIT ?, 10000", i)
		checkErr(err)

		for rows.Next() {
			var id int64
			var filename string
			rows.Scan(&id, &filename)
			files = append(files, File{ID: id, Name: filename})
		}
		rows.Close()

		tx, err := db.Begin()
		checkErr(err)

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
	ts, err := db.GetCount("SELECT sum(filesize) FROM files WHERE checksum_sha256 IS NULL AND file_found = '1'")
	checkErr(err)
	var totalSize int64
	totalSize = int64(ts)

	// dynamically calculate blocksize.
	// lots of small files = large blocksize (10000)
	// huge, few files = small blocksize (100)
	// this is relevant because the commit happens after each block
	fileSizePerCount := float32(totalSize) / float32(fileCount)
	bs := 10000.0 / fileSizePerCount * 50000
	blockSize := int(bs)

	// sqlite dies with "unable to open database [14]" when I run two stmts concurrently
	// therefore, we process by fetching blocks of files
	for i := fileCount + blockSize; i > 0; i = i - blockSize {
		var (
			tx           *sql.Tx
			stmtUpdate   *sql.Stmt
			stmtNotFound *sql.Stmt
			files        []File
			rows         *sql.Rows
		)
		remaining := i

		rows, err = db.Query("SELECT id, filename, filesize FROM files WHERE checksum_sha256 IS NULL AND file_found = '1' LIMIT ?", blockSize)
		checkErr(err)

		for rows.Next() {
			var id int64
			var filename string
			var filesize int64
			rows.Scan(&id, &filename, &filesize)
			files = append(files, File{ID: id, Name: filename, Size: filesize})
		}
		rows.Close()

		tx, err = db.Begin()
		checkErr(err)

		// prepare update statement
		stmtUpdate, err = tx.Prepare("UPDATE files SET checksum_sha256 = ? WHERE id = ?")
		checkErr(err)
		stmtNotFound, err = tx.Prepare("UPDATE files SET file_found = 0 WHERE id = ?")
		checkErr(err)

		for _, file := range files {
			path := basepath + file.Name

			fmt.Printf("(%d, %s) making checksum: %s (%s)... ", remaining, ByteSize(totalSize), path, ByteSize(file.Size))

			f, err := os.Open(path)
			if err != nil {
				// file not found
				_, err = stmtNotFound.Exec(file.ID)
				checkErr(err)
			} else {
				hash, err := HashFile(path)
				checkErr(err)
				_, err = stmtUpdate.Exec(hash, file.ID)
				checkErr(err)
			}
			f.Close()

			fmt.Println("OK")
			remaining--
			totalSize = totalSize - file.Size
		}

		stmtUpdate.Close()
		stmtNotFound.Close()
		tx.Commit()

		tx, err = db.Begin()
		checkErr(err)

		// prepare update statement
		stmtUpdate, err = tx.Prepare("UPDATE files SET checksum_sha256 = ? WHERE id = ?")
		checkErr(err)
		stmtNotFound, err = tx.Prepare("UPDATE files SET file_found = 0 WHERE id = ?")
		checkErr(err)
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
