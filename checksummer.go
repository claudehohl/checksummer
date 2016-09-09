package main

import (
	//"crypto/sha256"
	"database/sql"
	//"encoding/hex"
	"flag"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	//"io"
	"errors"
	"os"
	"path/filepath"
)

// channels
var insert = make(chan string)
var clear = make(chan bool)
var commit = make(chan bool)
var commitDone = make(chan bool)

// DB wraps sql.DB
type DB struct {
	*sql.DB
}

// Tx wraps sql.Tx
type Tx struct {
	*sql.Tx
}

// File is the struct for a file holding attributes
type File struct {
	Name string
}

func main() {
	flag.Parse()
	root := flag.Arg(0)

	// initialize database
	db, err := Open("foo.db")
	db.Init()

	// fire up insert worker
	go insertWorker()

	// walk through files
	err = filepath.Walk(root, fileInspector)
	checkErr(err)

	// final commit
	commit <- true

	// exit when commit is done
	<-commitDone
}

// Open returns a DB reference for a data source.
func Open(dataSourceName string) (*DB, error) {
	db, err := sql.Open("sqlite3", dataSourceName)
	if err != nil {
		return nil, err
	}
	return &DB{db}, nil
}

// Begin starts an returns a new transaction.
func (db *DB) Begin() (*Tx, error) {
	tx, err := db.DB.Begin()
	if err != nil {
		return nil, err
	}
	return &Tx{tx}, nil
}

// Init initializes the database
func (db *DB) Init() error {
	_, err := db.Exec(`CREATE TABLE files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT UNIQUE,
            checksum_sha256 TEXT,
            filesize INTEGER,
            mtime INTEGER,
            file_found INTEGER,
            checksum_ok INTEGER
            )`)
	if err != nil {
		return err
	}

	_, err = db.Exec(`CREATE TABLE options (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            o_name TEXT UNIQUE,
            o_value TEXT
            )`)
	if err != nil {
		return err
	}

	return nil
}

// InsertFilename inserts a filename
func (tx *Tx) InsertFilename(f *File) error {
	// Validate the input.
	if f == nil {
		return errors.New("file required")
	} else if f.Name == "" {
		return errors.New("name required")
	}

	// Perform the actual insert and return any errors.
	_, err := tx.Exec(`INSERT INTO files(filename) VALUES(?)`)
	return err
}

func fileInspector(path string, info os.FileInfo, err error) error {
	fmt.Printf("%s\n", path)
	file, err := os.Open(path)
	if err != nil {
		fmt.Printf("File not found: %s", path)
	}
	defer file.Close()

	// spew.Dump(info)

	if info.IsDir() {
		return nil
	}

	// hasher := sha256.New()
	// _, err = io.Copy(hasher, file)
	// checkErr(err)

	// hash := hex.EncodeToString(hasher.Sum(nil))
	// fmt.Printf(" %v\n", hash)

	// wait for clear
	<-clear

	insert <- path

	return nil
}

func insertWorker() {
	c := 0

	// TODO: make tx a type and assign methods to it

	// db, err := Open("foo.db")
	// checkErr(err)

	tx, err := db.Begin()
	checkErr(err)

	clear <- true
	for {
		select {
		case filename := <-insert:
			err := tx.InsertFilename(&File{Name: "autoexec.bat"})
			checkErr(err)
			c++
			fmt.Printf("insert filename: %v\n", filename)
			fmt.Printf("insert counter: %v\n", c)
			fmt.Println("")
			if c%10 == 0 {
				fmt.Println("would commit now.")
				tx.Commit()
				tx, err = db.Begin()
				checkErr(err)
			}

			clear <- true

		case <-commit:
			tx.Commit()
			commitDone <- true
		}
	}

}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
