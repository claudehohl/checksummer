package main

import (
	//"crypto/sha256"
	"database/sql"
	//"encoding/hex"
	"flag"
	"fmt"
	"github.com/mxk/go-sqlite/sqlite3"
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

// Conn wraps sql.DB
type Conn struct {
	*sqlite3.Conn
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
	if root == "" {
		panic("please provide rootpath!")
	}

	// initialize database
	db, err := Open("foo.db")
	db.Init()

	// fire up insert worker
	go insertWorker(db)

	// walk through files
	err = filepath.Walk(root, fileInspector)
	checkErr(err)

	// wait for clear
	<-clear

	// final commit
	commit <- true

	// exit when commit is done
	<-commitDone
}

// Open returns a DB reference for a data source.
func Open(dataSourceName string) (*Conn, error) {
	c, err := sqlite3.Open(dataSourceName)
	if err != nil {
		return nil, err
	}
	return &Conn{c}, nil
}

// Begin starts a new transaction.
func (c *Conn) Begin() error {
	err := c.Conn.Begin()
	if err != nil {
		return err
	}
	return nil
}

// Init initializes the database
func (c *Conn) Init() error {
	err := c.Exec(`CREATE TABLE files (
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

	err = c.Exec(`CREATE TABLE options (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            o_name TEXT UNIQUE,
            o_value TEXT
            )`)
	if err != nil {
		return err
	}

	// tuning
	err = c.Exec("PRAGMA synchronous=OFF")
	err = c.Exec("PRAGMA journal_size_limit=-1")

	return nil
}

// InsertFilename inserts a filename
func (c *Conn) InsertFilename(f *File, stmt *sqlite3.Stmt) error {
	// Validate the input.
	if f == nil {
		return errors.New("file required")
	} else if f.Name == "" {
		return errors.New("name required")
	}

	// Perform the actual insert and return any errors.
	// err := c.Exec(`INSERT INTO files(filename) VALUES(?)`, f.Name)
	err := stmt.Exec(f.Name)
	return err

	// time.Sleep(1000 * time.Nanosecond)
	// return nil
}

// PrepareInsert inserts a filename
func (c *Conn) PrepareInsert() (*sqlite3.Stmt, error) {
	stmt, err := c.Prepare(`INSERT INTO files(filename) VALUES(?)`)
	return stmt, err
}

func fileInspector(path string, info os.FileInfo, err error) error {
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

func insertWorker(c *Conn) {
	i := 0

	// TODO: make tx a type and assign methods to it

	// db, err := Open("foo.db")
	// checkErr(err)

	err := c.Begin()
	checkErr(err)

	stmt, err := c.PrepareInsert()

	clear <- true
	for {
		select {
		case filename := <-insert:
			err := c.InsertFilename(&File{Name: filename}, stmt)
			checkErr(err)
			// fmt.Printf("insert filename: %v\n", filename)
			// fmt.Printf("insert counter: %v\n", i)
			// fmt.Println("")
			i++
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
