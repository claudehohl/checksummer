package main

import (
	//"crypto/sha256"
	"database/sql"
	//"encoding/hex"
	"flag"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	_ "github.com/mattn/go-sqlite3"
	//"io"
	"log"
	"os"
	"path/filepath"
)

var insert = make(chan string)
var commit = make(chan bool)
var commitDone = make(chan bool)

var db *sql.DB
var err error

func getDB() *sql.DB {
	if db == nil {
		db, err = sql.Open("sqlite3", "foo.db")
		checkErr(err)
	}

	return db
}

func walkFn(path string, info os.FileInfo, err error) error {
	fmt.Printf("%s", path)
	file, err := os.Open(path)
	checkErr(err)
	defer file.Close()

	spew.Dump(info)

	if info.IsDir() {
		return nil
	}

	// hasher := sha256.New()
	// _, err = io.Copy(hasher, file)
	// if err != nil {
	// 	log.Fatal("this", err)
	// }

	// hash := hex.EncodeToString(hasher.Sum(nil))
	// fmt.Printf(" %v\n", hash)

	insert <- path
	//insert(path)

	return nil
}

func insertWorker() {
	db := getDB()

	//TODO: maintain insert-counter, commit every 1000 files

	tx, err := db.Begin()
	stmt, err := tx.Prepare("INSERT INTO files(filename) VALUES(?)")
	defer stmt.Close()
	for {
		select {
		case filename := <-insert:
			_, err = stmt.Exec(filename)
			if err != nil {
				log.Fatal(err)
			}
		case <-commit:
			tx.Commit()
			commitDone <- true
		}
	}

}

func main() {
	flag.Parse()
	root := flag.Arg(0)
	initDB()
	go insertWorker()
	err := filepath.Walk(root, walkFn)
	fmt.Printf("filepath.Walk() returned %v\n", err)
	commit <- true
	<-commitDone
}

func sqlite() {
	os.Remove("foo.db")
	db := getDB()

	rows, err := db.Query("select id, filename from files")
	checkErr(err)

	defer rows.Close()
	for rows.Next() {
		var id int
		var filename string
		err = rows.Scan(&id, &filename)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(id, filename)
	}

	err = rows.Err()
	checkErr(err)

}

func initDB() {
	os.Remove("foo.db")
	db := getDB()

	_, err = db.Exec(`CREATE TABLE files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT UNIQUE,
            checksum_sha256 TEXT,
            filesize INTEGER,
            mtime INTEGER,
            file_found INTEGER,
            checksum_ok INTEGER
            )`)
	checkErr(err)

	_, err = db.Exec(`CREATE TABLE options (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            o_name TEXT UNIQUE,
            o_value TEXT
            )`)
	checkErr(err)

}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
