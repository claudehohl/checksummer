package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	_ "github.com/mattn/go-sqlite3"
	"io"
	"log"
	"os"
	"path/filepath"
)

func walkFn(path string, info os.FileInfo, err error) error {
	fmt.Printf("%s", path)
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	spew.Dump(info)

	if info.IsDir() {
		return nil
	}
	hasher := sha256.New()
	_, err = io.Copy(hasher, file)
	if err != nil {
		log.Fatal("this", err)
	}

	fmt.Printf(" %v\n", hex.EncodeToString(hasher.Sum(nil)))

	return nil
}

func main() {
	flag.Parse()
	root := flag.Arg(0)
	initDB()
	err := filepath.Walk(root, walkFn)
	fmt.Printf("filepath.Walk() returned %v\n", err)
}

func sqlite() {
	os.Remove("foo.db")
	db, err := sql.Open("sqlite3", "foo.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query("select id, filename from files")
	if err != nil {
		log.Fatal(err)
	}

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
	if err != nil {
		log.Fatal(err)
	}

}

func initDB() {
	os.Remove("foo.db")
	db, err := sql.Open("sqlite3", "foo.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT UNIQUE,
            checksum_sha256 TEXT,
            filesize INTEGER,
            mtime INTEGER,
            file_found INTEGER,
            checksum_ok INTEGER
            )`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`CREATE TABLE options (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            o_name TEXT UNIQUE,
            o_value TEXT
            )`)
	if err != nil {
		log.Fatal(err)
	}

}
