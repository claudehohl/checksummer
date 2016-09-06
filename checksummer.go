package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
)

func visit(path string, f os.FileInfo, err error) error {
	fmt.Printf("Visited: %s\n", path)
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	if f.IsDir() {
		return nil
	}
	hasher := sha256.New()
	_, err = io.Copy(hasher, file)
	if err != nil {
		log.Fatal("this", err)
	}

	os.Stdout.WriteString(hex.EncodeToString(hasher.Sum(nil)))

	return nil
}

func main() {
	flag.Parse()
	root := flag.Arg(0)
	err := filepath.Walk(root, visit)
	fmt.Printf("filepath.Walk() returned %v\n", err)
}

func sqlite() {
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
