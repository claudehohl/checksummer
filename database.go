package main

import (
	"bufio"
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"os"
	"strings"
)

// DB wraps sql.DB
type DB struct {
	*sql.DB
}

// Open returns a DB reference for a data source.
func Open(dataSourceName string) (*DB, error) {
	db, err := sql.Open("sqlite3", dataSourceName)
	if err != nil {
		return nil, err
	}
	return &DB{db}, nil
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

	// tuning
	_, err = db.Exec("PRAGMA synchronous=OFF")
	checkErr(err)
	_, err = db.Exec("PRAGMA journal_size_limit=-1")
	checkErr(err)

	return nil
}

// ChangeBasepath sets the basepath
func ChangeBasepath(db *DB) {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Choose base path")
	fmt.Print("(enter full path, without trailing slash): ")
	basepath, _ := reader.ReadString('\n')
	basepath = strings.Trim(basepath, "\n")
	err := db.SetOption("basepath", basepath)
	checkErr(err)
}

// GetOption gets an option from db
func (db *DB) GetOption(key string) (val string, err error) {
	rows, err := db.Query("SELECT o_value FROM options WHERE o_name = ?", key)
	defer rows.Close()
	if err == nil {
		rows.Next()
		var oValue string
		err = rows.Scan(&oValue)
		if err != nil {
			return "", err
		}
		return oValue, nil
	}
	return "", err
}

// SetOption sets an option value
func (db *DB) SetOption(key string, value string) error {
	_, err := db.Exec("INSERT INTO options(o_name, o_value) VALUES(?, ?)", key, value)
	if err != nil {
		_, err = db.Exec("UPDATE options SET o_value = ? WHERE o_name = ?", value, key)
		checkErr(err)
	}
	return err
}

// GetCount returns the number of files
func (db *DB) GetCount(statement string) (val int, err error) {
	rows, err := db.Query(statement)
	defer rows.Close()
	if err == nil {
		rows.Next()
		var val int
		err = rows.Scan(&val)
		if err != nil {
			return -1, err
		}
		return val, nil
	}
	return -1, err
}

// RankFilesize returns a list of files, ordered by filesize
func (db *DB) RankFilesize() (files []File, err error) {
	rows, err := db.Query("SELECT filename, filesize FROM files WHERE filesize IS NOT NULL ORDER BY filesize DESC")
	defer rows.Close()
	if err == nil {
		rows.Next()
		var filename string
		var filesize int64
		err = rows.Scan(&filename, &filesize)
		if err != nil {
			return []File{}, err
		}
		files = append(files, File{Name: filename, Size: filesize})
		fmt.Println(files)
		return files, nil
	}
	return []File{}, err
}
