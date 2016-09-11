package main

import (
	"bufio"
	"fmt"
	"github.com/mxk/go-sqlite/sqlite3"
	"os"
	"strings"
)

// Conn wraps sqlite3.Conn
type Conn struct {
	*sqlite3.Conn
}

// Open returns a DB reference for a data source.
func Open(dataSourceName string) (*Conn, error) {
	c, err := sqlite3.Open(dataSourceName)
	if err != nil {
		return nil, err
	}
	return &Conn{c}, nil
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
	checkErr(err)
	err = c.Exec("PRAGMA journal_size_limit=-1")
	checkErr(err)

	return nil
}

// ChangeBasepath sets the basepath
func ChangeBasepath(db *Conn) {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Choose base path")
	fmt.Print("(enter full path, without trailing slash): ")
	basepath, _ := reader.ReadString('\n')
	basepath = strings.Trim(basepath, "\n")
	err := db.SetOption("basepath", basepath)
	checkErr(err)
}

// GetOption gets an option from db
func (c *Conn) GetOption(key string) (val string, err error) {
	stmt, err := c.Query("SELECT o_value FROM options WHERE o_name = ?", key)
	if err == nil {
		var oValue string
		err = stmt.Scan(&oValue)
		if err != nil {
			return "", err
		}
		return oValue, nil
	}
	return "", err
}

// SetOption sets an option value
func (c *Conn) SetOption(key string, value string) error {
	err := c.Exec("INSERT INTO options(o_name, o_value) VALUES(?, ?)", key, value)
	if err != nil {
		err = c.Exec("UPDATE options SET o_value = ? WHERE o_name = ?", value, key)
		checkErr(err)
	}
	return err
}

// GetCount returns the number of files
func (c *Conn) GetCount() (val int, err error) {
	stmt, err := c.Query("SELECT count(id) FROM files")
	if err == nil {
		var val int
		err = stmt.Scan(&val)
		if err != nil {
			return -1, err
		}
		return val, nil
	}
	return -1, err
}
