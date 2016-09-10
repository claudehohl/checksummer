package main

import (
	"errors"
	"github.com/mxk/go-sqlite/sqlite3"
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

// PrepareInsert precompiles insert statement
func (c *Conn) PrepareInsert() (*sqlite3.Stmt, error) {
	stmt, err := c.Prepare(`INSERT INTO files(filename) VALUES(?)`)
	return stmt, err
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
	err := stmt.Exec(f.Name)
	return err
}
