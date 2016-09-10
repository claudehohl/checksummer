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
	stmt, err := c.Prepare(`INSERT INTO files(filename, filesize, mtime) VALUES(?, ?, ?)`)
	return stmt, err
}

// InsertFilename inserts a filename
func (c *Conn) InsertFilename(f *File, stmt *sqlite3.Stmt) error {
	// Validate the input.
	if f == nil {
		return errors.New("file required")
	} else if f.Name == "" {
		return errors.New("name required")
	} else if f.Size == -1 {
		return errors.New("size required")
	}

	// Perform the actual insert and return any errors.
	err := stmt.Exec(f.Name, f.Size, f.Mtime)
	return err
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

// GetFilenames fetches all filenames
func (c *Conn) GetFilenames() (stmt *sqlite3.Stmt, err error) {

	stmt, err = c.Query("SELECT id, filename FROM files")
	return stmt, err

	// for stmt, err := c.Query("SELECT id, filename FROM files"); err == nil; err = stmt.Next() {
	// 	var id int
	// 	var filename string
	// 	stmt.Scan(&id, &filename)
	// 	// fmt.Println(filename)
	// 	filenames = append(filenames, filename)
	// }
	// return filenames, nil

	// return []string{}, nil
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
