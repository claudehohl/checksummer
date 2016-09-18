package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"os"
	"path/filepath"
	"strings"
	"time"
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
                        id integer primary key autoincrement,
                        o_name text unique,
                        o_value text
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
func ChangeBasepath(db *DB) error {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Choose base path")
	fmt.Print("(enter full path, without trailing slash): ")
	basepath, _ := reader.ReadString('\n')
	basepath = strings.Trim(basepath, "\n")
	err := db.SetOption("basepath", basepath)
	return err
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

// CollectFiles starts insert worker and walks through files
func (db *DB) CollectFiles() {

	// get basepath
	basepath, err := db.GetOption("basepath")
	checkErr(err)

	var tx *sql.Tx
	var stmt *sql.Stmt

	tx, err = db.Begin()
	checkErr(err)

	// Precompile SQL statement
	insertStatement := "INSERT INTO files(filename, filesize, mtime, file_found) VALUES(?, ?, ?, 1)"
	stmt, err = tx.Prepare(insertStatement)
	checkErr(err)

	i := 0
	err = filepath.Walk(basepath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Println(err)
			return nil // actually not true, but we just wanna skip the file.
		}

		// skip nonregular files
		if info.Mode().IsRegular() == false {
			return nil
		}

		// populate the file
		file := File{Name: path, Size: info.Size(), Mtime: info.ModTime().Unix()}

		// strip basepath
		file.Name = strings.Replace(file.Name, basepath, "", 1)

		_, err = stmt.Exec(file.Name, file.Size, file.Mtime)
		if err != nil {
			// unique constraint failed, just skip.
		}
		i++

		// commit every 10k files
		if i%10000 == 0 {
			fmt.Println(i)
			err = stmt.Close()
			checkErr(err)
			err = tx.Commit()
			checkErr(err)

			// well. sql closes the connection after Commit(), unlike in python.
			// so we have to reopen it again.

			tx, err = db.Begin()
			checkErr(err)

			// Precompile SQL statement
			stmt, err = tx.Prepare(insertStatement)
			checkErr(err)
		}

		return nil
	})
	checkErr(err)

	// final commit
	err = stmt.Close()
	checkErr(err)
	err = tx.Commit()
	checkErr(err)
}

// RankFilesize returns a list of files, ordered by filesize
func (db *DB) RankFilesize() error {
	var buffer bytes.Buffer
	rows, err := db.Query(`SELECT filename, filesize
                            FROM files
                            WHERE filesize IS NOT NULL
                            ORDER BY filesize DESC`)
	defer rows.Close()
	if err == nil {
		for rows.Next() {
			var filename string
			var filesize int64
			err = rows.Scan(&filename, &filesize)
			if err != nil {
				return err
			}
			buffer.WriteString(fmt.Sprintf("%v\t%v\n", ByteSize(filesize), filename))
		}
		pager(buffer.String(), false)
		return nil
	}
	return err
}

// RankModified returns a list of files, ordered by modified date
func (db *DB) RankModified() error {
	var buffer bytes.Buffer
	rows, err := db.Query(`SELECT filename, filesize, mtime
                            FROM files
                            WHERE file_found = '1'
                            ORDER BY mtime DESC`)
	defer rows.Close()
	if err == nil {
		for rows.Next() {
			var filename string
			var filesize int64
			var date int64
			err = rows.Scan(&filename, &filesize, &date)
			if err != nil {
				return err
			}
			buffer.WriteString(fmt.Sprintf("%v\t%v\t%v\n", time.Unix(date, 0), ByteSize(filesize), filename))
		}
		pager(buffer.String(), false)
		return nil
	}
	return err
}

// ListDuplicates returns a list of duplicate files, ordered by count
func (db *DB) ListDuplicates() error {
	var buffer bytes.Buffer
	rows, err := db.Query(`SELECT filename, COUNT(checksum_sha256) AS count, SUM(filesize) as totalsize
                            FROM files
                            GROUP BY checksum_sha256
                            HAVING (COUNT(checksum_sha256) > 1)
                            ORDER BY totalsize DESC`)
	defer rows.Close()
	if err == nil {
		for rows.Next() {
			var filename string
			var count int64
			var filesize int64
			err = rows.Scan(&filename, &count, &filesize)
			if err != nil {
				return err
			}
			buffer.WriteString(fmt.Sprintf("%v\t%v\t%v\n", count, ByteSize(filesize), filename))
		}
		pager(buffer.String(), false)
		return nil
	}
	return err
}

// ShowDeleted returns a list of deleted files, ordered by filesize
func (db *DB) ShowDeleted() error {
	var buffer bytes.Buffer
	rows, err := db.Query(`SELECT filename, filesize
                            FROM files
                            WHERE file_found = '0'
                            ORDER BY filesize DESC`)
	defer rows.Close()
	if err == nil {
		for rows.Next() {
			var filename string
			var filesize int64
			err = rows.Scan(&filename, &filesize)
			if err != nil {
				return err
			}
			buffer.WriteString(fmt.Sprintf("%v\t%v\n", filesize, filename))
		}
		pager(buffer.String(), false)
		return nil
	}
	return err
}

// ShowChanged returns a list of changed files, ordered by filesize
func (db *DB) ShowChanged() error {
	var buffer bytes.Buffer
	rows, err := db.Query(`SELECT filename, filesize
                            FROM files
                            WHERE checksum_ok = '0'
                            ORDER BY filesize DESC`)
	defer rows.Close()
	if err == nil {
		for rows.Next() {
			var filename string
			var filesize int64
			err = rows.Scan(&filename, &filesize)
			if err != nil {
				return err
			}
			buffer.WriteString(fmt.Sprintf("%v\t%v\n", filesize, filename))
		}
		pager(buffer.String(), false)
		return nil
	}
	return err
}

// PruneDeleted removes deleted files from db
func (db *DB) PruneDeleted() error {
	_, err := db.Exec("DELETE FROM files WHERE file_found = '0'")
	return err
}

// PruneChanged sets the checksum to NULL for changed files
func (db *DB) PruneChanged() error {
	_, err := db.Exec(`UPDATE files
                        SET checksum_sha256 = NULL,
                        checksum_ok = NULL,
                        filesize = NULL
                        WHERE checksum_ok = 0`)
	return err
}

// ReindexCheck [todo]
func (db *DB) ReindexCheck() error {
	return nil
}
