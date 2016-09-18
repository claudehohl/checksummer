package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"io"
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
func (db *DB) ChangeBasepath() error {
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

	fmt.Println("Collecting files")

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

// CheckFilesDB collects stats for all files in database
func (db *DB) CheckFilesDB() {

	fmt.Println("Checking files in DB")

	// get basepath
	basepath, err := db.GetOption("basepath")
	checkErr(err)

	fileCount, err := db.GetCount("SELECT count(id) FROM files")
	checkErr(err)

	// sqlite dies with "unable to open database [14]" when I run two stmts concurrently
	// therefore, we process by fetching blocks of 10000 files
	for i := 0; i < fileCount; i = i + 10000 {
		if i >= 10000 {
			fmt.Println(i)
		}

		var files []File

		rows, err := db.Query("SELECT id, filename FROM files LIMIT ?, 10000", i)
		defer rows.Close()
		checkErr(err)

		for rows.Next() {
			var id int64
			var filename string
			rows.Scan(&id, &filename)
			files = append(files, File{ID: id, Name: filename})
		}
		rows.Close()

		tx, err := db.Begin()
		checkErr(err)

		// prepare update statement
		stmt, err := tx.Prepare("UPDATE files SET filesize = ?, mtime = ?, file_found = ? WHERE id = ?")
		checkErr(err)

		for _, file := range files {
			path := basepath + file.Name

			f, err := os.Open(path)
			if err != nil {
				// file not found
				_, err = stmt.Exec(nil, nil, 0, file.ID)
				checkErr(err)
			} else {
				fi, err := f.Stat()
				file.Size = fi.Size()
				file.Mtime = fi.ModTime().Unix()
				checkErr(err)
				_, err = stmt.Exec(file.Size, file.Mtime, 1, file.ID)
				checkErr(err)
			}
			f.Close()
		}

		stmt.Close()
		tx.Commit()
	}

	return
}

// MakeChecksums makes checksums of all files
func (db *DB) MakeChecksums() {

	fmt.Println("Making checksums")

	// get basepath
	basepath, err := db.GetOption("basepath")
	checkErr(err)

	updateStatement := "UPDATE files SET checksum_sha256 = ? WHERE id = ?"
	notFoundStatement := "UPDATE files SET file_found = 0 WHERE id = ?"

	fileCount, err := db.GetCount("SELECT count(id) FROM files WHERE checksum_sha256 IS NULL AND file_found = '1'")
	checkErr(err)
	ts, err := db.GetCount("SELECT sum(filesize) FROM files WHERE checksum_sha256 IS NULL AND file_found = '1'")
	if err != nil {
		ts = 0
	}
	var totalSize int64
	totalSize = int64(ts)

	// dynamically calculate blocksize.
	// lots of small files = large blocksize (10000)
	// huge, few files = small blocksize (100)
	// this is relevant because the commit happens after each block
	fileSizePerCount := float32(totalSize) / float32(fileCount)
	bs := 10000.0 / fileSizePerCount * 50000
	blockSize := int(bs)

	// sqlite dies with "unable to open database [14]" when I run two stmts concurrently
	// therefore, we process by fetching blocks of files
	for i := fileCount + blockSize; i > 0; i = i - blockSize {
		var (
			tx           *sql.Tx
			stmtUpdate   *sql.Stmt
			stmtNotFound *sql.Stmt
			files        []File
			rows         *sql.Rows
		)
		remaining := i

		rows, err = db.Query("SELECT id, filename, filesize FROM files WHERE checksum_sha256 IS NULL AND file_found = '1' LIMIT ?", blockSize)
		defer rows.Close()
		checkErr(err)

		for rows.Next() {
			var id int64
			var filename string
			var filesize int64
			rows.Scan(&id, &filename, &filesize)
			files = append(files, File{ID: id, Name: filename, Size: filesize})
		}
		rows.Close()

		tx, err = db.Begin()
		checkErr(err)

		// prepare update statement
		stmtUpdate, err = tx.Prepare(updateStatement)
		checkErr(err)
		stmtNotFound, err = tx.Prepare(notFoundStatement)
		checkErr(err)

		for _, file := range files {
			path := basepath + file.Name

			fmt.Printf("(%d, %s) making checksum: %s (%s)... ", remaining, ByteSize(totalSize), path, ByteSize(file.Size))

			f, err := os.Open(path)
			if err != nil {
				// file not found
				_, err = stmtNotFound.Exec(file.ID)
				checkErr(err)
			} else {
				hash, err := HashFile(path)
				checkErr(err)
				_, err = stmtUpdate.Exec(hash, file.ID)
				checkErr(err)
			}
			f.Close()

			fmt.Println("OK")
			remaining--
			totalSize = totalSize - file.Size
		}

		stmtUpdate.Close()
		stmtNotFound.Close()
		tx.Commit()

		tx, err = db.Begin()
		checkErr(err)

		// prepare update statement
		stmtUpdate, err = tx.Prepare(updateStatement)
		checkErr(err)
		stmtNotFound, err = tx.Prepare(notFoundStatement)
		checkErr(err)
	}

	return
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

// ReindexCheck runs over all files and compares checksums
func (db *DB) ReindexCheck() {

	// get basepath
	basepath, err := db.GetOption("basepath")
	checkErr(err)

	updateStatement := "UPDATE files SET checksum_ok = ? WHERE id = ?"
	notFoundStatement := "UPDATE files SET file_found = 0 WHERE id = ?"

	db.CollectFiles()
	db.CheckFilesDB()
	db.MakeChecksums()

	// set to check
	fmt.Printf("preparing to check files...")
	_, err = db.Exec(`UPDATE files SET checksum_ok = NULL WHERE file_found = '1'`)
	checkErr(err)
	fmt.Printf("OK\n")

	fileCount, err := db.GetCount("SELECT count(id) FROM files WHERE checksum_ok IS NULL AND file_found = '1'")
	checkErr(err)
	ts, err := db.GetCount("SELECT sum(filesize) FROM files WHERE checksum_ok IS NULL AND file_found = '1'")
	checkErr(err)
	var totalSize int64
	totalSize = int64(ts)

	// dynamically calculate blocksize.
	// lots of small files = large blocksize (10000)
	// huge, few files = small blocksize (100)
	// this is relevant because the commit happens after each block
	fileSizePerCount := float32(totalSize) / float32(fileCount)
	bs := 10000.0 / fileSizePerCount * 50000
	blockSize := int(bs)

	// sqlite dies with "unable to open database [14]" when I run two stmts concurrently
	// therefore, we process by fetching blocks of files
	for i := fileCount + blockSize; i > 0; i = i - blockSize {
		var (
			tx           *sql.Tx
			stmtUpdate   *sql.Stmt
			stmtNotFound *sql.Stmt
			files        []File
			rows         *sql.Rows
		)
		remaining := i

		rows, err = db.Query(`SELECT id, filename, filesize, checksum_sha256
                              FROM files
                              WHERE checksum_ok IS NULL
                              AND file_found = '1'
                              LIMIT ?`, blockSize)
		defer rows.Close()
		checkErr(err)

		for rows.Next() {
			var id int64
			var filename string
			var filesize int64
			var checksum string
			rows.Scan(&id, &filename, &filesize, &checksum)
			files = append(files, File{ID: id, Name: filename, Size: filesize, Checksum: checksum})
		}
		rows.Close()

		tx, err = db.Begin()
		checkErr(err)

		// prepare update statement
		stmtUpdate, err = tx.Prepare(updateStatement)
		checkErr(err)
		stmtNotFound, err = tx.Prepare(notFoundStatement)
		checkErr(err)

		for _, file := range files {
			path := basepath + file.Name

			fmt.Printf("(%d, %s) making checksum: %s (%s)... ", remaining, ByteSize(totalSize), path, ByteSize(file.Size))

			f, err := os.Open(path)
			if err != nil {
				// file not found
				_, err = stmtNotFound.Exec(file.ID)
				checkErr(err)
			} else {
				hash, err := HashFile(path)
				checkErr(err)
				if hash == file.Checksum {
					_, err = stmtUpdate.Exec(1, file.ID)
					checkErr(err)
				} else {
					_, err = stmtUpdate.Exec(0, file.ID)
					checkErr(err)
				}
			}
			f.Close()

			fmt.Println("OK")
			remaining--
			totalSize = totalSize - file.Size
		}

		stmtUpdate.Close()
		stmtNotFound.Close()
		tx.Commit()

		tx, err = db.Begin()
		checkErr(err)

		// prepare update statement
		stmtUpdate, err = tx.Prepare(updateStatement)
		checkErr(err)
		stmtNotFound, err = tx.Prepare(notFoundStatement)
		checkErr(err)
	}

}

// ByteSize displays bytes in human-readable format
type ByteSize float64

func (b ByteSize) String() string {
	const (
		_           = iota // ignore first value by assigning to blank identifier
		KB ByteSize = 1 << (10 * iota)
		MB
		GB
		TB
		PB
		EB
		ZB
		YB
	)

	switch {
	case b >= YB:
		return fmt.Sprintf("%.2fYB", b/YB)
	case b >= ZB:
		return fmt.Sprintf("%.2fZB", b/ZB)
	case b >= EB:
		return fmt.Sprintf("%.2fEB", b/EB)
	case b >= PB:
		return fmt.Sprintf("%.2fPB", b/PB)
	case b >= TB:
		return fmt.Sprintf("%.2fTB", b/TB)
	case b >= GB:
		return fmt.Sprintf("%.2fGB", b/GB)
	case b >= MB:
		return fmt.Sprintf("%.2fMB", b/MB)
	case b >= KB:
		return fmt.Sprintf("%.2fKB", b/KB)
	}
	return fmt.Sprintf("%.2fB", b)
}

// HashFile takes a path and returns a hash
func HashFile(path string) (hash string, err error) {

	file, err := os.Open(path)
	if err != nil {
		fmt.Printf("File not found: %s", path)
		return "", err
	}
	defer file.Close()

	hasher := sha256.New()
	_, err = io.Copy(hasher, file)
	if err != nil {
		return "", err
	}

	hash = hex.EncodeToString(hasher.Sum(nil))

	return hash, nil
}
