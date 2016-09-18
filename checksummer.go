package main

import (
	"flag"
	"fmt"
	"os"
)

// VERSION sets the version
const VERSION = "v3.0.0-beta12"

// File holds the attributes
type File struct {
	ID       int64
	Name     string
	Size     int64
	Mtime    int64
	Checksum string
}

func main() {
	flag.Parse()
	database := flag.Arg(0)
	if database == "" {
		fmt.Println("Checksummer version", VERSION)
		fmt.Println("")
		fmt.Println("Usage:   ./checksummer sqlite3.db [search arguments]")
		fmt.Println("")
		fmt.Println("Example: ./checksummer myfiles.db")
		fmt.Println("")
		os.Exit(1)
	}

	// initialize database
	db, err := Open(database)
	checkErr(err)
	db.Init()

	basepath, _ := db.GetOption("basepath")
	if basepath == "" {
		db.ChangeBasepath()
	}

	term := flag.Arg(1)
	if term != "" {
		db.Search(term)
		os.Exit(0)
	}

	LaunchGUI(db)
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
