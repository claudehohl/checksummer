package main

import (
	"flag"
	"fmt"
	"os"
)

// VERSION sets the version
const VERSION = "v3.0.0-dev300"

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
		// TODO: nicer error msg
		fmt.Println("please provide path to checksummer.db!")
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

	LaunchGUI(db)
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
