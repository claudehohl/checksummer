package main

import (
	"flag"
)

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
		panic("please provide path to checksummer.db!")
	}

	// initialize database
	db, err := Open(database)
	checkErr(err)
	db.Init()

	basepath, _ := db.GetOption("basepath")
	if basepath == "" {
		ChangeBasepath(db)
	}

	LaunchGUI(db)
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
