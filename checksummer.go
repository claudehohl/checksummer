package main

import (
	"flag"
	"time"
)

// File holds the attributes
type File struct {
	Name     string
	Size     int64
	Mtime    time.Time
	Checksum string
}

func main() {
	flag.Parse()
	database := flag.Arg(0)
	if database == "" {
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
