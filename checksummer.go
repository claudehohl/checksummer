package main

import (
	"flag"
)

// channels
var insert = make(chan string)
var clear = make(chan bool)
var commit = make(chan bool)
var commitDone = make(chan bool)
var exit = make(chan bool)

// File is the struct for a file holding attributes
type File struct {
	Name string
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
