package main

import (
	"flag"
	"path/filepath"
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
	root := flag.Arg(0)
	if root == "" {
		panic("please provide rootpath!")
	}

	// initialize database
	db, err := Open("foo.db")
	db.Init()

	// fire up insert worker
	go InsertWorker(db)

	// walk through files
	err = filepath.Walk(root, FileInspector)
	checkErr(err)

	// wait for clear
	<-clear

	// final commit
	commit <- true

	// wait for commit
	<-commitDone

	// terminate InsertWorker
	exit <- true
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
