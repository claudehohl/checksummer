package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// LaunchGUI starts the user interface
func LaunchGUI(db *Conn) {

	clearScreen()

	basepath, err := db.GetOption("basepath")
	checkErr(err)

	fmt.Println("")
	fmt.Println("basepath is:", basepath)
	fmt.Println("total size: ")
	fmt.Println("")
	fmt.Println("=== Collecting ===")
	fmt.Println("[cf] collect files")
	// fmt.Println("[cs] collect filestats")
	// fmt.Println("[mc] make checksums")
	// fmt.Println("[rc] reindex & check all files")
	// fmt.Println("")
	// fmt.Println("=== Stats ===")
	// fmt.Println("[s] search files")
	// fmt.Println("[r] rank by filesize")
	// fmt.Println("[m] recently modified files")
	// fmt.Println("[ld] list duplicate files")
	// fmt.Println("[d] show X deleted files")
	// fmt.Println("[pd] prune deleted files")
	// fmt.Println("[ch] show X changed files")
	// fmt.Println("[pc] prune changed files")
	fmt.Println("")
	fmt.Println("[cb] change basepath")
	fmt.Println("[q] exit")
	fmt.Println("")

	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Select: ")
	choice, _ := reader.ReadString('\n')

	choice = strings.Trim(choice, "\n")

	switch choice {
	case "cf":
		CollectFiles(db)
	case "cb":
		ChangeBasepath(db)
	case "q":
		return
	}

	LaunchGUI(db)

}

func clearScreen() {
	fmt.Print("\033[H\033[2J")
}

// CollectFiles starts insert worker and walks through files
func CollectFiles(db *Conn) {
	// fire up insert worker
	go InsertWorker(db)

	// walk through files
	err := filepath.Walk("../test/", FileInspector)
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

// ChangeBasepath sets the basepath
func ChangeBasepath(db *Conn) {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Choose base path")
	fmt.Print("(enter full path, without trailing slash): ")
	basepath, _ := reader.ReadString('\n')
	basepath = strings.Trim(basepath, "\n")
	err := db.SetOption("basepath", basepath)
	checkErr(err)
}
