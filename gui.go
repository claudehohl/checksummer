package main

import (
	"bufio"
	"fmt"
	"os"
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
	fmt.Println("[cd] check files in database")
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
	case "cd":
		CheckFilesDB(db)
	case "cb":
		ChangeBasepath(db)
	case "q":
		return
	}

	LaunchGUI(db)

}

func clearScreen() {
	// fmt.Print("\033[H\033[2J")
}
