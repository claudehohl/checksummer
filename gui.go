package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

// LaunchGUI starts the user interface
func LaunchGUI(db *DB) {

	clearScreen()

	basepath, err := db.GetOption("basepath")
	checkErr(err)

	filesInDB, err := db.GetCount("SELECT id FROM files LIMIT 1")
	if err != nil {
		filesInDB = 0
	}

	ts, err := db.GetCount("SELECT sum(filesize) FROM files")
	if err != nil {
		ts = 0
	}
	totalSize := ByteSize(ts)

	fmt.Println("Checksummer v3.0.0-dev200 - filesystem intelligence")
	fmt.Println("")
	fmt.Println("basepath is:", basepath)
	fmt.Println("total size: ", totalSize)
	fmt.Println("")
	fmt.Println("=== Collection ===")
	fmt.Println("[cf] collect files")
	if filesInDB > 0 {
		fmt.Println("[cd] check files in database")
		fmt.Println("[mc] make checksums")
		// fmt.Println("[rc] reindex & check all files")
	}
	fmt.Println("")
	fmt.Println("=== Analysis ===")
	if filesInDB > 0 {
		fmt.Println("[s] search files")
		fmt.Println("[r] rank by filesize")
		// fmt.Println("[m] recently modified files")
	}
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
	case "mc":
		MakeChecksums(db)
	case "r":
		db.RankFilesize()
	case "q":
		return
	}

	LaunchGUI(db)

}

func clearScreen() {
	fmt.Print("\033[H\033[2J")
}

func pager(str string, autoQuit bool) {

	var cmd *exec.Cmd

	if autoQuit {
		cmd = exec.Command("less", "-X", "--quit-if-one-screen")
	} else {
		cmd = exec.Command("less", "-X")
	}

	// create a pipe (blocking)
	r, stdin := io.Pipe()

	// Set your i/o's
	cmd.Stdin = r
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// Create a blocking chan, Run the pager and unblock once it is finished
	c := make(chan struct{})
	go func() {
		defer close(c)
		cmd.Run()
	}()

	// Pass anything to your pipe
	fmt.Fprintf(stdin, str)

	// Close stdin (result in pager to exit)
	stdin.Close()

	// Wait for the pager to be finished
	<-c

}
