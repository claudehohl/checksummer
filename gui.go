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

	fmt.Printf("getting basepath...")
	basepath, err := db.GetOption("basepath")
	checkErr(err)
	fmt.Printf("OK\n")

	fmt.Printf("getting file count...")
	filesInDB, err := db.GetCount("SELECT id FROM files LIMIT 1")
	if err != nil {
		filesInDB = 0
	}
	fmt.Printf("OK\n")

	fmt.Printf("getting total filesize...")
	ts, err := db.GetCount("SELECT sum(filesize) FROM files")
	if err != nil {
		ts = 0
	}
	totalSize := ByteSize(ts)
	fmt.Printf("OK\n")

	fmt.Printf("getting deleted files count...")
	deletedFiles, err := db.GetCount("SELECT count(id) FROM files WHERE file_found = '0'")
	if err != nil {
		deletedFiles = 0
	}
	fmt.Printf("OK\n")

	fmt.Printf("getting changed files count...")
	changedFiles, err := db.GetCount("SELECT id FROM files WHERE checksum_ok = '0'")
	if err != nil {
		changedFiles = 0
	}
	fmt.Printf("OK\n")

	clearScreen()

	fmt.Printf("Checksummer %v - filesystem intelligence", VERSION)
	fmt.Println("")
	fmt.Println("basepath is:", basepath)
	fmt.Println("total size: ", totalSize)
	fmt.Println("")
	fmt.Println("=== Collection ===")
	fmt.Println("[cf] collect files")
	if filesInDB > 0 {
		fmt.Println("[cd] check files in database")
		fmt.Println("[mc] make checksums")
		fmt.Println("[rc] reindex & check all files")
	}
	fmt.Println("")
	fmt.Println("=== Analysis ===")
	if filesInDB > 0 {
		fmt.Println("[s] search files")
		fmt.Println("[r] rank by filesize")
		fmt.Println("[m] recently modified files")
		fmt.Println("[ld] list duplicate files")
	}
	if deletedFiles > 0 {
		fmt.Printf("[d] show %v deleted files\n", deletedFiles)
		fmt.Println("[pd] prune deleted files")
	}
	if changedFiles > 0 {
		fmt.Printf("[ch] show %v changed files", changedFiles)
		fmt.Println("[pc] prune changed files")
	}
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
		db.CollectFiles()
	case "cd":
		db.CheckFilesDB()
	case "cb":
		db.ChangeBasepath()
	case "mc":
		db.MakeChecksums()
	case "rc":
		db.ReindexCheck()
	case "r":
		db.RankFilesize()
	case "m":
		db.RankModified()
	case "ld":
		db.ListDuplicates()
	case "d":
		db.ShowDeleted()
	case "pd":
		db.PruneDeleted()
	case "ch":
		db.ShowChanged()
	case "pc":
		db.PruneChanged()
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
