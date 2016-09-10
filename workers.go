package main

import (
	"fmt"
)

// InsertWorker runs the statement, commits every 10k inserts
func InsertWorker(c *Conn) {
	err := c.Begin()
	checkErr(err)

	// Precompile SQL statement
	stmt, err := c.PrepareInsert()

	clear <- true

	i := 0
	for {
		select {
		case filename := <-insert:
			err := c.InsertFilename(&File{Name: filename}, stmt)
			checkErr(err)
			i++

			// commit every 10k inserts
			if i%10000 == 0 {
				fmt.Println(i)
				c.Commit()
				err = c.Begin()
				checkErr(err)
			}

			clear <- true

		case <-commit:
			c.Commit()
			commitDone <- true
		}
	}
}
