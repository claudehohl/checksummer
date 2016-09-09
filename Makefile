all:
	clear
	rm foo.db*
	go run checksummer.go ../test/

.PHONY: all
