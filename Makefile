all:
	clear
	go build
	./checksummer foo.db

.PHONY: all
