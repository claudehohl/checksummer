all:
	clear
	go build
	./checksummer foo.db
	rm foo.db*

.PHONY: all
