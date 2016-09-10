all:
	clear
	go build
	./checksummer foo.db
	rm foo.db*
	rm ./checksummer

.PHONY: all
