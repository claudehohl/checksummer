all:
	clear
	go build
	./checksummer ../test/
	rm foo.db*
	rm ./checksummer

.PHONY: all
