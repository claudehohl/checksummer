all:
	clear
	go build -ldflags '-linkmode external -extldflags -static -w'
	./checksummer foo.db

.PHONY: all
