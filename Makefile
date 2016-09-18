all:
	clear
	go build -ldflags '-linkmode external -extldflags -static -w'
	./checksummer foo.db

darwin:
	CGO_ENABLED=1 GOOS=darwin GOARCH=amd64 CC=o64-clang go build

.PHONY: all
