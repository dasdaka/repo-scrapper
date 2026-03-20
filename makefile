.PHONY: build run serve scrape aggregate all test clean help

BINARY := app.exe

## build       Compile the binary
build:
	go build -o $(BINARY) ./cmd/app

## serve       Build then start the web dashboard (http://localhost:8080)
serve: build
	./$(BINARY) serve

## scrape      Build then fetch raw PR data from Bitbucket
scrape: build
	./$(BINARY) scrape

## aggregate   Build then rebuild pr_report from stored raw data
aggregate: build
	./$(BINARY) aggregate

## run         Build then run the full pipeline  scrape -> aggregate
run: build
	./$(BINARY) all

## test        Run all tests
test:
	go test ./...

## clean       Remove the compiled binary
clean:
	rm -f $(BINARY)

## help        List available targets
help:
	@grep -E '^## ' Makefile | sed 's/## //'
