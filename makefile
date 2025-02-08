build-app:
	go mod tidy
	go test ./... -v -coverpkg=./...
	go build -o app ./cmd/app/main.go

test:
	go test ./... -v -coverpkg=./...

run-app:
	go run ./cmd/app/main.go