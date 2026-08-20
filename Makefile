.PHONY: proto test build

proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		api/anton/v1/ingest.proto

test:
	go test ./...

build:
	CGO_ENABLED=0 go build -o anton-gton-extension .
