BINARY := bin/scheduler

.PHONY: build test lint run vet fmt clean

build:
	go build -o $(BINARY) ./cmd/scheduler

test:
	go test -race ./...

vet:
	go vet ./...

lint:
	golangci-lint run

fmt:
	gofmt -l -w .
	go run golang.org/x/tools/cmd/goimports@latest -w -local scheduler .

run: build
	./$(BINARY)

clean:
	rm -rf bin
