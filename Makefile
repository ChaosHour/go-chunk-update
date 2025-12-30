.PHONY: build clean test

# Build the binary into ./bin/
build:
	mkdir -p bin
	go build -o bin/go-chunk-update ./cmd/chunk

# Clean build artifacts
clean:
	rm -rf bin/

# Run comprehensive tests with Docker MySQL
test-docker:
	./test_runner.sh all

# Run unit tests only
test-unit:
	go test ./...

# Run integration tests only (requires Docker MySQL running)
test-integration:
	./test_runner.sh test

# Run performance tests only (requires Docker MySQL running)
test-perf:
	./test_runner.sh perf

# Start Docker MySQL environment
test-start:
	./test_runner.sh start

# Stop Docker MySQL environment
test-stop:
	./test_runner.sh stop

# Run tests
test:
	go test ./...

# Run tests with coverage
test-cover:
	go test -cover ./...

# Run tests with verbose output
test-verbose:
	go test -v ./...

# Format code
fmt:
	go fmt ./...

# Lint code (requires golangci-lint)
lint:
	golangci-lint run

# Install dependencies
deps:
	go mod download

# Tidy dependencies
tidy:
	go mod tidy