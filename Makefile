.PHONY: help proto run build clean test docker-build docker-up docker-down docker-logs install-deps lint

help:
	@echo "Market Data Streaming Service - Makefile"
	@echo ""
	@echo "Available commands:"
	@echo "  make proto        - Generate Protocol Buffer code"
	@echo "  make run          - Run the service locally"
	@echo "  make build        - Build the service binary"
	@echo "  make clean        - Clean generated files and binaries"
	@echo "  make test         - Run tests"
	@echo "  make lint         - Run linters"
	@echo "  make docker-build - Build Docker image"
	@echo "  make docker-up    - Start all services with Docker Compose"
	@echo "  make docker-down  - Stop all services"
	@echo "  make docker-logs  - View service logs"
	@echo "  make install-deps - Install development dependencies"

proto:
	@echo "Generating Protocol Buffer code..."
	@./generate_proto.sh

run: proto
	@echo "Starting market service..."
	@go run cmd/market-service/main.go

build: proto
	@echo "Building market service..."
	@mkdir -p bin
	@go build -o bin/market-service cmd/market-service/main.go
	@echo "Binary created: bin/market-service"

clean:
	@echo "Cleaning..."
	@rm -rf bin/
	@rm -f proto/*.pb.go
	@go clean
	@echo "Done"

test:
	@echo "Running tests..."
	@go test -v -race -cover ./...

lint:
	@echo "Running linters..."
	@go vet ./...
	@golangci-lint run || echo "golangci-lint not installed, skipping..."

install-deps:
	@echo "Installing dependencies..."
	@go mod download
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	@echo "Done"

docker-build:
	@echo "Building Docker image..."
	@docker build -t market-service:latest .

docker-up:
	@echo "Starting services with Docker Compose..."
	@docker-compose up -d
	@echo ""
	@echo "Services started!"
	@echo "- gRPC: localhost:50051"
	@echo "- HTTP:  http://localhost:8080"
	@echo ""
	@echo "Check health: curl http://localhost:8080/health"
	@echo "View logs: make docker-logs"

docker-down:
	@echo "Stopping services..."
	@docker-compose down

docker-logs:
	@docker-compose logs -f market-service

docker-restart: docker-down docker-up

# Development helpers
dev-config:
	@cp configs/config.development.yaml configs/config.yaml
	@echo "Development config copied to configs/config.yaml"

prod-config:
	@cp configs/config.production.yaml configs/config.yaml
	@echo "Production config copied to configs/config.yaml"
