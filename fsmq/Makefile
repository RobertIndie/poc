.PHONY: build build-linux-amd64 build-linux-arm64 docker-build docker-build-multi clean

# Binary name
BINARY_NAME := fsmq-perf

# Output directories
BIN_DIR := bin
DIST_DIR := dist

# Docker image name (can be overridden)
IMAGE ?= fsmq-perf:latest

# Build for current platform
build:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/$(BINARY_NAME) ./cmd/fsmq-perf

# Build for Linux AMD64
build-linux-amd64:
	@mkdir -p $(DIST_DIR)
	GOOS=linux GOARCH=amd64 go build -o $(DIST_DIR)/$(BINARY_NAME)-linux-amd64 ./cmd/fsmq-perf

# Build for Linux ARM64
build-linux-arm64:
	@mkdir -p $(DIST_DIR)
	GOOS=linux GOARCH=arm64 go build -o $(DIST_DIR)/$(BINARY_NAME)-linux-arm64 ./cmd/fsmq-perf

# Build Docker image for amd64 (loads to local Docker)
docker-build: build-linux-amd64
	docker buildx build --platform linux/amd64 -t $(IMAGE) --load .

# Build Docker images for both amd64 and arm64
# Note: --load only loads one platform. Use PUSH=true to push both to registry.
# Example: PUSH=true make docker-build-multi
docker-build-all: build-linux-amd64 build-linux-arm64
	@if [ "$(PUSH)" = "true" ]; then \
		echo "Building and pushing multi-arch image to registry..."; \
		docker buildx build --platform linux/amd64,linux/arm64 -t $(IMAGE) --push .; \
	else \
		echo "Building multi-arch image (only amd64 will be loaded locally)..."; \
		echo "Use PUSH=true make docker-build-multi to push both architectures to registry"; \
		docker buildx build --platform linux/amd64,linux/arm64 -t $(IMAGE) --load .; \
	fi

# Clean build artifacts
clean:
	rm -rf $(BIN_DIR) $(DIST_DIR)

