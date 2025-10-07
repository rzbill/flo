.PHONY: build build-flo install test lint generate tidy proto proto-clean coverage coverage-report coverage-summary coverage-check

PROTO_DIR := proto
PROTO_GO_OUT := api
API_DIR := api

# Binaries
BIN_DIR := bin
BINARY_NAME := flo

## Coverage
COVERAGE_FILE := coverage.out
THRESHOLD ?= $(or $(COVERAGE_THRESHOLD),50)

build:
	go build ./...

dashboard-build:
	cd dashboard && ( [ -f package-lock.json ] && npm ci || npm install ) && npm run build
	@# Sync built assets into internal/ui/static for embedding
	rm -rf internal/ui/static
	mkdir -p internal/ui/static
	cp -R dashboard/dist/* internal/ui/static/

# Build the flo CLI binary
build-flo:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/$(BINARY_NAME) ./cmd/flo

## Install binaries to GOPATH/bin (fallback to $(HOME)/go/bin if GOPATH unset)
install: build-flo dashboard-build
	@echo "Installing $(BINARY_NAME)..."
	@install -m 755 $(BIN_DIR)/$(BINARY_NAME) $(GOPATH)/bin/ 2>/dev/null || install -m 755 $(BIN_DIR)/$(BINARY_NAME) $(HOME)/go/bin/
	@echo "Installation completed!"

install-dashboard: dashboard-build
	@echo "Installing dashboard..."
	@install -m 755 dashboard/dist/* $(HOME)/go/bin/
	@echo "Installation completed!"

install-flo: build-flo
	@echo "Installing $(BINARY_NAME)..."
	@install -m 755 $(BIN_DIR)/$(BINARY_NAME) $(GOPATH)/bin/ 2>/dev/null || install -m 755 $(BIN_DIR)/$(BINARY_NAME) $(HOME)/go/bin/
	@echo "Installation completed!"

test:
	go test ./...

lint:
	golangci-lint run

generate:
	$(MAKE) proto

tidy:
	go mod tidy

# Protobuf helpers (via protoc)
proto:
	@mkdir -p $(PROTO_GO_OUT)
	@if ! command -v protoc >/dev/null 2>&1; then \
		echo "protoc not found. Please install Protocol Buffers compiler:"; \
		echo "  - macOS:   brew install protobuf"; \
		echo "  - Ubuntu:  apt-get install protobuf-compiler"; \
		echo "  - Windows: download from https://github.com/protocolbuffers/protobuf/releases"; \
		exit 1; \
	fi
	@echo "Installing/updating Go protoc plugins (protoc-gen-go, protoc-gen-go-grpc)"
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	@PROTO_FILES=$$(find $(PROTO_DIR) -name "*.proto" 2>/dev/null); \
	if [ -z "$$PROTO_FILES" ]; then \
		echo "No .proto files under $(PROTO_DIR)/; skipping generation."; \
	else \
		echo "Generating Go stubs..."; \
		protoc -I $(PROTO_DIR) \
			--go_out=$(PROTO_GO_OUT) --go_opt=paths=source_relative \
			--go-grpc_out=$(PROTO_GO_OUT) --go-grpc_opt=paths=source_relative \
			$$PROTO_FILES; \
		echo "Protos generated in $(PROTO_GO_OUT)"; \
	fi

proto-clean:
	rm -rf $(PROTO_DIR)/gen

# Coverage helpers
coverage:
	go test -coverprofile=$(COVERAGE_FILE) ./...

coverage-report:
	@if [ ! -f $(COVERAGE_FILE) ]; then \
		echo "Error: $(COVERAGE_FILE) not found. Run 'make coverage' first."; \
		exit 1; \
	fi
	go tool cover -html=$(COVERAGE_FILE) -o coverage.html
	@echo "Coverage report generated at coverage.html"

coverage-summary:
	@if [ ! -f $(COVERAGE_FILE) ]; then \
		echo "Error: $(COVERAGE_FILE) not found. Run 'make coverage' first."; \
		exit 1; \
	fi
	@go tool cover -func=$(COVERAGE_FILE) | grep total | awk '{print "Total coverage: " $$3}'

coverage-check:
	@echo "Checking unit test coverage threshold: $(THRESHOLD)%"
	@if [ ! -f $(COVERAGE_FILE) ]; then \
		echo "Error: $(COVERAGE_FILE) not found. Run 'make coverage' first."; \
		exit 1; \
	fi
	@unit_cov=$$(go tool cover -func=$(COVERAGE_FILE) | grep total: | awk '{print $$3}' | sed 's/%//'); \
	echo "Unit test coverage: $$unit_cov%"; \
	awk -v a="$$unit_cov" -v b="$(THRESHOLD)" 'BEGIN { exit (a>=b)?0:1 }' && echo "✅ Coverage ("$$unit_cov"%) meets threshold ($(THRESHOLD)%)" || (echo "❌ Coverage ("$$unit_cov"%) below threshold ($(THRESHOLD)%)" && exit 1)


