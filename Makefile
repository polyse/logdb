GOCMD=go
GOBUILD=$(GOCMD) build
BINARY_NAME=./bin/adapter
INTEGRATION_TEST_PATH?=./test/integration

all: test build run_server

wire_build:
	wire gen ./cmd/adapter
	echo "wire build"

build:
	$(GOBUILD) -o $(BINARY_NAME) ./cmd/adapter
	echo "binary build"

run_server:
	 LOG_LEVEL=debug $(BINARY_NAME)

test:
	mockery --srcpkg github.com/senyast4745/meilisearch-go --output ./test/mock --all
	mockery --output ./test/mock --dir ./internal/adapter/ --all
	$(GOCMD) test -v ./...

test_integration:
	$(GOCMD) test -tags=integration $(INTEGRATION_TEST_PATH) -count=1 -run=$(INTEGRATION_TEST_SUITE_PATH)