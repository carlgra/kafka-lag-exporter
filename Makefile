BINARY_NAME := kafka-lag-exporter
DOCKER_IMAGE := seglo/kafka-lag-exporter
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")

.PHONY: build test bench lint docker clean fmt vet helm-test

build:
	CGO_ENABLED=0 go build -ldflags="-s -w -X main.version=$(VERSION)" -o bin/$(BINARY_NAME) ./cmd/kafka-lag-exporter

test:
	go test -race -count=1 ./internal/... ./cmd/...

bench:
	go test -bench=. -benchmem -count=3 ./internal/...

test-integration:
	go test -race -tags=integration -count=1 ./integration/...

lint:
	golangci-lint run ./...

fmt:
	gofmt -s -w .

vet:
	go vet ./...

docker:
	docker build -t $(DOCKER_IMAGE):$(VERSION) -t $(DOCKER_IMAGE):latest .

helm-test:
	helm unittest charts/kafka-lag-exporter

clean:
	rm -rf bin/
	go clean -testcache
