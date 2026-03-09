BINARY_NAME := kafka-lag-exporter
DOCKER_IMAGE := ghcr.io/carlgra/kafka-lag-exporter
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")

.PHONY: build test bench lint docker clean fmt vet helm-test check install-hooks push

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

check: fmt vet lint test
	@echo "All checks passed."

install-hooks:
	cp scripts/pre-push .git/hooks/pre-push
	chmod +x .git/hooks/pre-push
	@echo "Git hooks installed."

push: check
	git push $(PUSH_ARGS)

clean:
	rm -rf bin/
	go clean -testcache
