VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
IMAGE   ?= telemetry-pipeline

.PHONY: all build test race lint generate image check-config clean

all: lint test build

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/pipeline ./cmd/pipeline

test:
	go test ./...

race:
	go test -race -count=1 ./...

lint:
	gofmt -l . | (! grep .) || (echo "run gofmt -w ."; exit 1)
	go vet ./...

# Regenerate Go code from proto/ (requires buf, protoc-gen-go, protoc-gen-go-grpc).
generate:
	buf generate

image:
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE):$(VERSION) .

check-config: build
	GNMI_PASSWORD=x INFLUX_TOKEN=x ./bin/pipeline -config examples/pipeline.yaml -check

clean:
	rm -rf bin
