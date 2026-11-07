VERSION ?= $(patsubst v%,%,$(shell git describe --tags --always --dirty 2>/dev/null || echo dev))
IMAGE   ?= ghcr.io/tpural/possum
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: build test lint fmt image chart install-yaml

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/possum ./cmd/possum

test:
	go test -race ./...

lint:
	golangci-lint run
	helm lint charts/possum

fmt:
	gofumpt -w .

image:
	docker buildx build --platform linux/amd64,linux/arm64 --build-arg VERSION=$(VERSION) -t $(IMAGE):$(VERSION) .

chart:
	helm package charts/possum --version $(VERSION) --app-version $(VERSION) -d dist

install-yaml:
	mkdir -p dist
	helm template possum charts/possum --namespace possum --set image.tag=$(VERSION) > dist/install.yaml
