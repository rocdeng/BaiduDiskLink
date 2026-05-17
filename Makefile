GOCACHE ?= /private/tmp/baidudisklink-go-cache
GOMODCACHE ?= /private/tmp/baidudisklink-gomodcache
GO ?= /usr/local/go/bin/go

.PHONY: test verify check dsm-verify build docker-build docker-up

test:
	GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(GO) test ./...

verify:
	scripts/verify.sh

check: test verify

dsm-verify:
	BAIDUDISKLINK_CONTAINER="$(if $(BAIDUDISKLINK_CONTAINER),$(BAIDUDISKLINK_CONTAINER),baidudisklink)" scripts/dsm-verify.sh

build:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 $(GO) build -o bin/baidudisklink ./cmd/baidudisklink

docker-build:
	docker build -t baidudisklink:latest .

docker-up:
	docker compose up --build
