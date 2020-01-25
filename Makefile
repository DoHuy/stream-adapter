# BINARY_NAME defaults to the name of the repository
BINARY_NAME := $(notdir $(shell pwd))
BUILD_INFO_FLAGS := -X main.BuildTime=$(shell date -u '+%Y-%m-%d_%H:%M:%S') -X main.BuildCommitHash=$(shell git rev-parse HEAD)
LIST_NO_VENDOR := $(shell go list ./... | grep -v /vendor/ | grep -v mocks)
GOBIN := $(GOPATH)/bin
CONFIG_FILE ?= config.json
DEFAULT_PAYLOAD_FILE ?= sample/payload-video.json

default: check fmt clean deps test build
deploy: build-docker push-dev push-stage push-prod push-ukprod

.PHONY: build
build:
	# Build project
	go build -ldflags "$(BUILD_INFO_FLAGS) $(OSX_BUILD_FLAGS)" -a -o $(BINARY_NAME) .

.PHONY: build-docker
build-docker:
	docker build -t webstream-adapter \
		--build-arg GITHUB_ACCESS_TOKEN=$(GITHUB_ACCESS_TOKEN) \
		--build-arg CONFIG_FILE=$(CONFIG_FILE) \
		--build-arg DEFAULT_PAYLOAD_FILE=$(DEFAULT_PAYLOAD_FILE) .

.PHONY: push-dev
push-dev:
	docker tag webstream-adapter docker.aws-dev.veritone.com/7682/webstream-adapter
	docker push docker.aws-dev.veritone.com/7682/webstream-adapter

.PHONY: push-stage
push-stage:
	docker tag webstream-adapter docker.stage.veritone.com/7682/webstream-adapter
	docker push docker.stage.veritone.com/7682/webstream-adapter

.PHONY: push-prod
push-prod:
	docker tag webstream-adapter docker.veritone.com/7682/webstream-adapter
	docker push docker.veritone.com/7682/webstream-adapter

.PHONY: push-ukprod
push-ukprod:
	docker tag webstream-adapter docker.uk.veritone.com/1/webstream-adapter
	docker push docker.uk.veritone.com/1/webstream-adapter

.PHONY: check
check:
	# Only continue if go is installed
	go version || ( echo "Go not installed, exiting"; exit 1 )

.PHONY: clean
clean:
	go clean -i
	rm -rf ./vendor
	rm -f $(BINARY_NAME)

deps:
	# Install or update govend
	go get -u github.com/govend/govend
	# Fetch vendored dependencies
	rm -rf ./vendor
	$(GOBIN)/govend -v --prune

.PHONY: fmt
fmt:
	# Format all Go source files (excluding vendored packages)
	go fmt $(LIST_NO_VENDOR)

generate-deps:
	# Generate vendor.yml
	$(GOBIN)/govend -v -l --prune

.PHONY: test
test:
	# Run all tests (excluding vendored packages)
	go test -a -v -cover -timeout 30s $(LIST_NO_VENDOR)
