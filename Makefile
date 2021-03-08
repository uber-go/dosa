SHELL := /bin/bash
PROJECT_ROOT := github.com/uber-go/dosa

SUPPORT_FILES := .build
include $(SUPPORT_FILES)/colors.mk
include $(SUPPORT_FILES)/deps.mk
include $(SUPPORT_FILES)/flags.mk
include $(SUPPORT_FILES)/verbosity.mk

.PHONY: all
all: lint test
.DEFAULT_GOAL := all

# all .go files that don't exist in hidden directories
ALL_SRC := $(shell find . -name "*.go" | grep -v -e vendor \
	-e ".*/\..*" \
	-e ".*/_.*")

TEST_TIMEOUT := "-timeout=60s"

.PHONY: test
test: clean vendor
	$(ECHO_V)go test $(RACE) $(TEST_TIMEOUT) $(PKGS)

TEST_IGNORES = vendor .git

include $(SUPPORT_FILES)/lint.mk

.PHONY: clean
clean:
	$(ECHO_V)rm -f $(LINT_LOG)

.PHONY: vendor
vendor:
	$(MAKE) deps

.PHONY: fmt
GOIMPORTS=goimports
fmt:
	$(ECHO_V)gofmt -s -w $(ALL_SRC)
	$(ECHO_V)if [ "$$TRAVIS" != "true" ]; then \
		$(GOIMPORTS) -w $(ALL_SRC) ; \
	fi

CLI_BUILD_VERSION ?= $(shell git describe --abbrev=0 --tags)
CLI_BUILD_TIMESTAMP ?= $(shell date -u '+%Y-%m-%d_%I:%M:%S%p')
CLI_BUILD_REF ?= $(shell git rev-parse --short HEAD)
CLI_LINKER_FLAGS="-X main.version=$(CLI_BUILD_VERSION) -X main.timestamp=$(CLI_BUILD_TIMESTAMP) -X main.githash=$(CLI_BUILD_REF)"

.PHONY: cli
cli:
	$(ECHO_V)go build -ldflags $(CLI_LINKER_FLAGS) -o $$GOPATH/bin/dosa ./cmd/dosa
ifdef target
ifeq ($(target), Darwin)
	$(ECHO_V)GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 go build -ldflags $(CLI_LINKER_FLAGS) -o ./out/cli/darwin/dosa ./cmd/dosa 
else ifeq ($(target), Linux)
	$(ECHO_V)GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags $(CLI_LINKER_FLAGS) -o ./out/cli/linux/dosa ./cmd/dosa
endif
endif

mocks/client.go: client.go
	mockgen -package mocks github.com/uber-go/dosa Client,AdminClient > ./mocks/client.go

mocks/connector.go: connector.go
	mockgen -package mocks github.com/uber-go/dosa Connector > ./mocks/connector.go

.PHONY: mocks
mocks: mocks/client.go mocks/connector.go
	python ./script/license-headers.py -t LICENSE.txt -d mocks
