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

COVER_OUT := profile.coverprofile

# all .go files that don't exist in hidden directories
ALL_SRC := $(shell find . -name "*.go" | grep -v -e vendor \
	-e ".*/\..*" \
	-e ".*/_.*")

TEST_TIMEOUT := "-timeout=3s"

.PHONY: test
test: clean vendor
	$(ECHO_V)go test $(RACE) $(TEST_TIMEOUT) $(PKGS)
	$(ECHO_V)$(MAKE) $(COV_REPORT)

TEST_IGNORES = vendor .git
COVER_IGNORES = $(TEST_IGNORES)

comma := ,
null :=
space := $(null) #
OVERALLS_IGNORE = $(subst $(space),$(comma),$(strip $(COVER_IGNORES)))

ifeq ($(V),0)
_FILTER_OVERALLS = cat
else
_FILTER_OVERALLS = grep -v "^Processing:"
endif

COV_REPORT := overalls.coverprofile

$(COV_REPORT): $(PKG_FILES) $(ALL_SRC)
	@$(call label,Running tests)
	@echo
	$(ECHO_V)$(OVERALLS) -project=$(PROJECT_ROOT) \
		-ignore "$(OVERALLS_IGNORE)" \
		-covermode=atomic \
		$(DEBUG_FLAG) -- \
		$(TEST_FLAGS) $(RACE) $(TEST_TIMEOUT) $(TEST_VERBOSITY_FLAG) | \
		grep -v "No Go Test files" | \
		$(_FILTER_OVERALLS)
	$(ECHO_V)if [ -a $(COV_REPORT) ]; then \
		$(GOCOV) convert $@ | $(GOCOV) report ; \
	fi

COV_HTML := coverage.html

$(COV_HTML): $(COV_REPORT)
	$(ECHO_V)$(GOCOV) convert $< | gocov-html > $@

.PHONY: coveralls
coveralls: $(COV_REPORT)
	$(ECHO_V)goveralls -service=travis-ci -coverprofile=$(COV_REPORT)

include $(SUPPORT_FILES)/lint.mk

.PHONY: gendoc
gendoc:
	$(ECHO_V)find . \( \
		-path ./vendor -o  \
		-path ./node_modules -o \
		-path ./.glide \
	\) -prune -o -name README.md -print | \
	xargs -I% md-to-godoc -input=%

.PHONY: clean
clean:
	$(ECHO_V)rm -f $(COV_REPORT) $(COV_HTML) $(LINT_LOG)
	$(ECHO_V)find $(subst /...,,$(PKGS)) -name $(COVER_OUT) -delete

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

CLI_BUILD_VERSION ?= $(shell cat VERSION.txt)
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

mocks/client.go:
	mockgen -package mocks github.com/uber-go/dosa Client,AdminClient > ./mocks/client.go

mocks/connector.go:
	mockgen -package mocks github.com/uber-go/dosa Connector > ./mocks/connector.go

.PHONY: mocks
mocks: mocks/client.go mocks/connector.go
	python ./script/license-headers.py -t LICENSE.txt -d mocks
