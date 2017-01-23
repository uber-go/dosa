SHELL := /bin/bash
PROJECT_ROOT := go.uber.org/dosa

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
test: vendor
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
	$(ECHO_V)find . -name README.md \
		-not -path "./vendor/*" \
		-not -path "./node_modules/*" | \
		xargs -I% md-to-godoc -input=%

.PHONY: clean
clean:
	$(ECHO_V)rm -f $(COV_REPORT) $(COV_HTML) $(LINT_LOG)
	$(ECHO_V)find $(subst /...,,$(PKGS)) -name $(COVER_OUT) -delete

.PHONY: vendor
vendor:
	$(ECHO_V)test -d vendor
	$(MAKE) deps

.PHONY: fmt
fmt:
  GOIMPORTS := $(shell command -v goimports 2> /dev/null)
ifdef GOIMPORTS
				$(ECHO_V)$(GOIMPORTS) -w $(ALL_SRC)
else
				$(ECHO_V)gofmt -w $(ALL_SRC)
endif
