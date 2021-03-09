LINT_EXCLUDES = _string.go mocks
# Create a pipeline filter for go vet/golint. Patterns specified in LINT_EXCLUDES are
# converted to a grep -v pipeline. If there are no filters, cat is used.
FILTER_LINT := $(if $(LINT_EXCLUDES), grep -v $(foreach file, $(LINT_EXCLUDES),-e $(file)),cat)

FILTER_LOG :=

LINT_LOG := lint.log

_THIS_MAKEFILE := $(lastword $(MAKEFILE_LIST))
_THIS_DIR := $(dir $(_THIS_MAKEFILE))

ERRCHECK_FLAGS := -ignore "bytes:Write*" -ignoretests

.PHONY: lint
lint: vendor
	$(ECHO_V)rm -rf $(LINT_LOG)
	@echo "Installing test dependencies for vet..."
	$(ECHO_V)go test -i $(PKGS)
	@echo "Checking formatting..."
	@$(MAKE) fmt
	@echo "Checking lint..."
	$(ECHO_V)$(foreach dir,$(PKGS),golint $(dir) 2>&1 | $(FILTER_LINT) | tee -a $(LINT_LOG);)
	@echo "Checking unchecked errors..."
	$(ECHO_V)$(foreach dir,$(PKGS),errcheck $(ERRCHECK_FLAGS) $(dir) 2>&1 | $(FILTER_LINT) | tee -a $(LINT_LOG);)
	@echo "Checking for unresolved FIXMEs..."
	$(ECHO_V)git grep -w -i fixme | grep -v -e $(_THIS_MAKEFILE) -e CONTRIBUTING.md | tee -a $(LINT_LOG)
	@echo "Checking for imports of log package"
	$(ECHO_V)go list -f '{{ .ImportPath }}: {{ .Imports }}' $(shell glide nv) | grep -e "\blog\b" | tee -a $(LINT_LOG)
