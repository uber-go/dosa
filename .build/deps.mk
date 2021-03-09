.PHONY: libdeps
modvendor:
	@$(call label,Updating vendor mod...)
	$(ECHO_V)go mod vendor

.PHONY: deps
deps: modvendor
	@$(call label,Installing golint...)
	$(ECHO_V)go install ./vendor/golang.org/x/lint/golint
