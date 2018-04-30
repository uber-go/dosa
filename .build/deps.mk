.PHONY: libdeps
libdeps:
	@$(call label,Installing Glide and locked dependencies...)
	$(ECHO_V)glide --version 2>/dev/null || go get -u -f github.com/Masterminds/glide
	$(ECHO_V)glide install

.PHONY: deps
deps: libdeps
	@$(call label,Installing test dependencies...)
	$(ECHO_V)go install ./vendor/github.com/axw/gocov/gocov
	$(ECHO_V)go install ./vendor/github.com/matm/gocov-html
	$(ECHO_V)go install ./vendor/github.com/mattn/goveralls
	$(ECHO_V)go install ./vendor/github.com/go-playground/overalls
	@$(call label,Installing golint...)
	$(ECHO_V)go install ./vendor/golang.org/x/lint/golint
	@$(call label,Installing errcheck...)
	$(ECHO_V)go install ./vendor/github.com/kisielk/errcheck
	@$(call label,Installing md-to-godoc...)
	$(ECHO_V)go install ./vendor/github.com/sectioneight/md-to-godoc

GOCOV := gocov
OVERALLS := overalls
