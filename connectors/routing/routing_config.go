// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package routing

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
)

// Rule is an assignment from scope.prefixPattern to a connector name.
type Rule struct {
	scope         string
	namePrefix    string
	connector     string
	prefixPattern *regexp.Regexp
	scopePattern  *regexp.Regexp
	canonScope    string
	canonPfx      string
}

// NewRule initializes Rule
func NewRule(scope, namePrefix, connector string) (*Rule, error) {
	// scope must be a valid name, optionally with a suffix *.
	if scope == "" {
		return nil, errors.New("scope cannot be empty")
	}
	var scopeIsPattern bool
	if strings.HasSuffix(scope, "*") {
		scopeIsPattern = true
		scope = strings.TrimSuffix(scope, "*")
	}
	nScope, err := canonicalize(scope, scopeIsPattern, true) // isScope = true
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("invalid scope name %s", scope))
	}

	// namePrefix must be a valid prefix name, optionally with a suffix *
	if namePrefix == "" {
		return nil, errors.New("namePrefix cannot be empty")
	}
	if namePrefix == "*" {
		namePrefix = DefaultName // This makes the following code simpler.
	}
	var prefixIsPattern bool
	if strings.HasSuffix(namePrefix, "*") {
		prefixIsPattern = true
		namePrefix = strings.TrimSuffix(namePrefix, "*")
	}
	if err := dosa.IsValidNamePrefix(namePrefix); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("could not parse routing rule: invalid namePrefix %s", namePrefix))
	}
	nPrefix, _ := canonicalize(namePrefix, prefixIsPattern, false) // isScope = false

	// Make a regular expressions and matchers
	prefixPat := makeRegexp(namePrefix, prefixIsPattern)
	scopePat := makeRegexp(scope, scopeIsPattern)

	return &Rule{
		namePrefix:    namePrefix,
		scope:         scope,
		connector:     connector,
		scopePattern:  scopePat,
		prefixPattern: prefixPat,
		canonScope:    nScope,
		canonPfx:      nPrefix,
	}, nil
}

// RouteTo (bad name!) says whether or not this Rule can handle the given scope:prefix.
func (r *Rule) RouteTo(scope string, namePrefix string) bool {
	if r.scope == scope {
		if r.namePrefix == namePrefix {
			// Exact match for both.
			return true
		}
		if r.prefixPattern != nil && r.prefixPattern.MatchString(namePrefix) {
			// Exact match for scope, RE match for prefix.
			return true
		}
	}

	if r.scopePattern != nil && r.scopePattern.MatchString(scope) {
		if r.namePrefix == namePrefix {
			// RE match for scope, exact match for prefix.
			return true
		}
		if r.prefixPattern != nil && r.prefixPattern.MatchString(namePrefix) {
			// RE match for both.
			return true
		}
	}

	return false
}

func (r *Rule) String() string {
	return fmt.Sprintf("{%s.%s -> %s}", r.scope, r.namePrefix, r.connector)
}

// Make a regexp from the "glob" pattern used in Rule rules.
func makeRegexp(pat string, isPrefix bool) *regexp.Regexp {
	if pat == DefaultName {
		return regexp.MustCompile(".")
	}
	// Quote dots and add begin-of-line
	pat = "^" + strings.Join(strings.Split(pat, "."), "\\.")
	if !isPrefix {
		pat = pat + "$"
	}
	return regexp.MustCompile(pat)
}

// Convert a string to canonical form that allows lexicographic sorting according to Rule rules.
func canonicalize(s string, isPattern bool, isScope bool) (string, error) {
	// Returned value is not necessarily a valid regexp.
	var err error
	if isScope {
		s, err = dosa.NormalizeName(s)
	} else {
		s, err = dosa.NormalizeNamePrefix(s)
	}
	if err != nil {
		return "", err
	}
	if !isPattern {
		return s, nil
	}
	return s + "~", nil // 0x7e
}
