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

const lastASCIIChar = "~"

// Rule is an assignment from scope.prefixPattern to a connector name.
type Rule struct {
	scope         string
	namePrefix    string
	connector     string
	scopePattern  *regexp.Regexp
	prefixPattern *regexp.Regexp
	// The canonical representations of scope and prefix, chosen so that they sort in the required orrder.
	canonScope string
	canonPfx   string
}

// NewRule initializes Rule
func NewRule(scope, namePrefix, connector string) (*Rule, error) {
	// scope must be a valid name, optionally with a suffix *. namePrefix must be a valid prefix name,
	// optionally with a suffix *. If it's just a "*" it's converted to "".
	if scope == "" {
		return nil, errors.New("could not parse routing rule: scope cannot be empty")
	}
	if scope == DefaultName {
		scope = "*"
	}
	if namePrefix == "" {
		return nil, errors.New("could not parse routing rule: namePrefix cannot be empty")
	}
	if namePrefix == DefaultName {
		namePrefix = "*"
	}

	scope, canonScope, scopePat, err := parseName(scope, true)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("could not parse routing rule: invalid scope %s", scope))
	}

	namePrefix, canonPrefix, prefixPat, err := parseName(namePrefix, false)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("could not parse routing rule: invalid namePrefix %s", namePrefix))
	}

	return &Rule{
		namePrefix:    namePrefix,
		scope:         scope,
		connector:     connector,
		scopePattern:  scopePat,
		prefixPattern: prefixPat,
		canonScope:    canonScope,
		canonPfx:      canonPrefix,
	}, nil
}

// Parse a name (scope or prefix) optionaly with a "*" suffix. Return the underlying name, a canonical representation
// of the name, and if the name is a glob, a regular expression recognizing it.
func parseName(name string, isScope bool) (string, string, *regexp.Regexp, error) {
	var regex *regexp.Regexp
	isGlob := false

	if strings.HasSuffix(name, "*") {
		name = strings.TrimSuffix(name, "*")
		isGlob = true
	}
	// Sanity check the name. No need to check "", that's the pattern "*".
	if name != "" {
		var err error
		if isScope {
			_, err = dosa.NormalizeName(name)
		} else {
			_, err = dosa.NormalizeNamePrefix(name)
		}
		if err != nil {
			return "", "", nil, err
		}
	}
	canon, err := canonicalize(name, isGlob, isScope)
	if err != nil {
		return "", "", nil, err
	}
	if isGlob {
		regex = makePrefixRegexp(name)
	}
	return name, canon, regex, nil
}

// Scope returns the rule's scope.
func (r *Rule) Scope() string {
	if r.scopePattern == nil {
		return r.scope
	}
	return r.scope + "*"
}

// NamePrefix returns the rule's name-prefix.
func (r *Rule) NamePrefix() string {
	if r.prefixPattern == nil {
		return r.namePrefix
	}
	return r.namePrefix + "*"
}

// Destination returns the rule's destination.
func (r *Rule) Destination() string {
	return r.connector
}

func (r *Rule) String() string {
	var prefixPat, scopePat string
	if r.scopePattern != nil {
		scopePat = "*"
	}
	if r.prefixPattern != nil {
		prefixPat = "*"
	}
	return fmt.Sprintf("{%s%s.%s%s -> %s}", r.scope, scopePat, r.namePrefix, prefixPat, r.connector)
}

// canHandle says whether or not this Rule can handle the given scope:prefix.
func (r *Rule) canHandle(scope string, namePrefix string) bool {
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

// Make a regexp from the "glob" pattern used in Rule rules.
func makePrefixRegexp(pat string) *regexp.Regexp {
	if pat == "" {
		return regexp.MustCompile(".")
	}
	// Quote dots and add begin-of-line. The string is known to be a valid scope/prefix name i.e.
	// alphanumeric possibly with "." characters, no other special chars need to be quoted.
	return regexp.MustCompile("^" + strings.Join(strings.Split(pat, "."), "\\."))
}

// Convert a string to canonical form that allows lexicographic sorting according to Rule rules.
func canonicalize(s string, isPattern bool, isScope bool) (string, error) {
	if s == "" && isPattern {
		return lastASCIIChar, nil
	}

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
	if s == DefaultName {
		s = lastASCIIChar
	}
	if !isPattern {
		return s, nil
	}
	return s + lastASCIIChar, nil // 0x7e
}
