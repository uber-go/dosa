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
	"strings"

	"github.com/gobwas/glob"
	"github.com/pkg/errors"
)

// Rule is an assignment from scope.prefixPattern to a connector name.
type Rule struct {
	Scope      string
	NamePrefix string
	Connector  string
	GlobMatch  glob.Glob
}

// NewRule initializes Rule
func NewRule(scope, namePrefix, connector string) (*Rule, error) {
	if namePrefix == "" {
		return nil, errors.New("namePrefix cannot be empty")
	}

	if scope == "" {
		return nil, errors.New("scope cannot be empty")
	}

	// namePrefix must be a valid prefix name, optionally with a suffix *
	var isPrefix bool
	if strings.HasSuffix(namePrefix, "*") {
		isPrefix = true
		namePrefix = namePrefix[:-1]
	}
	if err := dosa.IsValidNamePrefix(namePrefix); err != nil {
		return errors.Wrap(err, fmt.Sprintf("could not parse routing rule: invalid namePrefix %s", namePrefix))
	}

	// Make a regular expression and construct a matcher.
	globMatch := glob.MustCompile(makeRegexp(namePrefix, isPrefix))

	return &Rule{NamePrefix: namePrefix, Scope: scope, Connector: connector, GlobMatch: globMatch}, nil
}

// RouteTo implements the method to choose the matched connector
func (r *Rule) RouteTo(scope string, namePrefix string) bool {
	// scope should be an exact match
	if r.Scope != scope {
		return false
	}

	// namePrefix could be glob match, but first check if there's an exact match
	if r.NamePrefix == namePrefix {
		return true
	}

	if r.GlobMatch.Match(namePrefix) {
		return true
	}

	return false
}

func (r *Rule) String() string {
	return fmt.Sprintf("{%s.%s -> %s}", r.Scope, r.NamePrefix, r.Connector)
}
