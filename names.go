// Copyright (c) 2017 Uber Technologies, Inc.
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
package dosa

import (
	"strings"

	"github.com/pkg/errors"
)

const (
	fqnSeparator = "."
	rootFQN      = FQN("")
	maxNameLen   = 32
)

// FQN is the fully qualified name for an entity
type FQN string

// String satisfies fmt.Stringer interface
func (f FQN) String() string {
	return string(f)
}

// IsPrefix checks if this FQN is prefix to the other FQN
func (f FQN) IsPrefix(other FQN) bool {
	if f == rootFQN {
		return true
	}

	switch {
	case len(f) > len(other):
		return false
	case len(f) == len(other):
		return f == other
	}

	// e.g. "foo.bar" is not prefix of "foo.barz", but is prefix to "foo.bar.z"
	return other[0:len(f)] == f && other[len(f)] == '.'
}

// Child returns a new child FQN with the given comp at the end
func (f FQN) Child(comp string) (FQN, error) {
	return "", nil
}

func isInvalidFirstRune(r rune) bool {
	return !((r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || r == '_')
}

func isInvalidOtherRune(r rune) bool {
	return !(r >= '0' && r <= '9') && isInvalidFirstRune(r)
}

// Normalize normalizes names to a canonical representation that contains only [a-z0-9_] and conforms
// the following rules:
// 1. all upper case characters would be replaced with lower case ones
// 2. canonical name can only start with [a-z_]
// 3. the rest of canonical name can contain only [a-z0-9_]
// 4. the length of name must be greater than 0 and less than or equal to maxNameLen
func Normalize(name string) (string, error) {
	if len(name) == 0 || len(name) > maxNameLen {
		return "", errors.Errorf("name must not be empty and cannot have a length "+
			"greater than %d. Actual len= %d", maxNameLen, len(name))
	} else if strings.IndexFunc(name[:1], isInvalidFirstRune) != -1 {
		return "", errors.Errorf("name must start with [A-Za-z_]. Actual='%s'", name)
	} else if strings.IndexFunc(name[1:], isInvalidOtherRune) != -1 {
		return "", errors.Errorf("name must contain only [A-Za-z0-9_], Actual='%s'", name)
	}
	return strings.ToLower(name), nil
}
