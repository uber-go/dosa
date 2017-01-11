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

// Child returns a new child FQN with the given comp at the end
func (f FQN) Child(s string) (FQN, error) {
	comp, err := NormalizeName(s)
	if err != nil {
		return "", errors.Wrap(err, "cannot create child FQN")
	}
	return FQN(strings.Join([]string{string(f), comp}, fqnSeparator)), nil
}

// ToFQN converts the input string to FQN
func ToFQN(s string) (FQN, error) {
	if s == "" {
		return rootFQN, nil
	}
	comps := strings.Split(s, fqnSeparator)
	normalizedComps := make([]string, len(comps))
	for i, c := range comps {
		comp, err := NormalizeName(c)
		if err != nil {
			return "", errors.Wrap(err, "cannot create FQN with invalid name component")
		}
		normalizedComps[i] = comp
	}
	return FQN(strings.Join(normalizedComps, fqnSeparator)), nil
}

func isInvalidFirstRune(r rune) bool {
	return !((r >= 'a' && r <= 'z') || r == '_')
}

func isInvalidOtherRune(r rune) bool {
	return !(r >= '0' && r <= '9') && isInvalidFirstRune(r)
}

// IsValidName checks if a name conforms the following rules:
// 1. name starts with [a-z_]
// 2. the rest of name can contain only [a-z0-9_]
// 3. the length of name must be greater than 0 and less than or equal to maxNameLen
func IsValidName(name string) error {
	if len(name) == 0 || len(name) > maxNameLen {
		return errors.Errorf("name must not be empty and cannot have a length "+
			"greater than %d. Actual len= %d", maxNameLen, len(name))
	} else if strings.IndexFunc(name[:1], isInvalidFirstRune) != -1 {
		return errors.Errorf("name must start with [a-z_]. Actual='%s'", name)
	} else if strings.IndexFunc(name[1:], isInvalidOtherRune) != -1 {
		return errors.Errorf("name must contain only [a-z0-9_], Actual='%s'", name)
	}
	return nil
}

// NormalizeName normalizes names to a canonical representation by lowercase everything.
// It returns error if the resultant canonical name is invalid.
func NormalizeName(name string) (string, error) {
	lowercaseName := strings.ToLower(name)
	if err := IsValidName(lowercaseName); err != nil {
		return "", errors.Wrapf(err, "failed to normalize to a valid name for %s", name)
	}
	return lowercaseName, nil
}
