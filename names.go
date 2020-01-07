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

package dosa

import (
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

// This module does some sanity checking of names used in DOSA. A name must have a leading letter,
// followed by a string of letters and digits. A name can have up to 32 chars. Reserved words are
// not allowed as a name.
//
// A special kind of name is the name-prefix. A name-prefix has the same restrictions as a name,
// except that a name-prefix can also contain the "." character.

var (
	namePrefixRegex = regexp.MustCompile("^[a-z_][a-z0-9_.]*$")
	nameRegex       = regexp.MustCompile("^[a-z_][a-z0-9_]*$")

	// Reserved words
	reserved map[string]struct{}
)

func init() {
	// Cassandra's reserved words
	cassandraRsvd := []string{
		"add", "allow", "alter", "and", "apply", "asc", "authorize", "batch", "begin", "by",
		"columnfamily", "create", "delete", "desc", "describe", "drop", "entries", "execute", "from",
		"full", "grant", "if", "in", "index", "infinity", "insert", "into", "keyspace", "limit",
		"modify", "nan", "norecursive", "not", "null", "of", "on", "or", "order", "primary",
		"rename", "replace", "revoke", "schema", "select", "set", "table", "to", "token", "truncate",
		"unlogged", "update", "use", "using", "where", "with"}
	reserved = make(map[string]struct{})
	for _, n := range cassandraRsvd {
		reserved[n] = struct{}{}
	}
}

// DosaNamingRule is the error message for invalid names.
const DosaNamingRule = "DOSA valid names must start with a letter or underscore, and may contain letters, " +
	"digits, and underscores."

// IsValidNamePrefix checks if a name prefix is valid.
func IsValidNamePrefix(namePrefix string) error {
	normalized := strings.ToLower(strings.TrimSpace(namePrefix))
	if !namePrefixRegex.MatchString(normalized) {
		return errors.Errorf("invalid name-prefix '%s': %s", namePrefix, DosaNamingRule)
	}
	return nil
}

// IsValidName checks if a string corresponds to DOSA naming rules.
func IsValidName(name string) error {
	if !nameRegex.MatchString(name) {
		return errors.Errorf("invalid name '%s': %s", name, DosaNamingRule)
	}
	if _, ok := reserved[name]; !ok {
		return nil
	}
	return errors.Errorf("'%s' is a reserved word", name)
}

// NormalizeName normalizes a name to a canonical representation.
func NormalizeName(name string) (string, error) {
	lowercaseName := strings.ToLower(strings.TrimSpace(name))
	if err := IsValidName(lowercaseName); err != nil {
		return "", err
	}
	return lowercaseName, nil
}

// NormalizeNamePrefix normalizes a name-prefix to a canonical representation.
func NormalizeNamePrefix(name string) (string, error) {
	lowercaseName := strings.ToLower(strings.TrimSpace(name))
	if err := IsValidNamePrefix(lowercaseName); err != nil {
		return "", err
	}
	return lowercaseName, nil
}
