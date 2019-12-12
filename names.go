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

const (
	maxNameLen = 32
)

var (
	namePrefixRegex = regexp.MustCompile("^[a-z_][a-z0-9_.]{0,31}$")
	nameRegex       = regexp.MustCompile("^[a-z_][a-z0-9_]{0,31}$")

	// Reserved words
	reserved map[string]struct{}
)

func init() {
	// Cassandra's reserved words
	cassandraRsvd := []string{"add", "aggregate", "all", "allow", "alter", "and", "any", "apply", "as",
		"asc", "ascii", "authorize", "batch", "begin", "bigint", "blob", "boolean", "by", "clustering",
		"columnfamily", "compact", "consistency", "count", "counter", "create", "custom", "decimal",
		"delete", "desc", "distinct", "double", "drop", "each_quorum", "entries", "exists", "filtering",
		"float", "from", "frozen", "full", "grant", "if", "in", "index", "inet", "infinity", "insert",
		"int", "into", "key", "keyspace", "keyspaces", "level", "limit", "list", "local_one",
		"local_quorum", "map", "materialized", "modify", "nan", "norecursive", "nosuperuser", "not",
		"of", "on", "one", "order", "partition", "password", "per", "permission", "permissions",
		"primary", "quorum", "rename", "revoke", "schema", "select", "set", "static", "storage",
		"superuser", "table", "text", "time", "timestamp", "timeuuid", "three", "to", "token",
		"truncate", "ttl", "tuple", "two", "type", "unlogged", "update", "use", "user", "users",
		"using", "uuid", "values", "varchar", "varint", "view", "where", "with", "writetime"}
	reserved = make(map[string]struct{})
	for _, n := range cassandraRsvd {
		reserved[n] = struct{}{}
	}
}

// DosaNamingRule is the error message for invalid names.
const DosaNamingRule = "DOSA valid names must start with a letter or underscore, and may contain letters, " +
	"digits, and underscores, and must not be longer than 32 characters."

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
	return errors.Errorf("%s is a reserved word", name)
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
