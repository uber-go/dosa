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

package cassandra

import (
	"regexp"
)

// ProductionKeyspaceName is the name of the production keyspace
const ProductionKeyspaceName = "production"

// CleanupKeyspaceName "cleans up" a keyspace name by changing unacceptable
// characters into underscores
func CleanupKeyspaceName(ksn string) string {
	// clean up the scope, replacing invalid identifier characters with _
	re := regexp.MustCompile("[^a-zA-Z0-9_]")
	newKsn := re.ReplaceAllLiteralString(ksn, "_")
	// TODO: if len(newScope) > 48 then make a UUID hash
	return newKsn
}

// KeyspaceMapper is an interface that maps scope/namePrefix to a keyspace
type KeyspaceMapper interface {
	Keyspace(scope, namePrefix string) (keyspace string)
}

// UseScope is a simple KeyspaceMapper that always uses the scope
// to determine the keyspace name
type UseScope struct{}

// Keyspace just returns a cleaned-up scope name
func (*UseScope) Keyspace(scope, _ string) string {
	return CleanupKeyspaceName(scope)
}

// UseNamePrefix is a simple keyspace mapper that always uses
// the namePrefix for the schema name
type UseNamePrefix struct{}

// Keyspace returns a cleaned-up copy of the namePrefix
func (*UseNamePrefix) Keyspace(_, namePrefix string) string {
	return CleanupKeyspaceName(namePrefix)
}

// DefaultKeyspaceMapper is the KeyspaceMapper that is used when none is specified
var DefaultKeyspaceMapper = &UseScope{}
