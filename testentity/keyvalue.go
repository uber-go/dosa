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

package testentity

import "github.com/uber-go/dosa"

// KeyValue represents a key-value schema
type KeyValue struct {
	dosa.Entity `dosa:"name=awesome_test_entity, primaryKey=(K)"`
	K           []byte
	V           []byte
}

// ValueKey represents a key-value schema listing value first
type ValueKey struct {
	dosa.Entity `dosa:"name=awesome_test_entity, primaryKey=(K)"`
	V           []byte
	K           []byte
}

// KeyValues represents a key to multiple value schema
type KeyValues struct {
	dosa.Entity `dosa:"name=awesome_test_entity, primaryKey=(K)"`
	K           []byte
	V1          []byte
	V2          []byte
}

// KeyValueNonByte represents a key-value schema with non byte value
type KeyValueNonByte struct {
	dosa.Entity `dosa:"name=awesome_test_entity, primaryKey=(K)"`
	K           []byte
	V           string
}
