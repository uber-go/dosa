// Copyright (c) 2020 Uber Technologies, Inc.
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

import (
	"time"

	// Using a named import is the key change in this file. This is
	// used to test the entity parser against named dosa imports.
	dosav2 "github.com/uber-go/dosa"
)

// TestNamedImportEntity uses common key types and all types in value fields.
type TestNamedImportEntity struct {
	dosav2.Entity `dosa:"name=named_import_entity, primaryKey=(UUIDKey, StrKey ASC, Int64Key DESC)"`
	UUIDKey       dosav2.UUID `dosa:"name=an_uuid_key"`
	StrKey        string
	Int64Key      int64
	UUIDV         dosav2.UUID
	StrV          string
	Int64V        int64 `dosa:"name=an_int64_value"`
	Int32V        int32
	DoubleV       float64
	BoolV         bool
	BlobV         []byte
	TSV           time.Time
}
