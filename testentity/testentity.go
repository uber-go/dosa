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

package testentity

import (
	"time"

	"github.com/uber-go/dosa"
)

// TestEntity uses common key types and all types in value fields.
type TestEntity struct {
	dosa.Entity `dosa:"name=awesome_test_entity, primaryKey=(UUIDKey, StrKey ASC, Int64Key DESC)"`
	UUIDKey     dosa.UUID `dosa:"name=an_uuid_key"`
	StrKey      string
	Int64Key    int64
	UUIDV       dosa.UUID
	StrV        string
	Int64V      int64 `dosa:"name=an_int64_value"`
	Int32V      int32
	DoubleV     float64
	BoolV       bool
	BlobV       []byte
	TSV         time.Time

	UUIDVP   *dosa.UUID
	StrVP    *string
	Int64VP  *int64
	Int32VP  *int32
	DoubleVP *float64
	BoolVP   *bool
	TSVP     *time.Time
}
