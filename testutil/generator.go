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

package testutil

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
)

// TestInt64Ptr create pointer for int64
func TestInt64Ptr(i int64) *int64 {
	return &i
}

// TestInt32Ptr create pointer for int32
func TestInt32Ptr(i int32) *int32 {
	return &i
}

// TestTimePtr create pointer for time.Time
func TestTimePtr(t time.Time) *time.Time {
	return &t
}

// TestFloat64Ptr create pointer for float64
func TestFloat64Ptr(f float64) *float64 {
	return &f
}

// TestStringPtr create pointer for string
func TestStringPtr(s string) *string {
	return &s
}

// TestBoolPtr create pointer for bool
func TestBoolPtr(b bool) *bool {
	return &b
}

// TestUUIDPtr create pointer for dosa.UUID
func TestUUIDPtr(b dosa.UUID) *dosa.UUID {
	return &b
}

// AssertEqForPointer compares equal between interface of pointer
func AssertEqForPointer(t *testing.T, expected interface{}, p interface{}) {
	switch v := p.(type) {
	case *int32:
		assert.Equal(t, *v, expected)
	case *int64:
		assert.Equal(t, *v, expected)
	case *float64:
		assert.Equal(t, *v, expected)
	case *string:
		assert.Equal(t, *v, expected)
	case *dosa.UUID:
		assert.Equal(t, *v, expected)
	case *time.Time:
		assert.Equal(t, *v, expected)
	case *bool:
		assert.Equal(t, *v, expected)
	case *[]byte:
		assert.Equal(t, *v, expected)
	default:
		assert.Fail(t, fmt.Sprintf("invalid type %v", reflect.TypeOf(v)))
	}
}
