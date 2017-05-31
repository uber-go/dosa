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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNullInt(t *testing.T) {
	v := NewNullInt64(10)
	actual, err := v.Get()
	assert.NoError(t, err)
	assert.Equal(t, int64(10), actual)

	v.Set(20)
	actual, err = v.Get()
	assert.NoError(t, err)
	assert.Equal(t, int64(20), actual)

	v.Nullify()
	actual, err = v.Get()
	assert.Error(t, err, "Value is null")
}

func TestNullInt_MarshalText(t *testing.T) {
	v := NewNullInt64(10)
	bytes, err := v.MarshalText()
	assert.NoError(t, err)
	assert.Equal(t, "10", string(bytes))

	v.Nullify()
	bytes, err = v.MarshalText()
	assert.NoError(t, err)
	assert.Equal(t, "", string(bytes))
}

func TestNullInt_UnmarshalText(t *testing.T) {
	var v NullInt64
	err := v.UnmarshalText([]byte("10"))
	assert.NoError(t, err)

	value, err := v.Get()
	assert.NoError(t, err)
	assert.Equal(t, int64(10), value)

	err = v.UnmarshalText([]byte(""))
	assert.NoError(t, err)
	_, err = v.Get()
	assert.Equal(t, ErrNullValue, err)

	err = v.UnmarshalText([]byte("null"))
	assert.NoError(t, err)
	_, err = v.Get()
	assert.Equal(t, ErrNullValue, err)
}

func TestNullInt_MarshalJSON(t *testing.T) {
	v := NewNullInt64(-123)
	bytes, err := v.MarshalJSON()
	assert.NoError(t, err)
	assert.Equal(t, "-123", string(bytes))

	v.Nullify()
	bytes, err = v.MarshalJSON()
	assert.NoError(t, err)
	assert.Equal(t, "null", string(bytes))
}

func TestNullInt_UnmarshalJSON(t *testing.T) {
	var v NullInt64
	err := v.UnmarshalJSON([]byte("-12"))
	assert.NoError(t, err)
	value, err := v.Get()
	assert.NoError(t, err)
	assert.Equal(t, int64(-12), value)

	err = v.UnmarshalJSON([]byte("null"))
	assert.NoError(t, err)
	value, err = v.Get()
	assert.EqualError(t, err, "Value is null")

	// Invalid case
	err = v.UnmarshalJSON([]byte("true"))
	assert.Error(t, err)
}
