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

	"encoding/json"

	"github.com/stretchr/testify/assert"
)

func TestNullFloat(t *testing.T) {
	v := NewNullFloat64(1e9)
	actual, err := v.Get()
	assert.NoError(t, err)
	assert.Equal(t, float64(1e9), actual)

	v.Set(3.14)
	actual, err = v.Get()
	assert.NoError(t, err)
	assert.Equal(t, float64(3.14), actual)

	v.Nullify()
	actual, err = v.Get()
	assert.Error(t, err, "Value is null")
}

func TestNullFloat_MarshalText(t *testing.T) {
	v := NewNullFloat64(3.14)
	bytes, err := v.MarshalText()
	assert.NoError(t, err)
	assert.Equal(t, "3.14", string(bytes))

	v.Nullify()
	bytes, err = v.MarshalText()
	assert.NoError(t, err)
	assert.Equal(t, "", string(bytes))
}

func TestNullFloat_UnmarshalText(t *testing.T) {
	var v NullFloat64
	err := v.UnmarshalText([]byte("10.321"))
	assert.NoError(t, err)

	value, err := v.Get()
	assert.NoError(t, err)
	assert.Equal(t, float64(10.321), value)

	err = v.UnmarshalText([]byte(""))
	assert.NoError(t, err)
	_, err = v.Get()
	assert.Equal(t, ErrNullValue, err)

	err = v.UnmarshalText([]byte("null"))
	assert.NoError(t, err)
	_, err = v.Get()
	assert.Equal(t, ErrNullValue, err)
}

func TestNullFloat_MarshalJSON(t *testing.T) {
	v := NewNullFloat64(-123.5)
	bytes, err := v.MarshalJSON()
	assert.NoError(t, err)
	assert.Equal(t, "-123.5", string(bytes))

	v.Nullify()
	bytes, err = v.MarshalJSON()
	assert.NoError(t, err)
	assert.Equal(t, "null", string(bytes))
}

func TestNullFloat_UnmarshalJSON(t *testing.T) {
	var v NullFloat64
	err := v.UnmarshalJSON([]byte("-12.5"))
	assert.NoError(t, err)
	value, err := v.Get()
	assert.NoError(t, err)
	assert.Equal(t, float64(-12.5), value)

	err = v.UnmarshalJSON([]byte("null"))
	assert.NoError(t, err)
	value, err = v.Get()
	assert.EqualError(t, err, "Value is null")

	// Invalid case
	badData, err := json.Marshal("true")
	err = v.UnmarshalJSON(badData)
	assert.Error(t, err)
}
