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

func TestNullString(t *testing.T) {
	v := NewNullString("abc")
	actual, err := v.Get()
	assert.NoError(t, err)
	assert.Equal(t, "abc", actual)

	v.Set("def")
	actual, err = v.Get()
	assert.NoError(t, err)
	assert.Equal(t, "def", actual)

	v.Nullify()
	actual, err = v.Get()
	assert.Error(t, err, "Value is null")
}

func TestNullString_MarshalText(t *testing.T) {
	v := NewNullString("a b c")
	bytes, err := v.MarshalText()
	assert.NoError(t, err)
	assert.Equal(t, "a b c", string(bytes))

	v = NewNullString("whatever")
	bytes, err = v.MarshalText()
	assert.NoError(t, err)
	assert.Equal(t, "whatever", string(bytes))

	v.Nullify()
	bytes, err = v.MarshalText()
	assert.NoError(t, err)
	assert.Equal(t, "", string(bytes))
}

func TestNullString_UnmarshalText(t *testing.T) {
	var v NullString
	err := v.UnmarshalText([]byte("abc"))
	assert.NoError(t, err)

	value, err := v.Get()
	assert.NoError(t, err)
	assert.Equal(t, "abc", value)

	err = v.UnmarshalText([]byte("def"))
	assert.NoError(t, err)

	value, err = v.Get()
	assert.NoError(t, err)
	assert.Equal(t, "def", value)

	err = v.UnmarshalText([]byte(""))
	assert.NoError(t, err)
	_, err = v.Get()
	assert.Equal(t, ErrNullValue, err)

	err = v.UnmarshalText([]byte(""))
	assert.NoError(t, err)
	_, err = v.Get()
	assert.Equal(t, ErrNullValue, err)
}

func TestNullString_MarshalJSON(t *testing.T) {
	v := NewNullString("sometype")
	bytes, err := v.MarshalJSON()
	assert.NoError(t, err)
	assert.Equal(t, "\"sometype\"", string(bytes))

	v.Set("abc")
	bytes, err = v.MarshalJSON()
	assert.NoError(t, err)
	assert.Equal(t, "\"abc\"", string(bytes))

	v.Nullify()
	bytes, err = v.MarshalJSON()
	assert.NoError(t, err)
	assert.Equal(t, "null", string(bytes))
}

func TestNullString_UnmarshalJSON(t *testing.T) {
	var v NullString
	err := v.UnmarshalJSON([]byte("\"true\""))
	assert.NoError(t, err)

	value, err := v.Get()
	assert.NoError(t, err)
	assert.Equal(t, "true", value)

	err = v.UnmarshalJSON([]byte("null"))
	assert.NoError(t, err)
	value, err = v.Get()
	assert.EqualError(t, err, "Value is null")

	// Invalid case
	badData, err := json.Marshal(float64(3.14))
	err = v.UnmarshalJSON(badData)
	assert.Error(t, err)
}
