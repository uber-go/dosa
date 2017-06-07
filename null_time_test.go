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
	"time"
)

func TestNullTime(t *testing.T) {
	now := time.Now()
	v := NewNullTime(now)
	actual, err := v.Get()
	assert.NoError(t, err)
	assert.Equal(t, now.Unix(), actual.Unix())

	v.Set(time.Unix(0, 0))
	actual, err = v.Get()
	assert.NoError(t, err)
	assert.Equal(t, time.Unix(0, 0).Unix(), actual.Unix())

	v.Nullify()
	actual, err = v.Get()
	assert.Error(t, err, "Value is null")
}

func TestNullTime_MarshalText(t *testing.T) {
	now := time.Now()
	expected, err := now.MarshalText()
	v := NewNullTime(now)
	bytes, err := v.MarshalText()
	assert.NoError(t, err)
	assert.Equal(t, string(expected), string(bytes))

	v.Nullify()
	bytes, err = v.MarshalText()
	assert.NoError(t, err)
	assert.Equal(t, "null", string(bytes))
}

func TestNullTime_UnmarshalText(t *testing.T) {
	now := time.Now()
	timeText, err := now.MarshalText()
	var v NullTime
	err = v.UnmarshalText(timeText)
	assert.NoError(t, err)

	value, err := v.Get()
	assert.NoError(t, err)
	assert.Equal(t, now.Unix(), value.Unix())

	err = v.UnmarshalText([]byte(""))
	assert.NoError(t, err)
	_, err = v.Get()
	assert.Equal(t, ErrNullValue, err)

	// Invalid case.
	err = v.UnmarshalText([]byte("bad time"))
	assert.Error(t, err)
}

func TestNullTime_MarshalJSON(t *testing.T) {
	now := time.Now()
	v := NewNullTime(now)
	bytes, err := v.MarshalJSON()
	assert.NoError(t, err)
	expected, err := now.MarshalJSON()
	assert.NoError(t, err)
	assert.Equal(t, string(expected), string(bytes))

	v.Nullify()
	bytes, err = v.MarshalJSON()
	assert.NoError(t, err)
	assert.Equal(t, "null", string(bytes))
}

func TestNullTime_UnmarshalJSON(t *testing.T) {
	now := time.Now()
	var v NullTime
	timeJson, err := now.MarshalJSON()
	err = v.UnmarshalJSON(timeJson)
	assert.NoError(t, err)

	value, err := v.Get()
	assert.NoError(t, err)
	assert.Equal(t, now.Unix(), value.Unix())

	err = v.UnmarshalJSON([]byte("null"))
	assert.NoError(t, err)
	value, err = v.Get()
	assert.EqualError(t, err, "Value is null")

	// Invalid json case
	err = v.UnmarshalJSON([]byte("\"abc\""))
	assert.Error(t, err)

	// Invalid case
	badData, err := json.Marshal(float64(3.14))
	err = v.UnmarshalJSON(badData)
	assert.Error(t, err)
}
