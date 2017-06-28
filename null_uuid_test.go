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

	"fmt"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

var testUUID = uuid.NewV4().String()

func TestNullUUID(t *testing.T) {
	v := NewNullUUID()
	actual, err := v.Get()
	assert.NoError(t, err)
	assert.Equal(t, len(testUUID), len(actual))

	// def isn't a valid UUID.
	err = v.Set("def")
	assert.Error(t, err, "invalid uuid string")

	err = v.Set(testUUID)
	assert.NoError(t, err)

	actual, err = v.Get()
	assert.NoError(t, err)
	assert.Equal(t, testUUID, actual)

	v.Nullify()
	actual, err = v.Get()
	assert.Error(t, err, "Value is null")
}

func TestNullUUID_MarshalText(t *testing.T) {
	v, err := NullUUIDFromString(testUUID)
	assert.NoError(t, err)

	bytes, err := v.MarshalText()
	assert.NoError(t, err)
	assert.Equal(t, testUUID, string(bytes))

	v.Nullify()
	bytes, err = v.MarshalText()
	assert.NoError(t, err)
	assert.Equal(t, "", string(bytes))
}

func TestNullUUID_UnmarshalText(t *testing.T) {
	var v NullUUID
	err := v.UnmarshalText([]byte(testUUID))
	assert.NoError(t, err)

	value, err := v.Get()
	assert.NoError(t, err)
	assert.Equal(t, testUUID, value)

	err = v.UnmarshalText([]byte(""))
	assert.NoError(t, err)
	_, err = v.Get()
	assert.Equal(t, ErrNullValue, err)

	err = v.UnmarshalText([]byte(""))
	assert.NoError(t, err)
	_, err = v.Get()
	assert.Equal(t, ErrNullValue, err)
}

func TestNullUUID_MarshalJSON(t *testing.T) {
	v, err := NullUUIDFromString(testUUID)
	assert.NoError(t, err)

	bytes, err := v.MarshalJSON()
	assert.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("%q", testUUID), string(bytes))

	tmp := uuid.NewV4().String()
	v.Set(tmp)
	bytes, err = v.MarshalJSON()
	assert.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("%q", tmp), string(bytes))

	v.Nullify()
	bytes, err = v.MarshalJSON()
	assert.NoError(t, err)
	assert.Equal(t, "null", string(bytes))
}

func TestNullUUID_UnmarshalJSON(t *testing.T) {
	var v NullUUID
	err := v.UnmarshalJSON([]byte(fmt.Sprintf("%q", testUUID)))
	assert.NoError(t, err)

	value, err := v.Get()
	assert.NoError(t, err)
	assert.Equal(t, testUUID, value)

	err = v.UnmarshalJSON([]byte("null"))
	assert.NoError(t, err)
	value, err = v.Get()
	assert.EqualError(t, err, "Value is null")

	// Invalid case
	badData, err := json.Marshal(float64(3.14))
	err = v.UnmarshalJSON(badData)
	assert.Error(t, err)

	// Another invalid case
	badData, err = json.Marshal(true)
	err = v.UnmarshalJSON(badData)
	assert.Error(t, err)

	// One more
	badData, err = json.Marshal("a-b-c-d")
	err = v.UnmarshalJSON(badData)
	assert.Error(t, err)
}
