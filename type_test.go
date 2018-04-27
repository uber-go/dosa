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

func TestNewUUID(t *testing.T) {
	id := NewUUID()
	assert.NotNil(t, id)
	assert.NotEqual(t, string(id), "")
}

func TestUUIDToBytes(t *testing.T) {
	id := NewUUID()
	bs, err := id.Bytes()
	assert.NoError(t, err)
	assert.Len(t, bs, 16)

	invalidUUID := UUID("xyz-1827380-kdwi-4829")
	_, err = invalidUUID.Bytes()
	assert.Error(t, err)
}

func TestBytesToUUID(t *testing.T) {
	id := NewUUID()
	bs, err0 := id.Bytes()
	assert.NoError(t, err0)
	uid, err := BytesToUUID(bs)
	assert.NoError(t, err)
	assert.EqualValues(t, id, uid)

	invalidBs := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	_, err = BytesToUUID(invalidBs)
	assert.Error(t, err)
}

func TestFromString(t *testing.T) {
	tt := []struct {
		input    string
		expected Type
	}{
		{
			input:    TUUID.String(),
			expected: TUUID,
		},
		{
			input:    String.String(),
			expected: String,
		},
		{
			input:    Int32.String(),
			expected: Int32,
		},
		{
			input:    Int64.String(),
			expected: Int64,
		},
		{
			input:    Double.String(),
			expected: Double,
		},
		{
			input:    Blob.String(),
			expected: Blob,
		},
		{
			input:    Timestamp.String(),
			expected: Timestamp,
		},
		{
			input:    Bool.String(),
			expected: Bool,
		},
		{
			input:    "invalid",
			expected: Invalid,
		},
	}

	for _, tc := range tt {
		assert.Equal(t, FromString(tc.input), tc.expected)
	}
}
