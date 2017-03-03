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

func TestNewRangeOp(t *testing.T) {
	assert.NotNil(t, NewRangeOp(&AllTypes{}))
}

func TestRangeOpStringer(t *testing.T) {
	tests := []struct {
		rop      *RangeOp
		expected string
		err      string
	}{
		{
			rop:      NewRangeOp(&AllTypes{}),
			expected: "<empty>",
		},
		{
			rop:      NewRangeOp(&AllTypes{}).Eq("StringType", "word"),
			expected: "StringType Eq word",
		},
		{
			rop: NewRangeOp(&AllTypes{}).Eq("badfield", "data"),
			err: "badfield",
		},
		{
			rop: NewRangeOp(&AllTypes{}).Gt("StringType", 1),
			err: "StringType: invalid value",
		},
		{
			rop:      NewRangeOp(&AllTypes{}).GtOrEq("Int32Type", int32(5)).LtOrEq("Int32Type", int32(10)),
			expected: "Int32Type GtOrEq 5, Int32Type LtOrEq 10",
		},
		{
			rop:      NewRangeOp(&AllTypes{}).Limit(10),
			expected: "<empty> limit 10",
		},
		{
			rop:      NewRangeOp(&AllTypes{}).Offset("toketoketoke"),
			expected: "<empty> token \"toketoketoke\"",
		},
		{
			rop: NewRangeOp(&AllTypes{}).Lt("badfieldpropogate", "oopsie").Lt("StringType", "42").Limit(10),
			err: "badfieldpropogate",
		},
		{
			rop:      NewRangeOp(&AllTypes{}).Eq("StringType", "word").Eq("Int32Type", int32(-1)),
			expected: "StringType Eq word, Int32Type Eq -1",
		},
		{
			// TODO: We could do better here, outputting the field list
			rop:      NewRangeOp(&AllTypes{}).Fields([]string{"StringType"}),
			expected: "<empty>",
		},
	}
	for _, test := range tests {
		if test.err != "" {
			assert.Contains(t, test.rop.pendingError.Error(), test.err)
		} else {
			assert.NoError(t, test.rop.pendingError)
			assert.Equal(t, test.expected, test.rop.String())
		}
	}
}
