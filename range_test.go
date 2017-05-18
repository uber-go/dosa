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

var RangeTestCases = []struct {
	descript  string
	rop       *RangeOp
	stringer  string
	converted string
	err       string
}{
	{
		descript:  "empty rangeop, valid",
		rop:       NewRangeOp(&AllTypes{}),
		stringer:  "<empty>",
		converted: "<empty>",
	},
	{
		descript:  "single string, valid",
		rop:       NewRangeOp(&AllTypes{}).Eq("StringType", "word"),
		stringer:  "StringType Eq word",
		converted: "stringtype Eq word",
	},
	{
		descript: "bad field name, invalid",
		rop:      NewRangeOp(&AllTypes{}).Eq("badfield", "data"),
		stringer: "badfield Eq data",
		err:      "badfield",
	},
	{
		descript: "numeric in string field, invalid",
		rop:      NewRangeOp(&AllTypes{}).Gt("StringType", 1),
		stringer: "StringType Gt 1",
		err:      "invalid value for string",
	},
	{
		descript:  "two conditions, valid",
		rop:       NewRangeOp(&AllTypes{}).GtOrEq("Int32Type", int32(5)).LtOrEq("Int32Type", int32(10)),
		stringer:  "Int32Type GtOrEq 5, Int32Type LtOrEq 10",
		converted: "int32type GtOrEq 5, int32type LtOrEq 10",
	},
	{
		descript:  "empty with limit",
		rop:       NewRangeOp(&AllTypes{}).Limit(10),
		stringer:  "<empty> limit 10",
		converted: "<empty> limit 10",
	},
	{
		descript:  "empty with token",
		rop:       NewRangeOp(&AllTypes{}).Offset("toketoketoke"),
		stringer:  "<empty> token \"toketoketoke\"",
		converted: "<empty> token \"toketoketoke\"",
	},
	{
		descript: "error in one field",
		rop:      NewRangeOp(&AllTypes{}).Lt("badfieldpropogate", "oopsie").Lt("StringType", "42").Limit(10),
		stringer: "StringType Lt 42, badfieldpropogate Lt oopsie limit 10",
		err:      "badfieldpropogate",
	},
	{
		descript:  "valid, mixed types",
		rop:       NewRangeOp(&AllTypes{}).Eq("StringType", "word").Eq("Int32Type", int32(-1)),
		stringer:  "Int32Type Eq -1, StringType Eq word",
		converted: "int32type Eq -1, stringtype Eq word",
	},
	{
		descript:  "with valid field list",
		rop:       NewRangeOp(&AllTypes{}).Fields([]string{"StringType"}),
		stringer:  "<empty>",
		converted: "<empty>",
	},
}

func TestNewRangeOp(t *testing.T) {
	assert.NotNil(t, NewRangeOp(&AllTypes{}))
}

func TestRangeOpStringer(t *testing.T) {

	for _, test := range RangeTestCases {
		assert.Equal(t, test.stringer, test.rop.String(), test.descript)
	}
}

func TestConvertRangeOpConditions(t *testing.T) {
	alltypesTable, _ := TableFromInstance((*AllTypes)(nil))
	for _, test := range RangeTestCases {
		result, err := convertRangeOpConditions(test.rop, alltypesTable)
		if err != nil {
			assert.Contains(t, err.Error(), test.err, test.descript)
		} else {
			if assert.NoError(t, err) {
				// we don't have a stringify method on just the conditions bit
				// so just build a new RangeOp from the old one
				newRop := *test.rop
				newRop.conditions = result
				final := (&newRop).String()
				assert.Equal(t, test.converted, final, test.descript)
			}
		}
	}
}

func TestRangeOpMatcher(t *testing.T) {
	RangeOp0 := NewRangeOp(&AllTypes{}).Eq("StringType", "Hello")
	RangeOp1 := NewRangeOp(&AllTypes{}).Eq("StringType", "Hello")
	RangeOp2 := NewRangeOp(&AllTypes{}).Lt("StringType", "Hello")
	RangeOp3 := NewRangeOp(&AllTypes{}).Eq("StringType", "Hello").Offset("token1")
	RangeOp4 := NewRangeOp(&AllTypes{}).Eq("StringType", "Hello").Limit(5)

	matcher := EqRangeOp(RangeOp0)
	assert.True(t, matcher.Matches(RangeOp1))
	assert.False(t, matcher.Matches(RangeOp2))
	assert.False(t, matcher.Matches(RangeOp3))
	assert.False(t, matcher.Matches(RangeOp4))
	assert.False(t, matcher.Matches(3))
}
