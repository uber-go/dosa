// Copyright (c) 2020 Uber Technologies, Inc.
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

func TestConvertConditions(t *testing.T) {
	rangeTestCases := []struct {
		descript  string
		rop       *RangeOp
		converted string
		err       string
	}{
		{
			descript:  "empty rangeop, valid",
			rop:       NewRangeOp(&AllTypes{}),
			converted: "()",
		},
		{
			descript:  "single string, valid",
			rop:       NewRangeOp(&AllTypes{}).Eq("StringType", "word"),
			converted: "(stringtype == word)",
		},
		{
			descript: "bad field name, invalid",
			rop:      NewRangeOp(&AllTypes{}).Eq("badfield", "data"),
			err:      "badfield",
		},
		{
			descript: "numeric in string field, invalid",
			rop:      NewRangeOp(&AllTypes{}).Gt("StringType", 1),
			err:      "invalid value for string",
		},
		{
			descript:  "two conditions, valid",
			rop:       NewRangeOp(&AllTypes{}).GtOrEq("Int32Type", int32(5)).LtOrEq("Int32Type", int32(10)),
			converted: "((int32type <= 10) && (int32type >= 5))",
		},
		{
			descript:  "empty with limit",
			rop:       NewRangeOp(&AllTypes{}).Limit(10),
			converted: "()",
		},
		{
			descript:  "empty with adaptive limit",
			rop:       NewRangeOp(&AllTypes{}).Limit(AdaptiveRangeLimit),
			converted: "()",
		},
		{
			descript:  "empty with token",
			rop:       NewRangeOp(&AllTypes{}).Offset("toketoketoke"),
			converted: "()",
		},
		{
			descript: "error in one field",
			rop:      NewRangeOp(&AllTypes{}).Lt("badfieldpropogate", "oopsie").Lt("StringType", "42").Limit(10),
			err:      "badfieldpropogate",
		},
		{
			descript:  "valid, mixed types",
			rop:       NewRangeOp(&AllTypes{}).Eq("stringtype", "word").Eq("int32type", int32(-1)),
			converted: "((int32type == -1) && (stringtype == word))",
		},
		{
			descript:  "with valid field list",
			rop:       NewRangeOp(&AllTypes{}).Fields([]string{"StringType"}),
			converted: "()",
		},
	}

	alltypesTable, _ := TableFromInstance((*AllTypes)(nil))
	for _, test := range rangeTestCases {
		result, err := ConvertConditions(test.rop.conditions, alltypesTable)
		if err != nil {
			assert.Contains(t, err.Error(), test.err, test.descript)
		} else {
			if assert.NoError(t, err) {
				assert.Equal(t, test.converted, ConditionsString(result), test.descript)
			}
		}
	}
}
