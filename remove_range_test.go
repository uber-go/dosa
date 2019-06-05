// Copyright (c) 2019 Uber Technologies, Inc.
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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type removeRangeTestCase struct {
	desc          string
	op            *RemoveRangeOp
	expectedConds map[string][]*Condition
}

var removeRangeTestCases = []removeRangeTestCase{
	{
		desc: "Equal",
		op:   NewRemoveRangeOp(&AllTypes{}).Eq("StringType", "hello"),
		expectedConds: map[string][]*Condition{
			"StringType": {{Op: Eq, Value: "hello"}},
		},
	},
	{
		desc: "Less than and Greater Than",
		op:   NewRemoveRangeOp(&AllTypes{}).Lt("Int32Type", int32(4)).Gt("Int32Type", int32(1)),
		expectedConds: map[string][]*Condition{
			"Int32Type": {{Op: Lt, Value: int32(4)}, {Op: Gt, Value: int32(1)}},
		},
	},
	{
		desc: "Less than or equal to and greater than or equal to",
		op:   NewRemoveRangeOp(&AllTypes{}).LtOrEq("Int32Type", int32(4)).GtOrEq("Int32Type", int32(1)),
		expectedConds: map[string][]*Condition{
			"Int32Type": {{Op: LtOrEq, Value: int32(4)}, {Op: GtOrEq, Value: int32(1)}},
		},
	},
}

func TestRemoveRangeConditions(t *testing.T) {
	for _, test := range removeRangeTestCases {
		t.Run(fmt.Sprintf("Test %s", test.desc), func(t *testing.T) {
			removeRangeTest(t, test)
		})
	}
}

func removeRangeTest(t *testing.T, test removeRangeTestCase) {
	assert.Equal(t, len(test.expectedConds), len(test.op.conditions))
	for col, expConds := range test.expectedConds {
		conds := test.op.conditions[col]
		assert.Equal(t, len(expConds), len(conds))
		for i, expCond := range expConds {
			assert.Equal(t, expCond, conds[i])
		}
	}
}
