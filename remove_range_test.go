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
