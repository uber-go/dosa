package dosa

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvertConditions(t *testing.T) {
	alltypesTable, _ := TableFromInstance((*AllTypes)(nil))
	for _, test := range rangeTestCases {
		result, err := convertConditions(test.rop.conditions, alltypesTable)
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
