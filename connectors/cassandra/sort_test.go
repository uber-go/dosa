package cassandra

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
)

func TestSortedField(t *testing.T) {
	a := []string{"l", "z", "21", "a"}
	sort.Sort(sortedFields(a))
	assert.Equal(t, a, []string{"21", "a", "l", "z"})
}

func TestSortedConditions(t *testing.T) {
	conds := map[string][]*dosa.Condition{
		"b": {{Op: dosa.Eq, Value: 9}},
		"c": {{Op: dosa.Gt, Value: 0}, {Op: dosa.Lt, Value: 1}},
		"a": {
			{Op: dosa.Gt, Value: 0},
			{Op: dosa.LtOrEq, Value: 2},
			{Op: dosa.GtOrEq, Value: 3},
			{Op: dosa.Lt, Value: 5},
			{Op: dosa.Eq, Value: 4},
		},
	}

	expected := []*ColumnCondition{
		{
			Name:      "a",
			Condition: &dosa.Condition{Op: dosa.Eq, Value: 4},
		},
		{
			Name:      "a",
			Condition: &dosa.Condition{Op: dosa.Lt, Value: 5},
		},
		{
			Name:      "a",
			Condition: &dosa.Condition{Op: dosa.LtOrEq, Value: 2},
		},
		{
			Name:      "a",
			Condition: &dosa.Condition{Op: dosa.Gt, Value: 0},
		},
		{
			Name:      "a",
			Condition: &dosa.Condition{Op: dosa.GtOrEq, Value: 3},
		},
		{
			Name:      "b",
			Condition: &dosa.Condition{Op: dosa.Eq, Value: 9},
		},
		{
			Name:      "c",
			Condition: &dosa.Condition{Op: dosa.Lt, Value: 1},
		},
		{
			Name:      "c",
			Condition: &dosa.Condition{Op: dosa.Gt, Value: 0},
		},
	}

	cc, _, _ := prepareConditions(conds)
	for i, c := range cc {
		assert.Equal(t, c.Name, expected[i].Name)
		assert.Equal(t, c.Condition, expected[i].Condition)
	}
}
