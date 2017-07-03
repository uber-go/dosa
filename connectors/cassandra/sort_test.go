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

package cassandra

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
)

func TestSortedField(t *testing.T) {
	a := []string{"l", "z", "21", "a"}
	sort.Strings(a)
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
