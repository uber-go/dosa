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

package dosa_test

import (
	"testing"

	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
)

// pk = ((a, b), c, d, e)
var testEntityRange = &dosa.EntityDefinition{
	Name: "testentityrange",
	Key: &dosa.PrimaryKey{
		PartitionKeys: []string{"a", "b"},
		ClusteringKeys: []*dosa.ClusteringKey{
			{
				Name:       "c",
				Descending: true,
			},
			{
				Name:       "d",
				Descending: true,
			},
			{
				Name:       "e",
				Descending: true,
			},
		},
	},
	Columns: []*dosa.ColumnDefinition{
		{
			Name: "a",
			Type: dosa.TUUID,
		},

		{
			Name: "c",
			Type: dosa.Int32,
		},
		{
			Name: "e",
			Type: dosa.String,
		},
		{
			Name: "d",
			Type: dosa.Timestamp,
		},
		{
			Name: "b",
			Type: dosa.Int64,
		},
		{
			Name: "f",
			Type: dosa.Blob,
		},
	},
}

var columnToFieldMap = map[string]string{
	"a": "FieldA",
	"b": "FieldB",
	"c": "FieldC",
	"d": "FieldD",
	"e": "FieldE",
	"f": "FieldF",
}

var simpleTransformer = func(x string) string {
	return columnToFieldMap[x]
}

func TestEnsureValidRangeConditions(t *testing.T) {
	assert.NoError(t, testEntityRange.EnsureValid()) // sanity check

	type validCase struct {
		conds map[string][]*dosa.Condition
		desc  string
	}
	validCases := []validCase{
		{
			conds: map[string][]*dosa.Condition{
				"a": {{dosa.Eq, dosa.UUID("66DF78EB-C41D-48EF-B366-0C7F91C5CE43")}},
				"b": {{dosa.Eq, int64(100)}},
			},
			desc: "supply only partition keys is allowed, no conditions on clustering keys",
		},
		{
			conds: map[string][]*dosa.Condition{
				"a": {{dosa.Eq, dosa.UUID("66DF78EB-C41D-48EF-B366-0C7F91C5CE43")}},
				"b": {{dosa.Eq, int64(100)}},
				"c": {{dosa.Eq, int32(99)}},
			},
			desc: "eq condition on first clustering key, no condition on second and third",
		},
		{
			conds: map[string][]*dosa.Condition{
				"a": {{dosa.Eq, dosa.UUID("66DF78EB-C41D-48EF-B366-0C7F91C5CE43")}},
				"b": {{dosa.Eq, int64(100)}},
				"c": {{dosa.GtOrEq, int32(99)}, {dosa.Lt, int32(200)}},
			},
			desc: "close range condition on first clustering key, no condition on second and third",
		},
		{
			conds: map[string][]*dosa.Condition{
				"a": {{dosa.Eq, dosa.UUID("66DF78EB-C41D-48EF-B366-0C7F91C5CE43")}},
				"b": {{dosa.Eq, int64(100)}},
				"c": {{dosa.Eq, int32(99)}},
				"d": {{dosa.LtOrEq, time.Now()}},
			},
			desc: "open range condition on second clustering key, no restaint on third",
		},
		{
			conds: map[string][]*dosa.Condition{
				"a": {{dosa.Eq, dosa.UUID("66DF78EB-C41D-48EF-B366-0C7F91C5CE43")}},
				"b": {{dosa.Eq, int64(100)}},
				"c": {{dosa.Eq, int32(99)}},
				"d": {{dosa.Eq, time.Unix(100, 0)}},
			},
			desc: "eq condition on second clustering key, no restaint on third",
		},
		{
			conds: map[string][]*dosa.Condition{
				"a": {{dosa.Eq, dosa.UUID("66DF78EB-C41D-48EF-B366-0C7F91C5CE43")}},
				"b": {{dosa.Eq, int64(100)}},
				"c": {{dosa.Eq, int32(99)}},
				"d": {{dosa.Eq, time.Unix(100, 0)}},
				"e": {{dosa.Gt, "aaa"}, {dosa.Lt, "zzz"}},
			},
			desc: "close range condition on third/last clustering key with < and >",
		},
		{
			conds: map[string][]*dosa.Condition{
				"a": {{dosa.Eq, dosa.UUID("66DF78EB-C41D-48EF-B366-0C7F91C5CE43")}},
				"b": {{dosa.Eq, int64(100)}},
				"c": {{dosa.Eq, int32(99)}},
				"d": {{dosa.Eq, time.Unix(100, 0)}},
				"e": {{dosa.GtOrEq, "aaa"}, {dosa.LtOrEq, "zzz"}},
			},
			desc: "close range condition on third/last clustering key with <= and >=",
		},
		{
			conds: map[string][]*dosa.Condition{
				"a": {{dosa.Eq, dosa.UUID("66DF78EB-C41D-48EF-B366-0C7F91C5CE43")}},
				"b": {{dosa.Eq, int64(100)}},
				"c": {{dosa.Eq, int32(99)}},
				"d": {{dosa.Eq, time.Unix(100, 0)}},
				"e": {{dosa.GtOrEq, "aaa"}, {dosa.Lt, "zzz"}},
			},
			desc: "close range condition on third/last clustering key with < and >=",
		},
		{
			conds: map[string][]*dosa.Condition{
				"a": {{dosa.Eq, dosa.UUID("66DF78EB-C41D-48EF-B366-0C7F91C5CE43")}},
				"b": {{dosa.Eq, int64(100)}},
				"c": {{dosa.Eq, int32(99)}},
				"d": {{dosa.Eq, time.Unix(100, 0)}},
				"e": {{dosa.Gt, "aaa"}, {dosa.LtOrEq, "zzz"}},
			},
			desc: "close range condition on third/last clustering key with <= and >",
		},
	}

	type invalidCase struct {
		conds    map[string][]*dosa.Condition
		desc     string
		errMsg   string
		errField string
	}

	invalidCases := []invalidCase{
		{
			conds: map[string][]*dosa.Condition{
				"a": {{dosa.Eq, dosa.UUID("66DF78EB-C41D-48EF-B366-0C7F91C5CE43")}},
				"b": {{dosa.Eq, int64(100)}},
				"f": {{dosa.Eq, []byte{1, 2, 3}}},
			},
			errMsg:   "cannot enforce condition on non-key column",
			desc:     "conditions on non-key column",
			errField: columnToFieldMap["f"],
		},
		{
			conds: map[string][]*dosa.Condition{
				"a": {{dosa.Eq, dosa.UUID("66DF78EB-C41D-48EF-B366-0C7F91C5CE43")}},
				"c": {{dosa.Gt, int32(100)}},
			},
			errMsg:   "missing Eq condition on partition keys",
			desc:     "missing partition key condition",
			errField: columnToFieldMap["b"],
		},
		{
			conds: map[string][]*dosa.Condition{
				"a": {{dosa.Eq, dosa.UUID("66DF78EB-C41D-48EF-B366-0C7F91C5CE43")}},
				"b": {{dosa.Gt, int64(100)}},
			},
			errMsg:   "invalid conditions for partition key",
			desc:     "Gt condition on partition key",
			errField: columnToFieldMap["b"],
		},
		{
			conds: map[string][]*dosa.Condition{
				"a": {{dosa.Eq, dosa.UUID("66DF78EB-C41D-48EF-B366-0C7F91C5CE43")}},
				"b": {{dosa.Eq, int64(100)}, {dosa.Eq, int64(200)}},
			},
			errMsg:   "invalid conditions for partition key",
			desc:     "more than one conditions on partition key",
			errField: columnToFieldMap["b"],
		},
		{
			conds: map[string][]*dosa.Condition{
				"a": {{dosa.Eq, dosa.UUID("66DF78EB-C41D-48EF-B366-0C7F91C5CE43")}},
				"b": {{dosa.Eq, "100"}},
			},
			errMsg:   "does not have expected type",
			desc:     "wrong value type for partition key",
			errField: columnToFieldMap["b"],
		},
		{
			conds: map[string][]*dosa.Condition{
				"a": {{dosa.Eq, dosa.UUID("66DF78EB-C41D-48EF-B366-0C7F91C5CE43")}},
				"b": {{dosa.Eq, int64(100)}},
				"c": {{dosa.Eq, "100"}},
			},
			errMsg:   "invalid value for",
			desc:     "wrong value type for clustering key",
			errField: columnToFieldMap["c"],
		},
		{
			conds: map[string][]*dosa.Condition{
				"a": {{dosa.Eq, dosa.UUID("66DF78EB-C41D-48EF-B366-0C7F91C5CE43")}},
				"b": {{dosa.Eq, int64(100)}},
				"c": {{dosa.Lt, int32(100)}, {dosa.Gt, int32(200)}},
			},
			errMsg:   "invalid or unsupported conditions for clustering key",
			desc:     "invalid range condition on clustering key",
			errField: columnToFieldMap["c"],
		},
		{
			conds: map[string][]*dosa.Condition{
				"a": {{dosa.Eq, dosa.UUID("66DF78EB-C41D-48EF-B366-0C7F91C5CE43")}},
				"b": {{dosa.Eq, int64(100)}},
				"c": {{dosa.Lt, int32(100)}, {dosa.Gt, int32(200)}},
				"d": {{dosa.GtOrEq, time.Now()}},
			},
			errMsg:   "exact one Eq condition can be applied except for the last",
			desc:     "applying conditions other than eq to clustering keys that's not the last retrained",
			errField: columnToFieldMap["c"],
		},
		{
			conds: map[string][]*dosa.Condition{
				"a": {{dosa.Eq, dosa.UUID("66DF78EB-C41D-48EF-B366-0C7F91C5CE43")}},
				"b": {{dosa.Eq, int64(100)}},
				"c": {{dosa.Lt, int32(100)}, {dosa.Gt, int32(200)}},
				"d": {{dosa.Eq, time.Unix(100, 0)}},
				"e": {{dosa.Eq, "aaa"}},
			},
			errMsg: "exact one Eq condition can be applied except for the last",
			desc: "applying conditions other than eq to clustering keys that's not the last retrained, " +
				"different last retained clusterin key",
			errField: columnToFieldMap["c"],
		},
		{
			conds: map[string][]*dosa.Condition{
				"a": {{dosa.Eq, dosa.UUID("66DF78EB-C41D-48EF-B366-0C7F91C5CE43")}},
				"b": {{dosa.Eq, int64(100)}},
				"c": {{dosa.Eq, int32(100)}},
				"e": {{dosa.Eq, "aaa"}},
			},
			errMsg:   "conditions must be applied consecutively on clustering keys",
			desc:     "applying conditions non-consecutively to clustering keys",
			errField: columnToFieldMap["e"],
		},
	}

	for _, c := range validCases {
		assert.NoError(t, dosa.EnsureValidRangeConditions(testEntityRange, c.conds, simpleTransformer), c.desc)
	}

	for _, c := range invalidCases {
		err := dosa.EnsureValidRangeConditions(testEntityRange, c.conds, simpleTransformer)
		if assert.Error(t, err, c.desc) {
			assert.Contains(t, err.Error(), c.errMsg, c.desc)
			assert.Contains(t, err.Error(), c.errField, c.desc)
		}
	}
}
