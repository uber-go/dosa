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
	"bytes"
	"fmt"
	"reflect"
	"sort"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
)

// RangeOp is used to specify constraints to Range calls
type RangeOp struct {
	pager
	conditioner
}

// NewRangeOp returns a new RangeOp instance
func NewRangeOp(object DomainObject) *RangeOp {
	rop := &RangeOp{
		conditioner: conditioner{
			object:     object,
			conditions: map[string][]*Condition{},
		},
	}
	return rop
}

// Limit sets the number of rows returned per call. Default is 100
func (r *RangeOp) Limit(n int) *RangeOp {
	r.limit = n
	return r
}

// Offset sets the pagination token. If not set, an empty token would be used.
func (r *RangeOp) Offset(token string) *RangeOp {
	r.token = token
	return r
}

// Fields list the non-key fields users want to fetch.
// PrimaryKey fields are always fetched.
func (r *RangeOp) Fields(fields []string) *RangeOp {
	r.fieldsToRead = fields
	return r
}

// String satisfies the Stringer interface
func (r *RangeOp) String() string {
	result := &bytes.Buffer{}
	if r.conditions == nil || len(r.conditions) == 0 {
		result.WriteString("<empty>")
	} else {
		// sort the fields by name for deterministic results
		keys := make([]string, 0, len(r.conditions))
		for key := range r.conditions {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, field := range keys {
			conds := r.conditions[field]
			if result.Len() > 0 {
				result.WriteString(", ")
			}
			result.WriteString(field)
			result.WriteString(" ")
			for i, cond := range conds {
				if i > 0 {
					fmt.Fprintf(result, ", %s ", field)
				}
				fmt.Fprintf(result, "%s %v", cond.Op.String(), cond.Value)
			}
		}
	}
	addLimitTokenString(result, r.limit, r.token)
	return result.String()
}

// Eq is used to express an equality constraint for a range query
func (r *RangeOp) Eq(fieldName string, value interface{}) *RangeOp {
	r.appendOp(Eq, fieldName, value)
	return r
}

// Gt is used to express an "greater than" constraint for a range query
func (r *RangeOp) Gt(fieldName string, value interface{}) *RangeOp {
	r.appendOp(Gt, fieldName, value)
	return r
}

// GtOrEq is used to express an "greater than or equal" constraint for a
// range query
func (r *RangeOp) GtOrEq(fieldName string, value interface{}) *RangeOp {
	r.appendOp(GtOrEq, fieldName, value)
	return r
}

// Lt is used to express a "less than" constraint for a range query
func (r *RangeOp) Lt(fieldName string, value interface{}) *RangeOp {
	r.appendOp(Lt, fieldName, value)
	return r
}

// LtOrEq is used to express a "less than or equal" constraint for a
// range query
func (r *RangeOp) LtOrEq(fieldName string, value interface{}) *RangeOp {
	r.appendOp(LtOrEq, fieldName, value)
	return r
}

type rangeOpMatcher struct {
	conds map[string]map[Condition]bool
	p     pager
	typ   reflect.Type
}

// EqRangeOp creates a gomock Matcher that will match any RangeOp with the same conditions, limit, token, and fields
// as those specified in the op argument.
func EqRangeOp(op *RangeOp) gomock.Matcher {
	conds := make(map[string]map[Condition]bool)
	for col, colConds := range op.conditions {
		conds[col] = make(map[Condition]bool, len(colConds))
		for _, cond := range colConds {
			conds[col][*cond] = true
		}
	}

	return rangeOpMatcher{
		conds: conds,
		p:     op.pager,
		typ:   reflect.TypeOf(op.object).Elem(),
	}
}

// Matches satisfies the gomock.Matcher interface
func (m rangeOpMatcher) Matches(x interface{}) bool {
	op, ok := x.(*RangeOp)
	if !ok {
		return false
	}

	for col, conds := range op.conditions {
		for _, condition := range conds {
			if !m.conds[col][*condition] {
				return false
			}
		}
	}

	return m.p.equals(op.pager) && reflect.TypeOf(op.object).Elem() == m.typ
}

// String satisfies the gomock.Matcher and Stringer interface
func (m rangeOpMatcher) String() string {
	return fmt.Sprintf(
		" is equal to RangeOp with conditions %v, token %s, limit %d, fields %v, and entity type %v",
		m.conds,
		m.p.token,
		m.p.limit,
		m.p.fieldsToRead,
		m.typ)
}

// IndexFromConditions returns the name of the index or the base table to use, along with the key info
// for that index. If no suitable index could be found, an error is returned
func (ei *EntityInfo) IndexFromConditions(conditions map[string][]*Condition) (name string, key *PrimaryKey, err error) {
	identityFunc := func(s string) string { return s }
	// see if we match the primary key for this table
	var baseTableError error
	if baseTableError = EnsureValidRangeConditions(ei.Def, ei.Def.Key, conditions, identityFunc); baseTableError == nil {
		return ei.Def.Name, ei.Def.Key, nil
	}
	if len(ei.Def.Indexes) == 0 {
		return "", nil, baseTableError
	}
	// see if we match an index on this table
	var indexDef *IndexDefinition
	for name, indexDef = range ei.Def.Indexes {
		key = indexDef.Key
		// we check the range conditions before adding the uniqueness columns
		if err := EnsureValidRangeConditions(ei.Def, key, conditions, identityFunc); err == nil {
			return name, ei.Def.UniqueKey(key), nil
		}
	}
	// none of the indexes work, so fail
	return "", nil, errors.Wrapf(baseTableError, "No index matches specified conditions")
}
