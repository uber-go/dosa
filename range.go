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
	"sort"

	"github.com/pkg/errors"
	"github.com/golang/mock/gomock"
)

// RangeOp is used to specify constraints to Range calls
type RangeOp struct {
	sop        ScanOp
	conditions map[string][]*Condition
}

// NewRangeOp returns a new RangeOp instance
func NewRangeOp(object DomainObject) *RangeOp {
	rop := &RangeOp{conditions: map[string][]*Condition{}, sop: ScanOp{object: object}}
	return rop
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
	addLimitTokenString(result, r.sop.limit, r.sop.token)
	return result.String()
}

func (r *RangeOp) appendOp(op Operator, fieldName string, value interface{}) *RangeOp {
	r.conditions[fieldName] = append(r.conditions[fieldName], &Condition{Op: op, Value: value})
	return r
}

// Eq is used to express an equality constraint for a range query
func (r *RangeOp) Eq(fieldName string, value interface{}) *RangeOp {
	return r.appendOp(Eq, fieldName, value)
}

// Gt is used to express an "greater than" constraint for a range query
func (r *RangeOp) Gt(fieldName string, value interface{}) *RangeOp {
	return r.appendOp(Gt, fieldName, value)
}

// GtOrEq is used to express an "greater than or equal" constraint for a
// range query
func (r *RangeOp) GtOrEq(fieldName string, value interface{}) *RangeOp {
	return r.appendOp(GtOrEq, fieldName, value)
}

// Lt is used to express a "less than" constraint for a range query
func (r *RangeOp) Lt(fieldName string, value interface{}) *RangeOp {
	return r.appendOp(Lt, fieldName, value)
}

// LtOrEq is used to express a "less than or equal" constraint for a
// range query
func (r *RangeOp) LtOrEq(fieldName string, value interface{}) *RangeOp {
	return r.appendOp(LtOrEq, fieldName, value)
}

// Fields list the non-key fields users want to fetch. If not set, all fields would be fetched.
// PrimaryKey fields are always fetched.
func (r *RangeOp) Fields(fieldsToRead []string) *RangeOp {
	r.sop.fieldsToRead = fieldsToRead
	return r
}

// Limit sets the number of rows returned per call. If not set, a default
// value would be applied
func (r *RangeOp) Limit(n int) *RangeOp {
	r.sop.limit = n
	return r
}

// Offset sets the pagination token. If not set, an empty token would be used.
func (r *RangeOp) Offset(token string) *RangeOp {
	r.sop.token = token
	return r
}

// convertRangeOp converts a list of client field names to server side field names
//
func convertRangeOpConditions(r *RangeOp, t *Table) (map[string][]*Condition, error) {
	serverConditions := map[string][]*Condition{}
	for colName, conds := range r.conditions {
		if scolName, ok := t.FieldToCol[colName]; ok {
			serverConditions[scolName] = conds
			// we need to be sure each of the types are correct for marshaling
			cd := t.FindColumnDefinition(scolName)
			for _, cond := range conds {
				if err := ensureTypeMatch(cd.Type, cond.Value); err != nil {
					return nil, errors.Wrapf(err, "column %s", colName)
				}
			}
		} else {
			return nil, errors.Errorf("Cannot find column %q in struct %q", colName, t.StructName)
		}
	}
	return serverConditions, nil
}

type CondsMantcher struct {}



type rangeOpMatcher struct {
	conds  map[Condition]bool
	eqScanOp gomock.Matcher
}

// EqRangeOp creates a gomock Matcher that will match any RangeOp with the same conditions, limit, token, and fields
// as those specified in the op argument.
func EqRangeOp(op *RangeOp) gomock.Matcher {
	conds := make(map[Condition]bool)
	for _, colConds := range op.conditions {
		for _, cond := range colConds {
			conds[*cond] = true
		}
	}

	return rangeOpMatcher{
		conds: conds,
		eqScanOp: EqScanOp(&(op.sop)),
	}
}
func (m rangeOpMatcher) Matches(x interface{}) bool {
	op := x.(*RangeOp)
	for _, conditions := range op.conditions {
		for _, condition := range conditions {
			if !m.conds[*condition] {
				return false
			}
		}
	}

	return m.eqScanOp.Matches(&op.sop)
}

func (m rangeOpMatcher) String() string {
	return fmt.Sprintf(
		" is equals to RangeOp with conditions %q, and scan op %s",
		m.conds,
		m.eqScanOp.String(),
	)
}