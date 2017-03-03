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
	"github.com/pkg/errors"
)

// RangeOp is used to specify constraints to Range calls
type RangeOp struct {
	conditions    map[string][]*Condition
	table         *Table
	pendingError  error
	fieldsToFetch []string
	limit         int
	token         string
}

// NewRangeOp returns a new RangeOp instance
func NewRangeOp(object DomainObject) *RangeOp {
	rop := &RangeOp{conditions: map[string][]*Condition{}}
	// TODO: Use the registry to speed this up, which means NewRangeOp is a Registry or Client object method
	rop.table, rop.pendingError = TableFromInstance(object)
	return rop
}

// String satisfies the Stringer interface
func (r *RangeOp) String() string {
	result := &bytes.Buffer{}
	if r.conditions == nil || len(r.conditions) == 0 {
		result.WriteString("<empty>")
	} else {
		for field, conds := range r.conditions {
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
	if r.limit > 0 {
		fmt.Fprintf(result, " limit %d", r.limit)
	}
	if r.token != "" {
		fmt.Fprintf(result, " token %q", r.token)
	}
	return result.String()
}

func (r *RangeOp) appendOp(op Operator, fieldName string, value interface{}) *RangeOp {
	if r.pendingError != nil {
		return r
	}
	colName := r.table.FieldToCol[fieldName]
	if colName == "" {
		r.pendingError = errors.Errorf("Field %s not in entity %s", fieldName, r.table.StructName)
		return r
	}
	fieldInfo := r.table.FindColumnDefinition(colName)
	if err := ensureTypeMatch(fieldInfo.Type, value); err != nil {
		r.pendingError = errors.Wrapf(err, "Column %s", fieldName)
		return r
	}

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
func (r *RangeOp) Fields(fieldsToFetch []string) *RangeOp {
	r.fieldsToFetch = fieldsToFetch
	return r
}

// Limit sets the number of rows returned per call. If not set, a default
// value would be applied
func (r *RangeOp) Limit(n int) *RangeOp {
	r.limit = n
	return r
}

// Offset sets the pagination token. If not set, an empty token would be used.
func (r *RangeOp) Offset(token string) *RangeOp {
	r.token = token
	return r
}
