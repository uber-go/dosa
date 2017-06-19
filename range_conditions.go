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
	"time"

	"bytes"
	"strings"

	"github.com/pkg/errors"
)

// Condition holds an operator and a value for a condition on a field.
type Condition struct {
	Op    Operator
	Value FieldValue
}

// EnsureValidRangeConditions checks if the conditions for a range query is valid.
// The transform arg is a function to transform the column name to a better representation for error message under
// different circumstances. For example, on client side it can transform the column name to actual go struct field name;
// and on the server side, an identity transformer func can be used.
func EnsureValidRangeConditions(ed *EntityDefinition, pk *PrimaryKey, columnConditions map[string][]*Condition, transform func(string) string) error {
	unconstrainedPartitionKeySet := pk.PartitionKeySet()
	columnTypes := ed.ColumnTypes()

	clusteringKeyConditions := make([][]*Condition, len(pk.ClusteringKeys))

COND:
	for column, conds := range columnConditions {
		if _, ok := unconstrainedPartitionKeySet[column]; ok {
			delete(unconstrainedPartitionKeySet, column)
			if err := ensureExactOneEqCondition(columnTypes[column], conds); err != nil {
				return errors.Wrapf(err, "invalid conditions for partition key: %s", transform(column))
			}
			continue
		}

		for i, c := range pk.ClusteringKeys {
			if column == c.Name {
				clusteringKeyConditions[i] = conds
				continue COND
			}
		}

		return errors.Errorf("cannot enforce condition on non-key column: %s", transform(column))
	}

	if len(unconstrainedPartitionKeySet) > 0 {
		names := []string{}
		for k := range unconstrainedPartitionKeySet {
			names = append(names, transform(k))
		}
		return errors.Errorf("missing Eq condition on partition keys: %v", names)
	}

	if err := ensureClusteringKeyConditions(pk.ClusteringKeys, columnTypes, clusteringKeyConditions, transform); err != nil {
		return errors.Wrap(err, "conditions for clustering keys are invalid")
	}

	return nil
}

func ensureExactOneEqCondition(t Type, conditions []*Condition) error {
	if len(conditions) != 1 {
		return errors.Errorf("expect exact one Eq condition, found: %v", conditions)
	}

	r := conditions[0]
	if r.Op != Eq {
		return errors.Errorf("only Eq condition is allowed on this column for this query, found: %s", r.Op.String())
	}

	if err := ensureTypeMatch(t, r.Value); err != nil {
		return errors.Wrap(err, "the value in condition does not have expected type")
	}
	return nil
}

func ensureClusteringKeyConditions(cks []*ClusteringKey, columnTypes map[string]Type,
	clusteringKeyConditions [][]*Condition, transform func(string) string) error {
	// ensure conditions are applied to consecutive clustering keys
	lastConstrainedIndex := -1
	for i, conditions := range clusteringKeyConditions {
		if len(conditions) > 0 {
			if lastConstrainedIndex != i-1 {
				return errors.Errorf("conditions must be applied consecutively on clustering keys, "+
					"but at least one clustering key is unconstrained before: %s", transform(cks[i].Name))
			}
			lastConstrainedIndex = i
		}
	}

	// ensure only Eq is applied to clustering keys except for the last constrained one
	for i := 0; i < lastConstrainedIndex; i++ {
		name := cks[i].Name
		if err := ensureExactOneEqCondition(columnTypes[name], clusteringKeyConditions[i]); err != nil {
			return errors.Wrapf(err, "exact one Eq condition can be applied except for the last "+
				"constrained clustering key, found invalid condition for key: %s", transform(name))
		}
	}

	// ensure the last constrained clustering key has valid conditions
	if lastConstrainedIndex >= 0 {
		name := cks[lastConstrainedIndex].Name
		if err := ensureValidConditions(columnTypes[name], clusteringKeyConditions[lastConstrainedIndex]); err != nil {
			return errors.Wrapf(err, "invalid or unsupported conditions for clustering key: %s", transform(name))
		}
	}

	return nil
}

const conditionsRule = `
If you have a Lt or LtOrEq operator on a column, you can also have a Gt or GtOrEq on the same column.
No other combinations of operators are permitted.
`

// Start with simple rules as specified in `conditionsRule` above.
// Hence, the length of valid conditions slice is either one or two (won't be called if zero length).
func ensureValidConditions(t Type, conditions []*Condition) error {
	// check type sanity
	for _, r := range conditions {
		if err := ensureTypeMatch(t, r.Value); err != nil {
			return errors.Wrap(err, "invalid condition")
		}
	}

	switch {
	case len(conditions) == 1:
		return nil // single condition is always valid
	case len(conditions) > 2:
		return errors.Errorf("conditions: %v, rules: %s", conditions, conditionsRule)
	}

	r0 := conditions[0]
	r1 := conditions[1]
	// sort conditions according to operators so we have few cases to handle
	if r0.Op >= r1.Op {
		r0, r1 = r1, r0
	}

	op0 := r0.Op
	v0 := r0.Value
	op1 := r1.Op
	v1 := r1.Value

	switch {
	//  v1 < fv < v0, v1 <= fv < v0, v1 < fv <= v0    ===> v0 > v1
	case op0 == Lt && op1 == Gt, op0 == Lt && op1 == GtOrEq, op0 == LtOrEq && op1 == Gt:
		if compare(t, v0, v1) <= 0 {
			return errors.Errorf("invalid range: %v", conditions)
		}
		// v1 <= fv <= v0   ===> v0 >= v1
	case op0 == LtOrEq && op1 == GtOrEq:
		if compare(t, v0, v1) < 0 {
			return errors.Errorf("invalid range: %v", conditions)
		}
	default: // invalid combination of operators
		return errors.Errorf("unsupported conditions: %v, rules: %s", conditions, conditionsRule)

	}

	return nil
}

// compare compares two values; return 0 if equal, -1 if <, 1 if >.
// Assumes args are valid.
func compare(t Type, a, b interface{}) int {
	switch t {
	case TUUID:
		// TODO: make sure if comparison for UUID like below makes sense.
		return strings.Compare(string(a.(UUID)), string(b.(UUID)))
	case Int64:
		return int(a.(int64) - b.(int64))
	case Int32:
		return int(a.(int32) - b.(int32))
	case String:
		return strings.Compare(a.(string), b.(string))
	case Blob:
		return bytes.Compare(a.([]byte), b.([]byte))
	case Bool:
		// TODO: we don't need to order bools for range query and should report error if people do dumb things
		var ia, ib int
		if a.(bool) {
			ia = 1
		}
		if b.(bool) {
			ib = 1
		}
		return ia - ib
	case Double:
		fa := a.(float64)
		fb := b.(float64)
		if fa < fb {
			return -1
		}
		if fa > fb {
			return 1
		}
		return 0
	case Timestamp:
		ta := a.(time.Time)
		tb := b.(time.Time)
		if ta.Before(tb) {
			return -1
		}
		if ta.After(tb) {
			return 1
		}
		return 0
	}
	panic("invalid type") // shouldn't reach here
}

func ensureTypeMatch(t Type, v FieldValue) error {
	switch t {
	case TUUID:
		if _, ok := v.(UUID); !ok {
			return errors.Errorf("invalid value for UUID type: %v", v)
		}
	case Int64:
		if _, ok := v.(int64); !ok {
			return errors.Errorf("invalid value for int64 type: %v", v)
		}
	case Int32:
		if _, ok := v.(int32); !ok {
			return errors.Errorf("invalid value for int32 type: %v", v)
		}
	case String:
		if _, ok := v.(string); !ok {
			return errors.Errorf("invalid value for string type: %v", v)
		}
	case Blob:
		if _, ok := v.([]byte); !ok {
			return errors.Errorf("invalid value for blob type: %v", v)
		}
	case Bool:
		if _, ok := v.(bool); !ok {
			return errors.Errorf("invalid value for bool type: %v", v)
		}
	case Double:
		if _, ok := v.(float64); !ok {
			return errors.Errorf("invalid value for double/float64 type: %v", v)
		}
	case Timestamp:
		if _, ok := v.(time.Time); !ok {
			return errors.Errorf("invalid value for timestamp type: %v", v)
		}
	default:
		// will not happen unless we have a bug
		panic("invalid type")
	}
	return nil
}
