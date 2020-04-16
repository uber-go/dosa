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
	"bytes"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// Condition holds an operator and a value for a condition on a field.
type Condition struct {
	Op    Operator
	Value FieldValue
}

// ColumnCondition represents the condition of each column
type ColumnCondition struct {
	Name      string
	Condition *Condition
}

func (cc *ColumnCondition) String() string {
	return fmt.Sprintf("(%s %v %v)", cc.Name, cc.Condition.Op, cc.Condition.Value)
}

// SortedColumnCondition implements sorting of an array of columnConditions
type sortedColumnCondition []*ColumnCondition

func (list sortedColumnCondition) Len() int { return len(list) }

func (list sortedColumnCondition) Swap(i, j int) { list[i], list[j] = list[j], list[i] }

func (list sortedColumnCondition) Less(i, j int) bool {
	si := list[i]
	sj := list[j]

	if si.Name != sj.Name {
		return si.Name < sj.Name
	}

	return si.Condition.Op < sj.Condition.Op
}

// NormalizeConditions takes a set of conditions for columns and returns a sorted, denormalized view of the conditions.
func NormalizeConditions(columnConditions map[string][]*Condition) []*ColumnCondition {
	var cc []*ColumnCondition

	for column, conds := range columnConditions {
		for _, cond := range conds {
			cc = append(cc, &ColumnCondition{
				Name:      column,
				Condition: cond})
		}
	}

	sort.Sort(sortedColumnCondition(cc))
	return cc
}

func ConditionsString(columnConditions map[string][]*Condition) string {
	if len(columnConditions) == 0 {
		return "()"
	}
	nc := NormalizeConditions(columnConditions)
	s := make([]string, 0, len(nc))
	for _, cc := range nc {
		s = append(s, fmt.Sprintf("%v", cc))
	}
	if len(s) == 1 {
		return s[0]
	}
	return "(" + strings.Join(s, " && ") + ")"
}

// EnsureValidRangeConditions checks the conditions for a PK Range(). "transform" is a name-prettifying function.
func EnsureValidRangeConditions(ed *EntityDefinition, pk *PrimaryKey, columnConditions map[string][]*Condition, transform func(string) string) error {
	// The requirements for range conditions on the PK being valid:
	//     partition key: each field must be present, with a single Eq constraint on each
	//     clustering key: conditions must be applied to consecutive fields and must all be Eq except for the last one

	if transform == nil {
		transform = func(s string) string { return s }
	}

	// Get the partition key. Fields will be removed from missingPKs as we find them in columnConditions.
	partitionKeys := pk.PartitionKeySet()
	missingPKs := pk.PartitionKeySet()

	// For the clustering key the order matters; remember the position of each one.
	clusteringKeys := make(map[string]int)
	for i, k := range pk.ClusteringKeys {
		clusteringKeys[k.Name] = i
	}
	clusteringConds := make([][]*Condition, len(pk.ClusteringKeys))

	columnTypes := ed.ColumnTypes()

	for column, conds := range columnConditions {
		// column in the partition key?
		if _, ok := partitionKeys[column]; ok {
			if err := ensureExactOneEqCondition(columnTypes[column], conds); err != nil {
				return errors.Wrapf(err, "invalid conditions for partition key: %s", transform(column))
			}
			delete(missingPKs, column)
			continue
		}

		// column in the clustering key?
		if i, ok := clusteringKeys[column]; ok {
			// Save the condition, check after we've collected them all.
			clusteringConds[i] = conds
			continue
		}

		return errors.Errorf("column %s is not in the primary key", transform(column))
	}

	// Were all the partition key fields OK?
	if len(missingPKs) > 0 {
		names := []string{}
		for k := range missingPKs {
			names = append(names, transform(k))
		}
		return errors.Errorf("missing Eq condition on partition keys: %v", names)
	}

	// Finally, ensure the clustering key conditions are OK.
	if err := ensureClusteringKeyConditions(pk.ClusteringKeys, columnTypes, clusteringConds, transform); err != nil {
		return errors.Wrap(err, "conditions for clustering keys are invalid")
	}

	return nil
}

func ensureExactOneEqCondition(t Type, conditions []*Condition) error {
	if len(conditions) != 1 {
		return errors.Errorf("expected exactly one Eq condition, found: %v", conditions)
	}

	r := conditions[0]
	if r.Op != Eq {
		return errors.Errorf("only Eq is allowed on this column for this query, found: %s", r.Op.String())
	}

	if err := ensureTypeMatch(t, r.Value); err != nil {
		return errors.Wrapf(err, "the value %v in the condition does not have expected type %v", r.Value, t)
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
