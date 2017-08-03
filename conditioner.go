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

import "github.com/pkg/errors"

type conditioner struct {
	object     DomainObject
	conditions map[string][]*Condition
}

func (c *conditioner) appendOp(op Operator, fieldName string, value interface{}) {
	c.conditions[fieldName] = append(c.conditions[fieldName], &Condition{Op: op, Value: value})
}

// convertConditions converts a list of client field names to server side field names
func convertConditions(conditions map[string][]*Condition, t *Table) (map[string][]*Condition, error) {
	serverConditions := map[string][]*Condition{}
	for colName, conds := range conditions {
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
