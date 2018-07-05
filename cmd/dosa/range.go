// Copyright (c) 2018 Uber Technologies, Inc.
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

package main

import (
	"context"

	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
)

// Range uses the connector to fetch rows for a given range
func (c *shellQueryClient) Range(ctx context.Context, ops []*queryObj, fields []string, limit int) ([]map[string]dosa.FieldValue, error) {
	// look up the entity in the registry
	re, _ := c.registrar.Find(&dosa.Entity{})

	// build range operator with expressions and limit number
	r, err := buildRangeOp(ops, limit)
	if err != nil {
		return nil, err
	}

	// now convert the client range columns to server side column conditions structure
	columnConditions, err := dosa.ConvertConditions(r.Conditions(), re.Table())
	if err != nil {
		return nil, err
	}

	// convert the field names to column names
	columns, err := re.ColumnNames(fields)
	if err != nil {
		return nil, err
	}

	// call the server side method
	values, _, err := c.connector.Range(ctx, re.EntityInfo(), columnConditions, columns, "", r.LimitRows())
	if err != nil {
		return nil, err
	}

	return convertColToField(values, re.Table().ColToField), err
}

func buildRangeOp(ops []*queryObj, limit int) (*dosa.RangeOp, error) {
	r := dosa.NewRangeOp(&dosa.Entity{})

	// apply the queries
	for _, op := range ops {
		switch op.op {
		case "eq":
			r = r.Eq(op.fieldName, op.value)
		case "lt":
			r = r.Lt(op.fieldName, op.value)
		case "le":
			r = r.LtOrEq(op.fieldName, op.value)
		case "gt":
			r = r.Gt(op.fieldName, op.value)
		case "ge":
			r = r.GtOrEq(op.fieldName, op.value)
		default:
			// only eq, lt, le, gt, ge allowed for range
			return nil, errors.Errorf("wrong operator used for range, supported: eq, lt, le, gt, ge")
		}
	}

	// apply the limit of results to return
	r.Limit(limit)
	return r, nil
}
