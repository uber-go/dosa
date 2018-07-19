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

// Read fetches a row by primary key.
func (c *shellQueryClient) Read(ctx context.Context, ops []*queryObj, fields []string, limit int) ([]map[string]dosa.FieldValue, error) {
	// look up entity in the registry
	re, _ := c.registrar.Find(&dosa.Entity{})

	// build arguments for read with expressions
	fvs, err := buildReadArgs(ops)
	if err != nil {
		return nil, err
	}

	// convert the field names to column names
	columns, err := re.ColumnNames(fields)
	if err != nil {
		return nil, err
	}

	res, err := c.connector.Read(ctx, re.EntityInfo(), fvs, columns)

	return convertColToField([]map[string]dosa.FieldValue{res}, re.Table().ColToField), err
}

func buildReadArgs(ops []*queryObj) (map[string]dosa.FieldValue, error) {
	res := make(map[string]dosa.FieldValue)
	for _, op := range ops {
		// sanity check, only eq is allowed for read
		if op.op != "eq" {
			return nil, errors.New("wrong operator used for read, supported: eq")
		}
		res[op.colName] = op.value
	}
	return res, nil
}

func convertColToField(rowsCol []map[string]dosa.FieldValue, colToField map[string]string) []map[string]dosa.FieldValue {
	rowsField := make([]map[string]dosa.FieldValue, len(rowsCol))
	for idx := range rowsCol {
		rowField := make(map[string]dosa.FieldValue)
		for columnName, fieldValue := range rowsCol[idx] {
			fieldName, ok := colToField[columnName]
			if !ok {
				continue // we ignore fields that we don't know about
			}
			rowField[fieldName] = fieldValue
		}
		rowsField[idx] = rowField
	}
	return rowsField
}
