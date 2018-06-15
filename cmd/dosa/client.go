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

package main

import (
	"context"

	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
)

// ShellQueryClient defines methods to be use by CLI tools
type ShellQueryClient interface {
	// Range fetches entities within a range
	Range(ctx context.Context, ops []*queryObj, fields []string, limit int) ([]map[string]dosa.FieldValue, error)
	// Read fetches a row by primary key
	Read(ctx context.Context, ops []*queryObj, fields []string, limit int) ([]map[string]dosa.FieldValue, error)
	// GetRegistrar returns the registrar
	GetRegistrar() dosa.Registrar
	// Shutdown gracefully shuts down the shell query client
	Shutdown() error
	// Initialize is called when initialize the shell query client
	Initialize(ctx context.Context) error
}

type shellQueryClient struct {
	registrar dosa.Registrar
	connector dosa.Connector
}

type shellEntity struct {
	dosa.Entity
}

// newShellClient returns a new DOSA shell query client for the registrar and connector provided.
func newShellQueryClient(reg dosa.Registrar, conn dosa.Connector) ShellQueryClient {
	return &shellQueryClient{
		registrar: reg,
		connector: conn,
	}
}

// GetRegistrar returns the registrar that is registered in the client
func (c *shellQueryClient) GetRegistrar() dosa.Registrar {
	return c.registrar
}

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

func (c *shellQueryClient) Initialize(ctx context.Context) error {
	registar := c.GetRegistrar()
	reg, _ := registar.Find(&dosa.Entity{})

	version, err := c.connector.CheckSchema(ctx, registar.Scope(), registar.NamePrefix(), []*dosa.EntityDefinition{reg.EntityDefinition()})
	if err != nil {
		return errors.New("CheckSchema failed")
	}
	reg.SetVersion(version)
	return nil
}

// Shutdown gracefully shuts down the client
func (c *shellQueryClient) Shutdown() error {
	return c.connector.Shutdown()
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
