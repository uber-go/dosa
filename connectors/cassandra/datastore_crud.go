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

package cassandra

import (
	"context"
	"sort"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
	"github.com/uber-go/dosa"
)

func sortFieldValue(obj map[string]dosa.FieldValue) ([]string, []interface{}, error) {
	columns := make([]string, len(obj))

	pos := 0
	for k := range obj {
		columns[pos] = k
		pos++
	}

	sort.Strings(columns)

	values := make([]interface{}, len(obj))
	for pos, c := range columns {
		values[pos] = obj[c]
		var err error
		// UUIDs are converted between satori and gocql via string; errors are not expected.
		if u, ok := obj[c].(uuid.UUID); ok {
			values[pos], err = gocql.ParseUUID(u.String())
			if err != nil {
				return nil, nil, errors.Wrapf(err, "invalid uuid %s", u)
			}
		}
	}

	return columns, values, nil
}

// transforms a map of column name and the column value to a sorted array of ColumnConditions
func convertToColumnConditions(keys map[string]dosa.FieldValue) []*dosa.ColumnCondition {
	colConds := make(map[string][]*dosa.Condition, len(keys))
	for name, value := range keys {
		colConds[name] = []*dosa.Condition{
			{
				Op:    dosa.Eq,
				Value: value,
			},
		}
	}
	return dosa.NormalizeConditions(colConds)
}

// CreateIfNotExists creates an object if not exists
func (c *Connector) CreateIfNotExists(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	keyspace := c.KsMapper.Keyspace(ei.Ref.Scope, ei.Ref.NamePrefix)
	table := ei.Def.Name

	sortedColumns, sortedValues, err := sortFieldValue(values)
	if err != nil {
		return err
	}

	stmt, err := InsertStmt(
		Keyspace(keyspace),
		Table(table),
		Columns(sortedColumns),
		Values(sortedValues),
		IfNotExist(true),
	)
	if err != nil {
		return errors.Wrap(err, "failed to create cql statement")
	}

	applied, err := c.Session.Query(stmt, sortedValues...).WithContext(ctx).MapScanCAS(map[string]interface{}{})
	if err != nil {
		return errors.Wrapf(err, "failed to execute CreateIfNotExists query in cassandra: %s", stmt)
	}

	if !applied {
		return &dosa.ErrAlreadyExists{}
	}

	return nil
}

// Read reads an object based on primary key
func (c *Connector) Read(ctx context.Context, ei *dosa.EntityInfo, keys map[string]dosa.FieldValue, fieldsToRead []string) (map[string]dosa.FieldValue, error) {
	keyspace := c.KsMapper.Keyspace(ei.Ref.Scope, ei.Ref.NamePrefix)
	table := ei.Def.Name

	fields := fieldsToRead
	if len(fields) == 0 {
		fields = extractNonKeyColumns(ei.Def)
	}

	sort.Strings(fields)

	conds := convertToColumnConditions(keys)

	_, sortedValues, err := sortFieldValue(keys)
	if err != nil {
		return nil, err
	}

	stmt, err := SelectStmt(
		Keyspace(keyspace),
		Table(table),
		Columns(fields),
		Conditions(conds),
		Limit(1),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create cql statement")
	}

	result := make(map[string]interface{})
	// TODO workon timeout trace features
	if err := c.Session.Query(stmt, sortedValues...).WithContext(ctx).MapScan(result); err != nil {
		if err == gocql.ErrNotFound {
			return nil, &dosa.ErrNotFound{}
		}
		return nil, errors.Wrapf(err, "failed to execute read query in Cassandra: %s", stmt)
	}

	return convertToDOSATypes(ei, result)
}

// Upsert means update an existing object or create a new object
func (c *Connector) Upsert(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	keyspace := c.KsMapper.Keyspace(ei.Ref.Scope, ei.Ref.NamePrefix)
	table := ei.Def.Name

	sortedColumns, sortedValues, err := sortFieldValue(values)
	if err != nil {
		return err
	}

	stmt, err := InsertStmt(
		Keyspace(keyspace),
		Table(table),
		Columns(sortedColumns),
		Values(sortedValues),
		IfNotExist(false),
	)
	if err != nil {
		return errors.Wrap(err, "failed to create cql statement")
	}

	if err := c.Session.Query(stmt, sortedValues...).WithContext(ctx).Exec(); err != nil {
		return errors.Wrapf(err, "failed to execute upsert query in cassandra: %s", stmt)
	}

	return nil
}

// Remove object based on primary key
func (c *Connector) Remove(ctx context.Context, ei *dosa.EntityInfo, keys map[string]dosa.FieldValue) error {
	conds := convertToColumnConditions(keys)

	_, sortedValues, err := sortFieldValue(keys)
	if err != nil {
		return err
	}

	return c.remove(ctx, ei, conds, sortedValues)
}

// RemoveRange removes a range of objects based on column conditions.
func (c *Connector) RemoveRange(ctx context.Context, ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition) error {
	conds, values, err := prepareConditions(columnConditions)
	if err != nil {
		return err
	}
	return c.remove(ctx, ei, conds, values)
}

func (c *Connector) remove(ctx context.Context, ei *dosa.EntityInfo, conds []*dosa.ColumnCondition, values []interface{}) error {
	keyspace := c.KsMapper.Keyspace(ei.Ref.Scope, ei.Ref.NamePrefix)
	table := ei.Def.Name

	stmt, err := DeleteStmt(
		Keyspace(keyspace),
		Table(table),
		Conditions(conds),
	)

	if err != nil {
		return errors.Wrap(err, "failed to create cql statement")
	}

	if err := c.Session.Query(stmt, values...).WithContext(ctx).Exec(); err != nil {
		return errors.Wrapf(err, "failed to execute remove query in Cassandra: %s", stmt)
	}

	return nil
}

func convertToDOSATypes(ei *dosa.EntityInfo, row map[string]interface{}) (map[string]dosa.FieldValue, error) {
	var err error
	res := make(map[string]dosa.FieldValue)
	ct := extractColumnTypes(ei)
	for k, v := range row {
		dosaType := ct[k]
		raw := v
		// special handling
		switch dosaType {
		case dosa.TUUID:
			uuidStr := raw.(gocql.UUID).String()
			if raw, err = uuid.FromString(uuidStr); err != nil {
				return nil, errors.Wrapf(err, "incompatible UUID %s", uuidStr)
			}
		// for whatever reason, gocql returns int for int32 field
		// TODO: decide whether to store timestamp as int64 for better resolution; see
		// https://code.uberinternal.com/T733022
		case dosa.Int32:
			raw = int32(raw.(int))
		}
		res[k] = raw
	}
	return res, nil
}

func extractColumnTypes(ei *dosa.EntityInfo) map[string]dosa.Type {
	m := make(map[string]dosa.Type)
	for _, c := range ei.Def.Columns {
		m[c.Name] = c.Type
	}
	return m
}

func extractNonKeyColumns(ed *dosa.EntityDefinition) []string {
	columns := ed.ColumnTypes()
	keySet := ed.KeySet()
	res := []string{}
	for name := range columns {
		if _, ok := keySet[name]; !ok {
			res = append(res, name)
		}
	}
	return res
}
