package cassandra

import (
	"context"
	"sort"

	"code.uber.internal/infra/dosa-gateway/datastore/common"
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
)

func sortFieldValue(obj map[string]dosa.FieldValue) ([]string, []interface{}, error) {
	columns := make([]string, len(obj))

	pos := 0
	for k := range obj {
		columns[pos] = k
		pos++
	}

	sort.Sort(sortedFields(columns))

	values := make([]interface{}, len(obj))
	for pos, c := range columns {
		values[pos] = obj[c]
		var err error
		// specially handling for uuid is needed
		if u, ok := obj[c].(dosa.UUID); ok {
			values[pos], err = gocql.ParseUUID(string(u))
			if err != nil {
				return nil, nil, errors.Wrapf(err, "invalid uuid %s", u)
			}
		}
	}

	return columns, values, nil
}

// CreateIfNotExists creates an object if not exists
func (c *Connector) CreateIfNotExists(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	cluster, keyspace, table, err := c.resolve(ei)
	if err != nil {
		return err
	}

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

	applied, err := cluster.session.Query(stmt, sortedValues...).MapScanCAS(map[string]interface{}{})
	if err != nil {
		return errors.Wrapf(err, "failed to execute CreateIfNotExists query in cassandra: %s", stmt)
	}

	if !applied {
		return common.NewErrAlreadyExists("entity already exists")
	}

	return nil
}

// Read reads an object based on primary key
func (c *Connector) Read(ctx context.Context, ei *dosa.EntityInfo, keys map[string]dosa.FieldValue, fieldsToRead []string) (map[string]dosa.FieldValue, error) {
	cluster, keyspace, table, err := c.resolve(ei)
	if err != nil {
		return nil, err
	}

	fields := fieldsToRead
	if len(fields) == 0 {
		fields = extractNonKeyColumns(ei.Def)
	}

	sort.Sort(sortedFields(fields))

	conds := make([]*ColumnCondition, len(keys))
	pos := 0
	for name, value := range keys {
		conds[pos] = &ColumnCondition{
			Name: name,
			Condition: &dosa.Condition{
				Op:    dosa.Eq,
				Value: value,
			},
		}
		pos++
	}
	sort.Sort(sortedColumnCondition(conds))

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
	if err := cluster.session.Query(stmt, sortedValues...).MapScan(result); err != nil {
		if err == gocql.ErrNotFound {
			return nil, common.NewErrNotFound("entity not found")
		}
		return nil, errors.Wrapf(err, "failed to execute read query in Cassandra: %s", stmt)
	}

	return convertToDOSATypes(ei, result), nil
}

// Upsert means update an existing object or create a new object
func (c *Connector) Upsert(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	cluster, keyspace, table, err := c.resolve(ei)
	if err != nil {
		return err
	}

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

	if err := cluster.session.Query(stmt, sortedValues...).Exec(); err != nil {
		return errors.Wrapf(err, "failed to execute upsert query in cassandra: %s", stmt)
	}

	return nil
}

// Remove object based on primary key
func (c *Connector) Remove(ctx context.Context, ei *dosa.EntityInfo, keys map[string]dosa.FieldValue) error {
	conds := make([]*ColumnCondition, len(keys))
	pos := 0
	for name, value := range keys {
		conds[pos] = &ColumnCondition{
			Name: name,
			Condition: &dosa.Condition{
				Op:    dosa.Eq,
				Value: value,
			},
		}
		pos++
	}
	sort.Sort(sortedColumnCondition(conds))

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

func (c *Connector) remove(ctx context.Context, ei *dosa.EntityInfo, conds []*ColumnCondition, values []interface{}) error {
	cluster, keyspace, table, err := c.resolve(ei)
	if err != nil {
		return err
	}

	stmt, err := DeleteStmt(
		Keyspace(keyspace),
		Table(table),
		Conditions(conds),
	)

	if err != nil {
		return errors.Wrap(err, "failed to create cql statement")
	}

	if err := cluster.session.Query(stmt, values...).Exec(); err != nil {
		return errors.Wrapf(err, "failed to execute remove query in Cassandra: %s", stmt)
	}

	return nil
}

func convertToDOSATypes(ei *dosa.EntityInfo, row map[string]interface{}) map[string]dosa.FieldValue {
	res := make(map[string]dosa.FieldValue)
	ct := extractColumnTypes(ei)
	for k, v := range row {
		dosaType := ct[k]
		raw := v
		// special handling
		switch dosaType {
		case dosa.TUUID:
			uuid := raw.(gocql.UUID).String()
			raw = dosa.UUID(uuid)
		// for whatever reason, gocql returns int for int32 field
		// TODO: decide whether to store timestamp as int64 for better resolution; see
		// https://code.uberinternal.com/T733022
		case dosa.Int32:
			raw = int32(raw.(int))
		}
		res[k] = raw
	}
	return res
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
