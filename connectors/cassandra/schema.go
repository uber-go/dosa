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
	"fmt"

	"bytes"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
)

// CreateScope creates a keyspace
func (c *Connector) CreateScope(ctx context.Context, scope string) error {
	// drop the old scope, ignoring errors
	err := c.DropScope(ctx, scope)
	if err != nil {
		fmt.Errorf("drop scope failed: %v", err)
	}
	ksn := CleanupKeyspaceName(scope)
	// TODO: improve the replication factor, should have 3 replicas in each datacenter
	err = c.Session.Query(
		fmt.Sprintf(`CREATE KEYSPACE "%s" WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 1}`,
			ksn)).
		Exec()
	if err != nil {
		return errors.Wrapf(err, "Unable to create keyspace %q", ksn)
	}
	return nil
}

// TruncateScope is not implemented
func (c *Connector) TruncateScope(ctx context.Context, scope string) error {
	panic("not implemented")
}

// DropScope is not implemented
func (c *Connector) DropScope(ctx context.Context, scope string) error {
	ksn := CleanupKeyspaceName(scope)
	err := c.Session.Query(fmt.Sprintf(`DROP KEYSPACE IF EXISTS "%s"`, ksn)).Exec()
	if err != nil {
		return errors.Wrapf(err, "Unable to drop keyspace %q", ksn)
	}
	return nil
}

// RepairableSchemaMismatchError is an error describing what can be added to make
// this schema current. It might include a lot of tables or columns
type RepairableSchemaMismatchError struct {
	MissingColumns []MissingColumn
	MissingTables  []string
}

// MissingColumn describes a column that is missing
type MissingColumn struct {
	Column    dosa.ColumnDefinition
	Tablename string
}

// HasMissing returns true if there are missing columns
func (m *RepairableSchemaMismatchError) HasMissing() bool {
	return m.MissingColumns != nil || m.MissingTables != nil
}

// Error prints a human-readable error message describing the first missing table or column
func (m *RepairableSchemaMismatchError) Error() string {
	if m.MissingTables != nil {
		return fmt.Sprintf("Missing %d tables (first is %q)", len(m.MissingTables), m.MissingTables[0])
	}
	return fmt.Sprintf("Missing %d columns (first is %q in table %q)", len(m.MissingColumns), m.MissingColumns[0].Column.Name, m.MissingColumns[0].Tablename)
}

// Check partition keys
func checkPartitionKeys(ed *dosa.EntityDefinition, md *gocql.TableMetadata) error {
	if len(ed.Key.PartitionKeys) != len(md.PartitionKey) {
		return fmt.Errorf("Table %q partition key length mismatch (was %d should be %d)", ed.Name, len(md.PartitionKey), len(ed.Key.PartitionKeys))
	}
	for i, pk := range ed.Key.PartitionKeys {
		if md.PartitionKey[i].Name != pk {
			return fmt.Errorf("Table %q partition key mismatch (should be %q)", ed.Name, ed.Key.PartitionKeys)
		}
	}
	return nil
}

func checkClusteringKeys(ed *dosa.EntityDefinition, md *gocql.TableMetadata) error {
	if len(ed.Key.ClusteringKeys) != len(md.ClusteringColumns) {
		return fmt.Errorf("Table %q clustering key length mismatch (should be %q)", ed.Name, ed.Key.ClusteringKeys)
	}
	for i, ck := range ed.Key.ClusteringKeys {
		if md.ClusteringColumns[i].Name != ck.Name {
			return fmt.Errorf("Table %q clustering key mismatch (column %d should be %q)", ed.Name, i+1, ck.Name)
		}
	}
	return nil
}
func checkColumns(ed *dosa.EntityDefinition, md *gocql.TableMetadata, schemaErrors *RepairableSchemaMismatchError) {
	// Check each column
	for _, col := range ed.Columns {
		_, ok := md.Columns[col.Name]
		if !ok {
			schemaErrors.MissingColumns = append(schemaErrors.MissingColumns, MissingColumn{Column: *col, Tablename: ed.Name})
		}
		// TODO: check column type
	}
}

// compareStructToSchema compares a dosa EntityDefinition to the gocql TableMetadata
// There are two main cases, one that we can fix by adding some columns and all the other mismatches that we can't fix
func compareStructToSchema(ed *dosa.EntityDefinition, md *gocql.TableMetadata, schemaErrors *RepairableSchemaMismatchError) error {
	if err := checkPartitionKeys(ed, md); err != nil {
		return err
	}
	if err := checkClusteringKeys(ed, md); err != nil {
		return err
	}

	checkColumns(ed, md, schemaErrors)

	return nil

}

// CheckSchema verifies that the schema passed in the registered entities matches the database
// This implementation only returns 0 or 1, since we are not storing schema
// version information anywhere else
func (c *Connector) CheckSchema(ctx context.Context, scope string, namePrefix string, ed []*dosa.EntityDefinition) (int32, error) {
	schemaErrors := new(RepairableSchemaMismatchError)

	// TODO: unfortunately, gocql doesn't have a way to pass the context to this operation :(
	km, err := c.Session.KeyspaceMetadata(c.KsMapper.Keyspace(scope, namePrefix))
	if err != nil {
		return 0, err
	}
	for _, ed := range ed {
		tableMetadata, ok := km.Tables[ed.Name]
		if !ok {
			schemaErrors.MissingTables = append(schemaErrors.MissingTables, ed.Name)
			continue
		}
		if err := compareStructToSchema(ed, tableMetadata, schemaErrors); err != nil {
			return 0, err
		}

	}
	if schemaErrors.HasMissing() {
		return 0, schemaErrors
	}
	return int32(1), nil
}

// UpsertSchema checks the schema and then updates it
// We handle RepairableSchemaMismatchErrors, and return any
// other error from CheckSchema
func (c *Connector) UpsertSchema(ctx context.Context, scope string, namePrefix string, ed []*dosa.EntityDefinition) (*dosa.SchemaStatus, error) {
	sr, err := c.CheckSchema(ctx, scope, namePrefix, ed)
	if repairs, ok := err.(*RepairableSchemaMismatchError); ok {
		for _, table := range repairs.MissingTables {
			for _, def := range ed {
				if def.Name == table {
					err = c.createTable(c.KsMapper.Keyspace(scope, namePrefix), def)
					if err != nil {
						return nil, errors.Wrapf(err, "Unable to create table %q", def.Name)
					}
				}
			}
		}
		for _, col := range repairs.MissingColumns {
			if err := c.alterTable(c.KsMapper.Keyspace(scope, namePrefix), col.Tablename, col.Column); err != nil {
				return nil, errors.Wrapf(err, "Unable to add column %q to table %q", col.Column.Name, col.Tablename)
			}
		}
	}
	return &dosa.SchemaStatus{Version: sr, Status: "ready"}, err
}

func (c *Connector) alterTable(keyspace, table string, col dosa.ColumnDefinition) error {
	return c.Session.Query(alterTableString(keyspace, table, col)).Exec()
}

func alterTableString(keyspace, table string, col dosa.ColumnDefinition) string {
	cts := &bytes.Buffer{}
	fmt.Fprintf(cts, `ALTER TABLE "%s"."%s" ( ADD "%s" %s )`, keyspace, table, col.Name, cassandraType(col.Type))
	return cts.String()
}

func (c *Connector) createTable(keyspace string, ed *dosa.EntityDefinition) error {
	return c.Session.Query(createTableString(keyspace, ed)).Exec()
}

func createTableString(keyspace string, ed *dosa.EntityDefinition) string {
	stmt, err := CreateStmt(
		Keyspace(keyspace),
		Table(ed.Name),
		ColumnsWithType(ed.Columns),
		PrimaryKey(ed.Key),
	)

	if err != nil {
		panic(err)
	}
	return stmt
}

// ScopeExists is not implemented
func (c *Connector) ScopeExists(ctx context.Context, scope string) (bool, error) {
	panic("not implemented")
}

// CheckSchemaStatus is not implemented
func (c *Connector) CheckSchemaStatus(context.Context, string, string, int32) (*dosa.SchemaStatus, error) {
	panic("not implemented")
}
