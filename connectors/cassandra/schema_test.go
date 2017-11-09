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
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/testentity"
)

func TestCompareStructToSchemaWrongPk(t *testing.T) {
	ed := dosa.EntityDefinition{Key: &dosa.PrimaryKey{
		PartitionKeys: []string{"p1"}},
		Name: "test",
		Columns: []*dosa.ColumnDefinition{
			{Name: "p1", Type: dosa.String},
			{Name: "c1", Type: dosa.String},
		},
	}
	md := gocql.TableMetadata{PartitionKey: []*gocql.ColumnMetadata{
		{Name: "c1", Type: TestType{typ: gocql.TypeVarchar}},
	},
		Columns: map[string]*gocql.ColumnMetadata{
			"p1": {Name: "p1", Type: TestType{typ: gocql.TypeVarchar}},
		}}
	missing := RepairableSchemaMismatchError{}
	err := compareStructToSchema(&ed, &md, &missing)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `"test"`)
}

// test that the cluster key in gocql doesn't match cluster key in dosa definition. c1 != c2
func TestCompareStructToSchemaWrongCk(t *testing.T) {
	ed := dosa.EntityDefinition{
		Key: &dosa.PrimaryKey{
			PartitionKeys: []string{"p1"},
			ClusteringKeys: []*dosa.ClusteringKey{{Name:"c1", Descending: true}},
		},
		Name: "test",
		Columns: []*dosa.ColumnDefinition{
			{Name: "p1", Type: dosa.String},
			{Name: "c1", Type: dosa.String},
		},
	}
	md := gocql.TableMetadata{
		PartitionKey: []*gocql.ColumnMetadata{
			{Name: "p1", Type: TestType{typ: gocql.TypeText}},
		},
		ClusteringColumns: []*gocql.ColumnMetadata{
			{Name: "c2", Type: TestType{typ: gocql.TypeText}},
		},
		Columns: map[string]*gocql.ColumnMetadata{
			"p1": {Name: "p1", Type: TestType{typ: gocql.TypeText}},
		}}
	missing := RepairableSchemaMismatchError{}
	err := compareStructToSchema(&ed, &md, &missing)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `"test"`)
}

func TestCompareStructToSchemaMissingColumn(t *testing.T) {
	ed := dosa.EntityDefinition{Key: &dosa.PrimaryKey{
		PartitionKeys: []string{"p1"}},
		Name: "test",
		Columns: []*dosa.ColumnDefinition{
			{Name: "p1", Type: dosa.String},
			{Name: "c1", Type: dosa.String},
		},
	}
	md := gocql.TableMetadata{PartitionKey: []*gocql.ColumnMetadata{
		{Name: "p1", Type: TestType{typ: gocql.TypeText}},
	},
		Columns: map[string]*gocql.ColumnMetadata{
			"p1": {Name: "p1", Type: TestType{typ: gocql.TypeText}},
		}}
	missing := RepairableSchemaMismatchError{}
	err := compareStructToSchema(&ed, &md, &missing)
	assert.NoError(t, err)
	assert.True(t, missing.HasMissing())
	assert.Equal(t, 1, len(missing.MissingColumns))
	assert.Equal(t, "test", missing.MissingColumns[0].Tablename)
	assert.Equal(t, "c1", missing.MissingColumns[0].Column.Name)
	assert.Contains(t, missing.Error(), "Missing 1 column")
}

// dosa string type is a gocql test type, not varchar
func TestCompareStructToSchemaMissingColumnType(t *testing.T) {
	ed := dosa.EntityDefinition{Key: &dosa.PrimaryKey{
		PartitionKeys: []string{"pk"}},
		Name: "test",
		Columns: []*dosa.ColumnDefinition{
			{Name: "pk", Type: dosa.String},
		},
	}
	md := gocql.TableMetadata{
		PartitionKey: []*gocql.ColumnMetadata{
			{Name: "pk", Type: TestType{typ: gocql.TypeVarchar}},
		},
		Columns: map[string]*gocql.ColumnMetadata{
			"pk": {Name: "pk", Type: TestType{typ: gocql.TypeVarchar}},
		},
	}
	missing := RepairableSchemaMismatchError{}
	err := compareStructToSchema(&ed, &md, &missing)
	assert.NoError(t, err)
	assert.True(t, missing.HasMissing())
	assert.Equal(t, 1, len(missing.MissingColumns))
	assert.Equal(t, "test", missing.MissingColumns[0].Tablename)
	assert.Equal(t, "pk", missing.MissingColumns[0].Column.Name)
	assert.Contains(t, missing.Error(), "Missing 1 column")
}

func TestCompareStructToSchema(t *testing.T) {
	ed := dosa.EntityDefinition{Key: &dosa.PrimaryKey{
		PartitionKeys: []string{"p1"}},
		Name: "test",
		Columns: []*dosa.ColumnDefinition{
			{Name: "p1", Type: dosa.Int32},
		},
	}
	md := gocql.TableMetadata{
		PartitionKey: []*gocql.ColumnMetadata{
			{Name: "p1", Type: TestType{typ: gocql.TypeInt}},
		},
		Columns: map[string]*gocql.ColumnMetadata{
			"p1": {Name: "p1", Type: TestType{typ: gocql.TypeInt}},
		},
	}
	missing := RepairableSchemaMismatchError{}
	err := compareStructToSchema(&ed, &md, &missing)
	assert.NoError(t, err)
	assert.False(t, missing.HasMissing())
}

type TestType struct {
	typ gocql.Type
}

func (t TestType) New() interface{} {
	panic("not implemented")
}
func (t TestType) Version() byte {
	panic("not implemented")
}
func (t TestType) Custom() string {
	panic("not implemented")
}
func (t TestType) Type() gocql.Type {
	return t.typ
}

func TestCreateTableString(t *testing.T) {
	// grab this full-featured entity from the code
	testEntity, _ := dosa.TableFromInstance(&testentity.TestEntity{})
	testCases := []struct {
		name     string
		ed       dosa.EntityDefinition
		expected string
	}{
		{"simple",
			dosa.EntityDefinition{Key: &dosa.PrimaryKey{
				PartitionKeys: []string{"c1"}},
				Name: "t1",
				Columns: []*dosa.ColumnDefinition{
					{Name: "c1", Type: dosa.String},
				},
			},
			`CREATE TABLE "keyspace"."t1" (` +
				`"c1" text, ` +
				`PRIMARY KEY (("c1"))) ` +
				`WITH COMPACTION = {'class':'LeveledCompactionStrategy'}`,
		},
		{"testentity",
			testEntity.EntityDefinition,
			`CREATE TABLE "keyspace"."awesome_test_entity" (` +
				`"an_uuid_key" uuid, ` +
				`"strkey" text, ` +
				`"int64key" bigint, ` +
				`"uuidv" uuid, ` +
				`"strv" text, ` +
				`"an_int64_value" bigint, ` +
				`"int32v" int, ` +
				`"doublev" double, ` +
				`"boolv" boolean, ` +
				`"blobv" blob, ` +
				`"tsv" timestamp, ` +
				`"uuidvp" uuid, ` +
				`"strvp" text, ` +
				`"int64vp" bigint, ` +
				`"int32vp" int, ` +
				`"doublevp" double, ` +
				`"boolvp" boolean, ` +
				`"tsvp" timestamp, ` +
				`PRIMARY KEY (("an_uuid_key"),"strkey","int64key")) ` +
				`WITH CLUSTERING ORDER BY ("strkey" ASC,"int64key" DESC) ` +
				`AND COMPACTION = {'class':'LeveledCompactionStrategy'}`},
	}
	for _, testcase := range testCases {
		t.Run(testcase.name, func(t *testing.T) {
			assert.Equal(t, testcase.expected, createTableString("keyspace", &testcase.ed))
		})
	}
}
