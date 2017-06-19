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
		{Name: "p1", Type: TestType{typ: gocql.TypeVarchar}},
	},
		Columns: map[string]*gocql.ColumnMetadata{
			"p1": {Name: "p1", Type: TestType{typ: gocql.TypeVarchar}},
		}}
	missing := RepairableSchemaMismatchError{}
	err := compareStructToSchema(&ed, &md, &missing)
	assert.Nil(t, err)
	assert.True(t, missing.HasMissing())
	assert.Equal(t, 1, len(missing.MissingColumns))
	assert.Equal(t, "test", missing.MissingColumns[0].Tablename)
	assert.Equal(t, "c1", missing.MissingColumns[0].Column.Name)
	assert.Contains(t, missing.Error(), "Missing 1 column")
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
