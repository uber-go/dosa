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

package cql

import (
	"fmt"
	"testing"

	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
)

type AllTypes struct {
	dosa.Entity `dosa:"primaryKey=BoolType"`
	I1          dosa.Index `dosa:"key=Int32Type"`
	I2          dosa.Index `dosa:"key=Int64Type"`
	BoolType    bool
	Int32Type   int32
	Int64Type   int64
	DoubleType  float64
	StringType  string
	BlobType    []byte
	TimeType    time.Time
	UUIDType    dosa.UUID
}

type SinglePrimaryKey struct {
	dosa.Entity `dosa:"primaryKey=(PrimaryKey)"`
	PrimaryKey  int64
	Data        string
}

func TestCQL(t *testing.T) {
	data := []struct {
		Instance  dosa.DomainObject
		Statement string
	}{
		{
			Instance:  &SinglePrimaryKey{},
			Statement: `create table "singleprimarykey" ("primarykey" bigint, "data" text, primary key (primarykey));`,
		},
		{
			Instance: &AllTypes{},
			Statement: `create table "alltypes" ("booltype" boolean, "int32type" int, "int64type" bigint, "doubletype" double, "stringtype" text, "blobtype" blob, "timetype" timestamp, "uuidtype" uuid, primary key (booltype));
create materialized view "i1" as
  select * from "alltypes"
  where "int32type" is not null
  primary key (int32type, booltype ASC);
create materialized view "i2" as
  select * from "alltypes"
  where "int64type" is not null
  primary key (int64type, booltype ASC);`,
		},
		// TODO: Add more test cases
	}

	for _, d := range data {
		table, err := dosa.TableFromInstance(d.Instance)
		assert.Nil(t, err) // this code does not test TableFromInstance
		statement := ToCQL(&table.EntityDefinition)
		assert.Equal(t, d.Statement, statement, fmt.Sprintf("Instance: %T", d.Instance))
	}
}

func TestSelectFieldsInCreatingView(t *testing.T) {
	data := []struct {
		Columns   []string
		Statement string
	}{
		{
			Columns:   []string{"foo", "bar", "hello"},
			Statement: `"foo", "bar", "hello"`,
		},
		{
			Columns:   []string{"foo"},
			Statement: `"foo"`,
		},
		{
			Columns:   []string{},
			Statement: "*",
		},
	}
	for _, d := range data {
		statement := selectFieldsInCreatingView(d.Columns)
		assert.Equal(t, d.Statement, statement)
	}
}

func BenchmarkCQL(b *testing.B) {
	table, _ := dosa.TableFromInstance(&AllTypes{})
	for i := 0; i < b.N; i++ {
		ToCQL(&table.EntityDefinition)
	}
}

func TestTypemapUnknown(t *testing.T) {
	assert.Equal(t, "unknown", typeMap(dosa.Invalid))
}
