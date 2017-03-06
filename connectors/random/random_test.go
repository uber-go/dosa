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

package random_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/random"
	"time"
)

var sut = random.Connector{}

type AllTypes struct {
	dosa.Entity `dosa:"primaryKey=BoolType"`
	BoolType    bool
	Int32Type   int32
	Int64Type   int64
	DoubleType  float64
	StringType  string
	BlobType    []byte
	TimeType    time.Time
	UUIDType    dosa.UUID
}

var (
	testTable, _ = dosa.TableFromInstance((*AllTypes)(nil))
	testInfo     = &dosa.EntityInfo{
		Def: &testTable.EntityDefinition,
		Ref: &dosa.SchemaRef{
			Scope:      "testScope",
			NamePrefix: "testPrefix",
			EntityName: "testEntityName",
		},
	}
	testPairs       = dosa.FieldNameValuePair{}
	testValues      = make(map[string]dosa.FieldValue)
	testMultiValues = make([]map[string]dosa.FieldValue, 50)
	fieldsToRead    = []string{"booltype", "int32type", "int64type", "doubletype", "stringtype", "blobtype", "timetype", "uuidtype"}
)

func TestRandom_CreateIfNotExists(t *testing.T) {
	assert.NoError(t, sut.CreateIfNotExists(context.TODO(), testInfo, testValues))
}

func TestRandom_Read(t *testing.T) {
	val, err := sut.Read(context.TODO(), testInfo, testValues, fieldsToRead)
	assert.NoError(t, err)
	assert.NotNil(t, val)
	for _, field := range fieldsToRead {
		assert.NotNil(t, val[field])
	}
}

func TestRandom_MultiRead(t *testing.T) {
	v, e := sut.MultiRead(context.TODO(), testInfo, testMultiValues, fieldsToRead)
	assert.NotNil(t, v)
	assert.Nil(t, e)
	assert.Equal(t, len(testMultiValues), len(v))
	for i := range v {
		for _, field := range fieldsToRead {
			assert.NotNil(t, v[i].Values[field])
		}
	}
}

func TestRandom_Upsert(t *testing.T) {
	err := sut.Upsert(context.TODO(), testInfo, testValues)
	assert.Nil(t, err)
}

func TestRandom_MultiUpsert(t *testing.T) {
	errs, err := sut.MultiUpsert(context.TODO(), testInfo, testMultiValues)
	assert.NotNil(t, errs)
	assert.Nil(t, err)
}

func TestRandom_Remove(t *testing.T) {
	err := sut.Remove(context.TODO(), testInfo, testValues)
	assert.Error(t, err)
}

func TestRandom_MultiRemove(t *testing.T) {
	errs, err := sut.MultiRemove(context.TODO(), testInfo, testMultiValues)
	assert.NotNil(t, errs)
	assert.Nil(t, err)
}

func TestRandom_Range(t *testing.T) {
	conditions := make(map[string][]*dosa.Condition)
	vals, _, err := sut.Range(context.TODO(), testInfo, conditions, fieldsToRead, "", 32)
	assert.NotNil(t, vals)
	assert.NoError(t, err)
}

func TestRandom_Search(t *testing.T) {
	vals, _, err := sut.Search(context.TODO(), testInfo, testPairs, fieldsToRead, "", 32)
	assert.NotNil(t, vals)
	assert.NoError(t, err)
}

func TestRandom_Scan(t *testing.T) {
	vals, _, err := sut.Scan(context.TODO(), testInfo, fieldsToRead, "", 32)
	assert.NotNil(t, vals)
	assert.NoError(t, err)
}

func TestRandom_CheckSchema(t *testing.T) {
	defs := make([]*dosa.EntityDefinition, 4)
	versions, err := sut.CheckSchema(context.TODO(), "testScope", "testPrefix", defs)
	assert.NotNil(t, versions)
	assert.NoError(t, err)
}

func TestRandom_UpsertSchema(t *testing.T) {
	defs := make([]*dosa.EntityDefinition, 4)
	versions, err := sut.UpsertSchema(context.TODO(), "testScope", "testPrefix", defs)
	assert.NotNil(t, versions)
	assert.NoError(t, err)
}

func TestRandom_CreateScope(t *testing.T) {
	assert.NoError(t, sut.CreateScope(context.TODO(), ""))
}

func TestRandom_TruncateScope(t *testing.T) {
	assert.NoError(t, sut.TruncateScope(context.TODO(), ""))
}

func TestRandom_DropScope(t *testing.T) {
	assert.NoError(t, sut.DropScope(context.TODO(), ""))
}

func TestRandom_ScopeExists(t *testing.T) {
	exists, err := sut.ScopeExists(context.TODO(), "")
	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestRandom_Shutdown(t *testing.T) {
	assert.Nil(t, sut.Shutdown())
}

// this test is primarily just for 100% coverage
func TestRandom_badTypePanic(t *testing.T) {
	testInfo.Def.Columns[0].Type = dosa.Invalid
	assert.Panics(t, func() {
		random.Data(testInfo, fieldsToRead)
	})
}
