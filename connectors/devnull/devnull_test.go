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

package devnull_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/devnull"
)

var sut = devnull.Connector{}

var ctx = context.Background()

var (
	testInfo = &dosa.EntityInfo{
		Ref: &dosa.SchemaRef{
			Scope:      "testScope",
			NamePrefix: "testPrefix",
			EntityName: "testEntityName",
		},
		Def: &dosa.EntityDefinition{},
	}
	testConditions  = make(map[string][]*dosa.Condition)
	testPairs       = dosa.FieldNameValuePair{}
	testValues      = make(map[string]dosa.FieldValue)
	testMultiValues = make([]map[string]dosa.FieldValue, 1)
)

func TestDevNull_CreateIfNotExists(t *testing.T) {
	assert.NoError(t, sut.CreateIfNotExists(ctx, testInfo, testValues))
}

func TestDevNull_Read(t *testing.T) {
	minimumFields := make([]string, 1)
	val, err := sut.Read(ctx, testInfo, testValues, minimumFields)
	assert.Nil(t, val)
	assert.Error(t, err)
}

func TestDevNull_MultiRead(t *testing.T) {
	minimumFields := make([]string, 1)
	v, e := sut.MultiRead(ctx, testInfo, testMultiValues, minimumFields)
	assert.NotNil(t, v)
	assert.Nil(t, e)
}

func TestDevNull_Upsert(t *testing.T) {
	err := sut.Upsert(ctx, testInfo, testValues)
	assert.Nil(t, err)
}

func TestDevNull_MultiUpsert(t *testing.T) {
	errs, err := sut.MultiUpsert(ctx, testInfo, testMultiValues)
	assert.NotNil(t, errs)
	assert.Nil(t, err)
}

func TestDevNull_Remove(t *testing.T) {
	err := sut.Remove(ctx, testInfo, testValues)
	assert.Error(t, err)
}

func TestDevNull_RemoveRange(t *testing.T) {
	err := sut.RemoveRange(ctx, testInfo, testConditions)
	assert.NoError(t, err)
}

func TestDevNull_MultiRemove(t *testing.T) {
	errs, err := sut.MultiRemove(ctx, testInfo, testMultiValues)
	assert.NotNil(t, errs)
	assert.Nil(t, err)
}

func TestDevNull_Range(t *testing.T) {
	minimumFields := make([]string, 1)
	vals, _, err := sut.Range(ctx, testInfo, testConditions, minimumFields, "", 0)
	assert.Nil(t, vals)
	assert.Error(t, err)
}

func TestDevNull_Search(t *testing.T) {
	minimumFields := make([]string, 1)
	vals, _, err := sut.Search(ctx, testInfo, testPairs, minimumFields, "", 0)
	assert.Nil(t, vals)
	assert.Error(t, err)
}

func TestDevNull_Scan(t *testing.T) {
	minimumFields := make([]string, 1)
	vals, _, err := sut.Scan(ctx, testInfo, minimumFields, "", 0)
	assert.Nil(t, vals)
	assert.Error(t, err)
}

func TestDevNull_CheckSchema(t *testing.T) {
	defs := make([]*dosa.EntityDefinition, 4)
	versions, err := sut.CheckSchema(ctx, "testScope", "testPrefix", defs)
	assert.NotNil(t, versions)
	assert.NoError(t, err)
}

func TestDevNull_CheckSchemaStatus(t *testing.T) {
	status, err := sut.CheckSchemaStatus(ctx, "testScope", "testPrefix", int32(1))
	assert.NotNil(t, status)
	assert.NoError(t, err)
}

func TestDevNull_UpsertSchema(t *testing.T) {
	defs := make([]*dosa.EntityDefinition, 4)
	status, err := sut.UpsertSchema(ctx, "testScope", "testPrefix", defs)
	assert.NotNil(t, status)
	assert.NoError(t, err)
}

func TestDevNull_CreateScope(t *testing.T) {
	assert.NoError(t, sut.CreateScope(ctx, ""))
}

func TestDevNull_TruncateScope(t *testing.T) {
	assert.NoError(t, sut.TruncateScope(ctx, ""))
}

func TestDevNull_DropScope(t *testing.T) {
	assert.NoError(t, sut.DropScope(ctx, ""))
}

func TestDevNull_ScopeExists(t *testing.T) {
	e, err := sut.ScopeExists(ctx, "")
	assert.NoError(t, err)
	assert.True(t, e)
}

func TestDevNull_Shutdown(t *testing.T) {
	assert.Nil(t, sut.Shutdown())
}
