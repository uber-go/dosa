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

package base_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/base"
	"github.com/uber-go/dosa/connectors/devnull"
)

var (
	bc      = base.Connector{}
	dl      = devnull.Connector{}
	bcWNext = base.Connector{Next: &dl}
)

var (
	testInfo = &dosa.EntityInfo{
		Ref: &dosa.SchemaRef{
			Scope:      "testScope",
			NamePrefix: "testPrefix",
			EntityName: "testEntityName",
		},
		Def: &dosa.EntityDefinition{},
	}
	testPairs       = dosa.FieldNameValuePair{}
	testValues      = make(map[string]dosa.FieldValue)
	testMultiValues = make([]map[string]dosa.FieldValue, 1)
)

var ctx = context.Background()

func TestBase_CreateIfNotExists(t *testing.T) {
	assert.Error(t, bc.CreateIfNotExists(ctx, testInfo, testValues))
	assert.NoError(t, bcWNext.CreateIfNotExists(ctx, testInfo, testValues))
}

func TestBase_Read(t *testing.T) {
	minimumFields := make([]string, 1)
	_, err := bc.Read(ctx, testInfo, testValues, minimumFields)
	assert.Error(t, err)

	val, err := bcWNext.Read(ctx, testInfo, testValues, minimumFields)
	assert.Nil(t, val)
	assert.Error(t, err)
}

func TestBase_MultiRead(t *testing.T) {
	minimumFields := make([]string, 1)
	_, e := bc.MultiRead(ctx, testInfo, testMultiValues, minimumFields)
	assert.Error(t, e)

	v, e := bcWNext.MultiRead(ctx, testInfo, testMultiValues, minimumFields)
	assert.NotNil(t, v)
	assert.Nil(t, e)
}

func TestBase_Upsert(t *testing.T) {
	err := bc.Upsert(ctx, testInfo, testValues)
	assert.Error(t, err)

	err = bcWNext.Upsert(ctx, testInfo, testValues)
	assert.Nil(t, err)
}

func TestBase_MultiUpsert(t *testing.T) {
	_, err := bc.MultiUpsert(ctx, testInfo, testMultiValues)
	assert.Error(t, err)

	errs, err := bcWNext.MultiUpsert(ctx, testInfo, testMultiValues)
	assert.NotNil(t, errs)
	assert.Nil(t, err)
}

func TestBase_Remove(t *testing.T) {
	err := bc.Remove(ctx, testInfo, testValues)
	assert.Error(t, err)

	err = bcWNext.Remove(ctx, testInfo, testValues)
	assert.NoError(t, err)
}

func TestBase_RemoveRange(t *testing.T) {
	conditions := make(map[string][]*dosa.Condition)
	err := bc.RemoveRange(ctx, testInfo, conditions)
	assert.Error(t, err)

	err = bcWNext.RemoveRange(ctx, testInfo, conditions)
	assert.NoError(t, err)
}

func TestBase_MultiRemove(t *testing.T) {
	_, err := bc.MultiRemove(ctx, testInfo, testMultiValues)
	assert.Error(t, err)

	errs, err := bcWNext.MultiRemove(ctx, testInfo, testMultiValues)
	assert.NotNil(t, errs)
	assert.Nil(t, err)
}

func TestBase_Range(t *testing.T) {
	conditions := make(map[string][]*dosa.Condition)
	minimumFields := make([]string, 1)
	_, _, err := bc.Range(ctx, testInfo, conditions, minimumFields, "", 0)
	assert.Error(t, err)

	vals, _, err := bcWNext.Range(ctx, testInfo, conditions, minimumFields, "", 0)
	assert.Nil(t, vals)
	assert.Error(t, err)
}

func TestBase_Scan(t *testing.T) {
	minimumFields := make([]string, 1)
	_, _, err := bc.Scan(ctx, testInfo, minimumFields, "", 0)
	assert.Error(t, err)

	vals, _, err := bcWNext.Scan(ctx, testInfo, minimumFields, "", 0)
	assert.Nil(t, vals)
	assert.Error(t, err)
}

func TestBase_CheckSchema(t *testing.T) {
	defs := make([]*dosa.EntityDefinition, 4)
	_, err := bc.CheckSchema(ctx, "testScope", "testPrefix", defs)
	assert.Error(t, err)

	version, err := bcWNext.CheckSchema(ctx, "testScope", "testPrefix", defs)
	assert.NotNil(t, version)
	assert.NoError(t, err)
}

func TestBase_CheckSchemaToUpsert(t *testing.T) {
	defs := make([]*dosa.EntityDefinition, 4)
	_, err := bc.CheckSchema(ctx, "testScope", "testPrefix", defs)
	assert.Error(t, err)

	version, err := bcWNext.CheckSchemaToUpsert(ctx, "testScope", "testPrefix", defs)
	assert.NotNil(t, version)
	assert.NoError(t, err)
}

func TestBase_UpsertSchema(t *testing.T) {
	defs := make([]*dosa.EntityDefinition, 4)
	_, err := bc.UpsertSchema(ctx, "testScope", "testPrefix", defs)
	assert.Error(t, err)

	status, err := bcWNext.UpsertSchema(ctx, "testScope", "testPrefix", defs)
	assert.NotNil(t, status)
	assert.NoError(t, err)
}

func TestBase_CreateScope(t *testing.T) {
	assert.Error(t, bc.CreateScope(ctx, ""))
	assert.NoError(t, bcWNext.CreateScope(ctx, ""))
}

func TestBase_TruncateScope(t *testing.T) {
	assert.Error(t, bc.TruncateScope(ctx, ""))
	assert.NoError(t, bcWNext.TruncateScope(ctx, ""))
}

func TestBase_DropScope(t *testing.T) {
	assert.Error(t, bc.DropScope(ctx, ""))
	assert.NoError(t, bcWNext.DropScope(ctx, ""))
}

func TestBase_Shutdown(t *testing.T) {
	assert.Error(t, bc.Shutdown())
	assert.NoError(t, bcWNext.Shutdown())
}

func TestBase_CheckSchemaStatus(t *testing.T) {
	_, err := bc.CheckSchemaStatus(ctx, "testScope", "testPrefix", int32(1))
	assert.Error(t, err)

	versions, err := bcWNext.CheckSchemaStatus(ctx, "testScope", "testPrefix", int32(1))
	assert.NotNil(t, versions)
	assert.NoError(t, err)
}
