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

func TestBase_CreateIfNotExists(t *testing.T) {
	assert.Error(t, bc.CreateIfNotExists(context.TODO(), testInfo, testValues))
	assert.NoError(t, bcWNext.CreateIfNotExists(context.TODO(), testInfo, testValues))
}

func TestBase_Read(t *testing.T) {
	fieldsToRead := make([]string, 1)
	_, err := bc.Read(context.TODO(), testInfo, testValues, fieldsToRead)
	assert.Error(t, err)

	val, err := bcWNext.Read(context.TODO(), testInfo, testValues, fieldsToRead)
	assert.Nil(t, val)
	assert.Error(t, err)
}

func TestBase_MultiRead(t *testing.T) {
	fieldsToRead := make([]string, 1)
	_, e := bc.MultiRead(context.TODO(), testInfo, testMultiValues, fieldsToRead)
	assert.Error(t, e)

	v, e := bcWNext.MultiRead(context.TODO(), testInfo, testMultiValues, fieldsToRead)
	assert.NotNil(t, v)
	assert.Nil(t, e)
}

func TestBase_Upsert(t *testing.T) {
	err := bc.Upsert(context.TODO(), testInfo, testValues)
	assert.Error(t, err)

	err = bcWNext.Upsert(context.TODO(), testInfo, testValues)
	assert.Nil(t, err)
}

func TestBase_MultiUpsert(t *testing.T) {
	_, err := bc.MultiUpsert(context.TODO(), testInfo, testMultiValues)
	assert.Error(t, err)

	errs, err := bcWNext.MultiUpsert(context.TODO(), testInfo, testMultiValues)
	assert.NotNil(t, errs)
	assert.Nil(t, err)
}

func TestBase_Remove(t *testing.T) {
	err := bc.Remove(context.TODO(), testInfo, testValues)
	assert.Error(t, err)

	err = bcWNext.Remove(context.TODO(), testInfo, testValues)
	assert.Error(t, err)
}

func TestBase_MultiRemove(t *testing.T) {
	_, err := bc.MultiRemove(context.TODO(), testInfo, testMultiValues)
	assert.Error(t, err)

	errs, err := bcWNext.MultiRemove(context.TODO(), testInfo, testMultiValues)
	assert.NotNil(t, errs)
	assert.Nil(t, err)
}

func TestBase_Range(t *testing.T) {
	conditions := make(map[string][]dosa.Condition)
	fieldsToRead := make([]string, 1)
	_, _, err := bc.Range(context.TODO(), testInfo, conditions, fieldsToRead, "", 0)
	assert.Error(t, err)

	vals, _, err := bcWNext.Range(context.TODO(), testInfo, conditions, fieldsToRead, "", 0)
	assert.Nil(t, vals)
	assert.Error(t, err)
}

func TestBase_Search(t *testing.T) {
	fieldsToRead := make([]string, 1)
	_, _, err := bc.Search(context.TODO(), testInfo, testPairs, fieldsToRead, "", 0)
	assert.Error(t, err)

	vals, _, err := bcWNext.Search(context.TODO(), testInfo, testPairs, fieldsToRead, "", 0)
	assert.Nil(t, vals)
	assert.Error(t, err)
}

func TestBase_Scan(t *testing.T) {
	fieldsToRead := make([]string, 1)
	_, _, err := bc.Scan(context.TODO(), testInfo, fieldsToRead, "", 0)
	assert.Error(t, err)

	vals, _, err := bcWNext.Scan(context.TODO(), testInfo, fieldsToRead, "", 0)
	assert.Nil(t, vals)
	assert.Error(t, err)
}

func TestBase_CheckSchema(t *testing.T) {
	defs := make([]*dosa.EntityDefinition, 4)
	_, err := bc.CheckSchema(context.TODO(), "testScope", "testPrefix", defs)
	assert.Error(t, err)

	versions, err := bcWNext.CheckSchema(context.TODO(), "testScope", "testPrefix", defs)
	assert.NotNil(t, versions)
	assert.NoError(t, err)
}

func TestBase_UpsertSchema(t *testing.T) {
	defs := make([]*dosa.EntityDefinition, 4)
	_, err := bc.UpsertSchema(context.TODO(), "testScope", "testPrefix", defs)
	assert.Error(t, err)

	versions, err := bcWNext.UpsertSchema(context.TODO(), "testScope", "testPrefix", defs)
	assert.NotNil(t, versions)
	assert.NoError(t, err)
}

func TestBase_CreateScope(t *testing.T) {
	assert.Error(t, bc.CreateScope(context.TODO(), ""))
	assert.NoError(t, bcWNext.CreateScope(context.TODO(), ""))
}

func TestBase_TruncateScope(t *testing.T) {
	assert.Error(t, bc.TruncateScope(context.TODO(), ""))
	assert.NoError(t, bcWNext.TruncateScope(context.TODO(), ""))
}

func TestBase_DropScope(t *testing.T) {
	assert.Error(t, bc.DropScope(context.TODO(), ""))
	assert.NoError(t, bcWNext.DropScope(context.TODO(), ""))
}

func TestBase_Shutdown(t *testing.T) {
	assert.Nil(t, bc.Shutdown())
}
