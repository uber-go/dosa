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

var (
	testInfo = &dosa.EntityInfo{
		Ref: &dosa.SchemaRef{
			Scope:      "testScope",
			NamePrefix: "testPrefix",
			EntityName: "testEntityName",
		},
		Def: &dosa.EntityDefinition{},
	}
	testValues      = make(map[string]dosa.FieldValue)
	testMultiValues = make([]map[string]dosa.FieldValue, 1)
)

func TestDevNull_CreateIfNotExists(t *testing.T) {
	assert.NoError(t, sut.CreateIfNotExists(context.TODO(), testInfo, testValues))
}

func TestDevNull_Read(t *testing.T) {
	fieldsToRead := make([]string, 1)
	val, err := sut.Read(context.TODO(), testInfo, testValues, fieldsToRead)
	assert.Nil(t, val)
	assert.Error(t, err)
}

func TestDevNull_MultiRead(t *testing.T) {
	fieldsToRead := make([]string, 1)
	v, e := sut.MultiRead(context.TODO(), testInfo, testMultiValues, fieldsToRead)
	assert.NotNil(t, v)
	assert.Nil(t, e)
}

func TestDevNull_Upsert(t *testing.T) {
	err := sut.Upsert(context.TODO(), testInfo, testValues)
	assert.Nil(t, err)
}

func TestDevNull_MultiUpsert(t *testing.T) {
	errs, err := sut.MultiUpsert(context.TODO(), testInfo, testMultiValues)
	assert.NotNil(t, errs)
	assert.Nil(t, err)
}

func TestDevNull_Remove(t *testing.T) {
	err := sut.Remove(context.TODO(), testInfo, testValues)
	assert.Error(t, err)
}

func TestDevNull_MultiRemove(t *testing.T) {
	errs, err := sut.MultiRemove(context.TODO(), testInfo, testMultiValues)
	assert.NotNil(t, errs)
	assert.Nil(t, err)
}

func TestDevNull_Range(t *testing.T) {
	conditions := make([]dosa.Condition, 1)
	fieldsToRead := make([]string, 1)
	vals, _, err := sut.Range(context.TODO(), testInfo, conditions, fieldsToRead, "", 0)
	assert.Nil(t, vals)
	assert.Error(t, err)
}

func TestDevNull_Search(t *testing.T) {
	fieldPairs := make([]string, 1)
	fieldsToRead := make([]string, 1)
	vals, _, err := sut.Search(context.TODO(), testInfo, fieldPairs, fieldsToRead, "", 0)
	assert.Nil(t, vals)
	assert.Error(t, err)
}

func TestDevNull_Scan(t *testing.T) {
	fieldsToRead := make([]string, 1)
	vals, _, err := sut.Scan(context.TODO(), testInfo, fieldsToRead, "", 0)
	assert.Nil(t, vals)
	assert.Error(t, err)
}

func TestDevNull_CheckSchema(t *testing.T) {
	defs := make([]*dosa.EntityDefinition, 4)
	versions, err := sut.CheckSchema(context.TODO(), "testScope", "testPrefix", defs)
	assert.NotNil(t, versions)
	assert.NoError(t, err)
}

func TestDevNull_UpsertSchema(t *testing.T) {
	defs := make([]*dosa.EntityDefinition, 4)
	versions, err := sut.UpsertSchema(context.TODO(), "testScope", "testPrefix", defs)
	assert.NotNil(t, versions)
	assert.NoError(t, err)
}

func TestDevNull_CreateScope(t *testing.T) {
	assert.NoError(t, sut.CreateScope(context.TODO(), ""))
}

func TestDevNull_TruncateScope(t *testing.T) {
	assert.NoError(t, sut.TruncateScope(context.TODO(), ""))
}

func TestDevNull_DropScope(t *testing.T) {
	assert.NoError(t, sut.DropScope(context.TODO(), ""))
}

func TestDevNull_Shutdown(t *testing.T) {
	assert.Nil(t, sut.Shutdown())
}