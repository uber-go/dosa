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

package connectors_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors"
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
	testValues      = make(map[string]dosa.FieldValue)
	testMultiValues = make([]map[string]dosa.FieldValue, 1)
)

func TestNoopClient(t *testing.T) {
	assert.True(t, true)
}

func TestNoopClient_CreateIfNotExists(t *testing.T) {
	c := connectors.Noop{}
	assert.NoError(t, c.CreateIfNotExists(context.TODO(), testInfo, testValues))
}

func TestNoopClient_Read(t *testing.T) {
	c := connectors.Noop{}
	fieldsToRead := make([]string, 1)
	val, err := c.Read(context.TODO(), testInfo, testValues, fieldsToRead)
	assert.NotNil(t, val)
	assert.NoError(t, err)
}

func TestNoopClient_MultiRead(t *testing.T) {
	c := connectors.Noop{}
	fieldsToRead := make([]string, 1)
	assert.Panics(t, func() {
		c.MultiRead(context.TODO(), testInfo, testMultiValues, fieldsToRead)
	})
}

func TestNoopClient_Upsert(t *testing.T) {
	c := connectors.Noop{}
	assert.Panics(t, func() {
		c.Upsert(context.TODO(), testInfo, testValues)
	})
}

func TestNoopClient_MultiUpsert(t *testing.T) {
	c := connectors.Noop{}
	assert.Panics(t, func() {
		c.MultiUpsert(context.TODO(), testInfo, testMultiValues)
	})
}

func TestNoopClient_Remove(t *testing.T) {
	c := connectors.Noop{}
	assert.Panics(t, func() {
		c.Remove(context.TODO(), testInfo, testValues)
	})
}

func TestNoopClient_MultiRemove(t *testing.T) {
	c := connectors.Noop{}
	assert.Panics(t, func() {
		c.MultiUpsert(context.TODO(), testInfo, testMultiValues)
	})
}

func TestNoopClient_Range(t *testing.T) {
	c := connectors.Noop{}
	conditions := make([]dosa.Condition, 1)
	fieldsToRead := make([]string, 1)
	assert.Panics(t, func() {
		c.Range(context.TODO(), testInfo, conditions, fieldsToRead, "", 0)
	})
}

func TestNoopClient_Search(t *testing.T) {
	c := connectors.Noop{}
	fieldPairs := make([]string, 1)
	fieldsToRead := make([]string, 1)
	assert.Panics(t, func() {
		c.Search(context.TODO(), testInfo, fieldPairs, fieldsToRead, "", 0)
	})
}

func TestNoopClient_Scan(t *testing.T) {
	c := connectors.Noop{}
	fieldsToRead := make([]string, 1)
	assert.Panics(t, func() {
		c.Scan(context.TODO(), testInfo, fieldsToRead, "", 0)
	})
}

func TestNoopClient_CheckSchema(t *testing.T) {
	c := connectors.Noop{}
	defs := make([]*dosa.EntityDefinition, 4)
	versions, err := c.CheckSchema(context.TODO(), "testScope", "testPrefix", defs)
	assert.NotNil(t, versions)
	assert.NoError(t, err)
}

func TestNoopClient_UpsertSchema(t *testing.T) {
	c := connectors.Noop{}
	defs := make([]*dosa.EntityDefinition, 4)
	versions, err := c.UpsertSchema(context.TODO(), "testScope", "testPrefix", defs)
	assert.NotNil(t, versions)
	assert.NoError(t, err)
}

func TestNoopClient_CreateScope(t *testing.T) {
	c := connectors.Noop{}
	assert.Panics(t, func() {
		c.CreateScope(context.TODO(), "")
	})
}

func TestNoopClient_TruncateScope(t *testing.T) {
	c := connectors.Noop{}
	assert.Panics(t, func() {
		c.TruncateScope(context.TODO(), "")
	})
}

func TestNoopClient_DropScope(t *testing.T) {
	c := connectors.Noop{}
	assert.Panics(t, func() {
		c.DropScope(context.TODO(), "")
	})
}
