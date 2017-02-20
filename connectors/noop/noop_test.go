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

package noop_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/noop"
)

func TestNoopClient(t *testing.T) {
	assert.True(t, true)
}

func TestNoopClient_CreateIfNotExists(t *testing.T) {
	c := noop.Connector{}
	values := make(map[string]dosa.FieldValue)
	assert.NoError(t, c.CreateIfNotExists(context.TODO(), "", values))
}

func TestNoopClient_Read(t *testing.T) {
	c := noop.Connector{}
	keys := make(map[string]dosa.FieldValue)
	fieldsToRead := make([]string, 1)
	val, err := c.Read(context.TODO(), "", keys, fieldsToRead)
	assert.NoError(t, err)
	assert.NotNil(t, val)
}

func TestNoopClient_BatchRead(t *testing.T) {
	c := noop.Connector{}
	keys := make([]map[string]dosa.FieldValue, 1)
	fieldsToRead := make([]string, 1)
	assert.Panics(t, func() {
		c.BatchRead(context.TODO(), "", keys, fieldsToRead)
	})
}

func TestNoopClient_Upsert(t *testing.T) {
	c := noop.Connector{}
	keys := make(map[string]dosa.FieldValue)
	fieldsToUpdate := make([]string, 1)
	assert.Panics(t, func() {
		c.Upsert(context.TODO(), "", keys, fieldsToUpdate)
	})
}

func TestNoopClient_BatchUpsert(t *testing.T) {
	c := noop.Connector{}
	keys := make([]map[string]dosa.FieldValue, 1)
	fieldsToUpsert := make([]string, 1)
	assert.Panics(t, func() {
		c.BatchUpsert(context.TODO(), "", keys, fieldsToUpsert)
	})
}

func TestNoopClient_Remove(t *testing.T) {
	c := noop.Connector{}
	keys := make(map[string]dosa.FieldValue)
	assert.Panics(t, func() {
		c.Remove(context.TODO(), "", keys)
	})
}

func TestNoopClient_Range(t *testing.T) {
	c := noop.Connector{}
	conditions := make([]dosa.Condition, 1)
	fieldsToRead := make([]string, 1)
	assert.Panics(t, func() {
		c.Range(context.TODO(), "", conditions, fieldsToRead, "", 0)
	})
}

func TestNoopClient_Search(t *testing.T) {
	c := noop.Connector{}
	fieldPairs := make([]string, 1)
	fieldsToRead := make([]string, 1)
	assert.Panics(t, func() {
		c.Search(context.TODO(), "", fieldPairs, fieldsToRead, "", 0)
	})
}

func TestNoopClient_Scan(t *testing.T) {
	c := noop.Connector{}
	fieldsToRead := make([]string, 1)
	assert.Panics(t, func() {
		c.Scan(context.TODO(), "", fieldsToRead, "", 0)
	})
}

func TestNoopClient_CheckSchema(t *testing.T) {
	c := noop.Connector{}
	defs := make([]*dosa.EntityDefinition, 4)
	refs, err := c.CheckSchema(context.TODO(), defs)
	assert.NoError(t, err)
	assert.NotNil(t, refs)
}

func TestNoopClient_UpsertSchema(t *testing.T) {
	c := noop.Connector{}
	defs := make([]*dosa.EntityDefinition, 4)
	err := c.UpsertSchema(context.TODO(), defs)
	assert.NoError(t, err)
}

func TestNoopClient_CreateScope(t *testing.T) {
	c := noop.Connector{}
	assert.Panics(t, func() {
		c.CreateScope(context.TODO(), "")
	})
}

func TestNoopClient_TruncateScope(t *testing.T) {
	c := noop.Connector{}
	assert.Panics(t, func() {
		c.TruncateScope(context.TODO(), "")
	})
}

func TestNoopClient_DropScope(t *testing.T) {
	c := noop.Connector{}
	assert.Panics(t, func() {
		c.DropScope(context.TODO(), "")
	})
}
