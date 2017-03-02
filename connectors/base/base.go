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

package base

import (
	"context"

	"github.com/uber-go/dosa"
)

// connector always calls next connector in all the functions
type connector struct {
	next dosa.Connector
}

// NewConnector creates new base connector
func NewConnector(next dosa.Connector) dosa.Connector {
	return &connector{next: next}
}

// CreateIfNotExists calls next
func (c *connector) CreateIfNotExists(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	return c.next.CreateIfNotExists(ctx, ei, values)
}

// Read calls next
func (c *connector) Read(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue, fieldsToRead []string) (map[string]dosa.FieldValue, error) {
	return c.next.Read(ctx, ei, values, fieldsToRead)
}

// MultiRead calls next
func (c *connector) MultiRead(ctx context.Context, ei *dosa.EntityInfo, values []map[string]dosa.FieldValue, fieldsToRead []string) ([]*dosa.FieldValuesOrError, error) {
	return c.next.MultiRead(ctx, ei	, values, fieldsToRead)
}

// Upsert calls next
func (c *connector) Upsert(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	return c.next.Upsert(ctx, ei, values)
}

// MultiUpsert calls next
func (c *connector) MultiUpsert(ctx context.Context, ei *dosa.EntityInfo, values []map[string]dosa.FieldValue) ([]error, error) {
	return c.next.MultiUpsert(ctx, ei, values)
}

// Remove calls next
func (c *connector) Remove(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	return c.next.Remove(ctx, ei, values)
}

// MultiRemove calls next
func (c *connector) MultiRemove(ctx context.Context, ei *dosa.EntityInfo, multiValues []map[string]dosa.FieldValue) ([]error, error) {
	return c.next.MultiRemove(ctx, ei, multiValues)
}

// Range calls next
func (c *connector) Range(ctx context.Context, ei *dosa.EntityInfo, columnConditions map[string][]dosa.Condition, fieldsToRead []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	return c.next.Range(ctx, ei, columnConditions, fieldsToRead, token, limit)
}

// Search calls next
func (c *connector) Search(ctx context.Context, ei *dosa.EntityInfo, fieldPairs dosa.FieldNameValuePair, fieldsToRead []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	return c.next.Search(ctx, ei, fieldPairs, fieldsToRead, token, limit)
}

// Scan calls next
func (c *connector) Scan(ctx context.Context, ei *dosa.EntityInfo, fieldsToRead []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	return c.next.Scan(ctx, ei, fieldsToRead, token, limit)
}

// CheckSchema calls next
func (c *connector) CheckSchema(ctx context.Context, scope, namePrefix string, ed []*dosa.EntityDefinition) ([]int32, error) {
	return c.next.CheckSchema(ctx, scope, namePrefix, ed)
}

// UpsertSchema calls next
func (c *connector) UpsertSchema(ctx context.Context, scope, namePrefix string, ed []*dosa.EntityDefinition) ([]int32, error) {
	return c.next.UpsertSchema(ctx, scope, namePrefix, ed)
}

// CreateScope calls next
func (c *connector) CreateScope(ctx context.Context, scope string) error {
	return c.next.CreateScope(ctx, scope)
}

// TruncateScope calls next
func (c *connector) TruncateScope(ctx context.Context, scope string) error {
	return c.next.TruncateScope(ctx, scope)
}

// DropScope calls next
func (c *connector) DropScope(ctx context.Context, scope string) error {
	return c.next.DropScope(ctx, scope)
}

// ScopeExists calls next
func (c *connector) ScopeExists(ctx context.Context, scope string) (bool, error) {
	return c.next.ScopeExists(ctx, scope)
}

// Shutdown always returns nil
func (c *connector) Shutdown() error {
	return nil
}

