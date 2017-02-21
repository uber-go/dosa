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

package connectors

import (
	"context"

	"github.com/uber-go/dosa"
)

// Noop is a connector implementation for testing
type Noop struct{}

// CreateIfNotExists always returns nil
func (c *Noop) CreateIfNotExists(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	return nil
}

// Read always returns an empty resultset
func (c *Noop) Read(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue, fieldsToRead []string) (map[string]dosa.FieldValue, error) {
	results := make(map[string]dosa.FieldValue)
	results["id"] = int64(1)
	results["name"] = "updated name"
	results["email"] = "updated@email.com"
	return results, nil
}

// MultiRead is not yet implemented
func (c *Noop) MultiRead(ctx context.Context, ei *dosa.EntityInfo, values []map[string]dosa.FieldValue, fieldsToRead []string) ([]dosa.FieldValuesOrError, error) {
	panic("not implemented")
}

// Upsert is not yet implemented
func (c *Noop) Upsert(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	panic("not implemented")
}

// MultiUpsert is not yet implemented
func (c *Noop) MultiUpsert(ctx context.Context, ei *dosa.EntityInfo, values []map[string]dosa.FieldValue) ([]error, error) {
	panic("not implemented")
}

// Remove is not yet implemented
func (c *Noop) Remove(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	panic("not implemented")
}

// MultiRemove is not yet implemented
func (c *Noop) MultiRemove(ctx context.Context, ei *dosa.EntityInfo, multiValues []map[string]dosa.FieldValue) ([]error, error) {
	panic("not implemented")
}

// Range is not yet implemented
func (c *Noop) Range(ctx context.Context, ei *dosa.EntityInfo, conditions []dosa.Condition, fieldsToRead []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	panic("not implemented")
}

// Search is not yet implemented
func (c *Noop) Search(ctx context.Context, ei *dosa.EntityInfo, FieldNameValuePair []string, fieldsToRead []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	panic("not implemented")
}

// Scan is not yet implemented
func (c *Noop) Scan(ctx context.Context, ei *dosa.EntityInfo, fieldsToRead []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	panic("not implemented")
}

// CheckSchema always returns a slice of int32 values that match its index
func (c *Noop) CheckSchema(ctx context.Context, scope, namePrefix string, ed []*dosa.EntityDefinition) ([]int32, error) {
	versions := make([]int32, len(ed))
	for idx := range versions {
		versions[idx] = int32(idx)
	}
	return versions, nil
}

// UpsertSchema always returns a slice of int32 values that match its index
func (c *Noop) UpsertSchema(ctx context.Context, scope, namePrefix string, ed []*dosa.EntityDefinition) ([]int32, error) {
	versions := make([]int32, len(ed))
	for idx := range versions {
		versions[idx] = int32(idx)
	}
	return versions, nil
}

// CreateScope is not implemented yet
func (c *Noop) CreateScope(ctx context.Context, scope string) error {
	panic("not implemented")
}

// TruncateScope is not implemented yet
func (c *Noop) TruncateScope(ctx context.Context, scope string) error {
	panic("not implemented")
}

// DropScope is not implemented yet
func (c *Noop) DropScope(ctx context.Context, scope string) error {
	panic("not implemented")
}

// Shutdown always returns nil
func (c Noop) Shutdown() error {
	return nil
}
