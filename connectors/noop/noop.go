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

package noop

import (
	"context"

	"github.com/satori/go.uuid"
	"github.com/uber-go/dosa"
)

// Connector is a connector implementation for testing
type Connector struct{}

// CreateIfNotExists always returns nil
func (c *Connector) CreateIfNotExists(ctx context.Context, sr dosa.SchemaReference, values map[string]dosa.FieldValue) error {
	return nil
}

// Read always returns an empty resultset
func (c *Connector) Read(ctx context.Context, sr dosa.SchemaReference, keys map[string]dosa.FieldValue, fieldsToRead []string) (map[string]dosa.FieldValue, error) {
	values := make(map[string]dosa.FieldValue)
	values["ID"] = int64(1)
	values["Name"] = "test"
	values["Email"] = "email"
	return values, nil
}

// BatchRead is not yet implemented
func (c *Connector) BatchRead(ctx context.Context, sr dosa.SchemaReference, keys []map[string]dosa.FieldValue, fieldsToRead []string) ([]dosa.FieldValuesOrError, error) {
	panic("not implemented")
}

// Upsert is not yet implemented
func (c *Connector) Upsert(ctx context.Context, sr dosa.SchemaReference, keys map[string]dosa.FieldValue, fieldsToUpdate []string) error {
	panic("not implemented")
}

// BatchUpsert is not yet implemented
func (c *Connector) BatchUpsert(ctx context.Context, sr dosa.SchemaReference, keys []map[string]dosa.FieldValue, fieldsToUpdate []string) ([]error, error) {
	panic("not implemented")
}

// Remove is not yet implemented
func (c *Connector) Remove(ctx context.Context, sr dosa.SchemaReference, keys map[string]dosa.FieldValue) error {
	panic("not implemented")
}

// Range is not yet implemented
func (c *Connector) Range(ctx context.Context, sr dosa.SchemaReference, conditions []dosa.Condition, fieldsToRead []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	panic("not implemented")
}

// Search is not yet implemented
func (c *Connector) Search(ctx context.Context, sr dosa.SchemaReference, FieldNameValuePair []string, fieldsToRead []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	panic("not implemented")
}

// Scan is not yet implemented
func (c *Connector) Scan(ctx context.Context, sr dosa.SchemaReference, fieldsToRead []string, token string, limit int) ([]map[string]dosa.FieldValue, error) {
	panic("not implemented")
}

// CheckSchema is one way to register a set of entities. This can be further validated by
// a schema service downstream.
func (c *Connector) CheckSchema(ctx context.Context, ed []*dosa.EntityDefinition) ([]dosa.SchemaReference, error) {
	refs := make([]dosa.SchemaReference, len(ed))
	for idx := range refs {
		refs[idx] = dosa.SchemaReference(uuid.NewV4().String())
	}
	return refs, nil
}

// UpsertSchema always returns nil
func (c *Connector) UpsertSchema(ctx context.Context, ed []*dosa.EntityDefinition) error {
	return nil
}

// CreateScope is not implemented yet
func (c *Connector) CreateScope(ctx context.Context, scope string) error {
	panic("not implemented")
}

// TruncateScope is not implemented yet
func (c *Connector) TruncateScope(ctx context.Context, scope string) error {
	panic("not implemented")
}

// DropScope is not implemented yet
func (c *Connector) DropScope(ctx context.Context, scope string) error {
	panic("not implemented")
}
