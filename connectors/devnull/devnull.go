// Copyright (c) 2020 Uber Technologies, Inc.
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

package devnull

import (
	"context"

	"github.com/uber-go/dosa"
)

// Connector is a connector implementation for testing
type Connector struct{}

// CreateIfNotExists always returns nil
func (c *Connector) CreateIfNotExists(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	return nil
}

// Read always returns a not found error
func (c *Connector) Read(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue, minimumFields []string) (map[string]dosa.FieldValue, error) {
	return nil, &dosa.ErrNotFound{}
}

// MultiRead returns a set of not found errors for each key you specify
func (c *Connector) MultiRead(ctx context.Context, ei *dosa.EntityInfo, values []map[string]dosa.FieldValue, minimumFields []string) ([]*dosa.FieldValuesOrError, error) {
	errors := make([]*dosa.FieldValuesOrError, len(values))
	for inx := range values {
		errors[inx] = &dosa.FieldValuesOrError{Error: &dosa.ErrNotFound{}}
	}
	return errors, nil
}

// Upsert throws away the data you upsert
func (c *Connector) Upsert(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	return nil
}

// makeErrorSlice is a handy function to make a slice of errors or nil errors
func makeErrorSlice(len int, e error) []error {
	errors := make([]error, len)
	for inx := 0; inx < len; inx = inx + 1 {
		errors[inx] = e
	}
	return errors
}

// MultiUpsert throws away all the data you upsert, returning a set of no errors
func (c *Connector) MultiUpsert(ctx context.Context, ei *dosa.EntityInfo, values []map[string]dosa.FieldValue) ([]error, error) {
	return makeErrorSlice(len(values), nil), nil
}

// Remove always returns a not found error
func (c *Connector) Remove(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	return nil
}

// RemoveRange removes all entities within the range specified by the columnConditions.
func (c *Connector) RemoveRange(ctx context.Context, ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition) error {
	return nil
}

// MultiRemove returns a not found error for each value
func (c *Connector) MultiRemove(ctx context.Context, ei *dosa.EntityInfo, multiValues []map[string]dosa.FieldValue) ([]error, error) {
	return makeErrorSlice(len(multiValues), &dosa.ErrNotFound{}), nil
}

// Range is not yet implementedS
func (c *Connector) Range(ctx context.Context, ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition, minimumFields []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	return nil, "", &dosa.ErrNotFound{}
}

// Scan is not yet implemented
func (c *Connector) Scan(ctx context.Context, ei *dosa.EntityInfo, minimumFields []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	return nil, "", &dosa.ErrNotFound{}
}

// CheckSchema always returns schema version 1
func (c *Connector) CheckSchema(ctx context.Context, scope, namePrefix string, ed []*dosa.EntityDefinition) (int32, error) {
	return int32(1), nil
}

// CanUpsertSchema always returns schema version 1
func (c *Connector) CanUpsertSchema(ctx context.Context, scope, namePrefix string, ed []*dosa.EntityDefinition) (int32, error) {
	return int32(1), nil
}

// UpsertSchema always returns a SchemaStatus with version 1 and COMPLETED status
func (c *Connector) UpsertSchema(ctx context.Context, scope, namePrefix string, ed []*dosa.EntityDefinition) (*dosa.SchemaStatus, error) {
	return &dosa.SchemaStatus{Version: int32(1), Status: "COMPLETED"}, nil
}

// CheckSchemaStatus always returns a SchemaStatus with version 1 and ACCEPTED status
func (c *Connector) CheckSchemaStatus(ctx context.Context, scope, namePrefix string, version int32) (*dosa.SchemaStatus, error) {
	return &dosa.SchemaStatus{
		Version: int32(1),
		Status:  "ACCEPTED",
	}, nil
}

// GetEntitySchema always returns a blank EntityInfo and no error.
func (c *Connector) GetEntitySchema(ctx context.Context, scope, namePrefix, entityName string, version int32) (*dosa.EntityDefinition, error) {
	return &dosa.EntityDefinition{}, nil
}

// CreateScope returns success
func (c *Connector) CreateScope(ctx context.Context, _ *dosa.ScopeMetadata) error {
	return nil
}

// TruncateScope returns success
func (c *Connector) TruncateScope(ctx context.Context, scope string) error {
	return nil
}

// DropScope returns success
func (c *Connector) DropScope(ctx context.Context, scope string) error {
	return nil
}

// ScopeExists returns true
func (c *Connector) ScopeExists(ctx context.Context, scope string) (bool, error) {
	return true, nil
}

// Shutdown always returns nil
func (c *Connector) Shutdown() error {
	return nil
}

// NewConnector creates a new devnull connector
func NewConnector() *Connector {
	return &Connector{}
}
