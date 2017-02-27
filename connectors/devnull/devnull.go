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

package devnull

import (
	"context"

	"errors"

	"github.com/uber-go/dosa"
)

var errNotFound = errors.New("Not found")

// Connector is a connector implementation for testing
type Connector struct{}

// CreateIfNotExists always returns nil
func (c *Connector) CreateIfNotExists(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	return nil
}

// Read always returns a not found error
func (c *Connector) Read(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue, fieldsToRead []string) (map[string]dosa.FieldValue, error) {
	return nil, errNotFound
}

// MultiRead returns a set of not found errors for each key you specify
func (c *Connector) MultiRead(ctx context.Context, ei *dosa.EntityInfo, values []map[string]dosa.FieldValue, fieldsToRead []string) ([]dosa.FieldValuesOrError, error) {
	errors := make([]dosa.FieldValuesOrError, len(values))
	for inx := range values {
		errors[inx] = dosa.FieldValuesOrError{Error: errNotFound}
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
	return errNotFound
}

// MultiRemove returns a not found error for each value
func (c *Connector) MultiRemove(ctx context.Context, ei *dosa.EntityInfo, multiValues []map[string]dosa.FieldValue) ([]error, error) {
	return makeErrorSlice(len(multiValues), errNotFound), nil
}

// Range is not yet implemented
func (c *Connector) Range(ctx context.Context, ei *dosa.EntityInfo, conditions []dosa.Condition, fieldsToRead []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	return nil, "", errNotFound
}

// Search is not yet implemented
func (c *Connector) Search(ctx context.Context, ei *dosa.EntityInfo, FieldNameValuePair []string, fieldsToRead []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	return nil, "", errNotFound
}

// Scan is not yet implemented
func (c *Connector) Scan(ctx context.Context, ei *dosa.EntityInfo, fieldsToRead []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	return nil, "", errNotFound
}

// CheckSchema always returns a slice of int32 values that match its index
func (c *Connector) CheckSchema(ctx context.Context, scope, namePrefix string, ed []*dosa.EntityDefinition) ([]int32, error) {
	versions := make([]int32, len(ed))
	for idx := range versions {
		versions[idx] = int32(idx)
	}
	return versions, nil
}

// UpsertSchema always returns a slice of int32 values that match its index
func (c *Connector) UpsertSchema(ctx context.Context, scope, namePrefix string, ed []*dosa.EntityDefinition) ([]int32, error) {
	return c.CheckSchema(ctx, scope, namePrefix, ed)
}

// CreateScope returns success
func (c *Connector) CreateScope(ctx context.Context, scope string) error {
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

// ScopeExists checks whether a scope exists or not
func (c *Connector) ScopeExists(ctx context.Context, scope string) (bool, error) {
	panic("not implemented")
}

// Shutdown always returns nil
func (c *Connector) Shutdown() error {
	return nil
}
