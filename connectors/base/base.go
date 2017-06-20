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

const name = "base"

// ErrNoMoreConnector is used when there is no more next connector
type ErrNoMoreConnector struct {
}

// Error satisfies the error interface.
func (e ErrNoMoreConnector) Error() string {
	return "no more connectors"
}

// Connector always calls Next Connector in all the functions
type Connector struct {
	Next dosa.Connector
}

// NewConnector creates new base Connector
func NewConnector(next dosa.Connector) dosa.Connector {
	return &Connector{Next: next}
}

// CreateIfNotExists calls Next
func (c *Connector) CreateIfNotExists(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	if c.Next == nil {
		return ErrNoMoreConnector{}
	}
	return c.Next.CreateIfNotExists(ctx, ei, values)
}

// Read calls Next
func (c *Connector) Read(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue, minimumFields []string) (map[string]dosa.FieldValue, error) {
	if c.Next == nil {
		return nil, ErrNoMoreConnector{}
	}
	return c.Next.Read(ctx, ei, values, minimumFields)
}

// MultiRead calls Next
func (c *Connector) MultiRead(ctx context.Context, ei *dosa.EntityInfo, values []map[string]dosa.FieldValue, minimumFields []string) ([]*dosa.FieldValuesOrError, error) {
	if c.Next == nil {
		return nil, ErrNoMoreConnector{}
	}
	return c.Next.MultiRead(ctx, ei, values, minimumFields)
}

// Upsert calls Next
func (c *Connector) Upsert(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	if c.Next == nil {
		return ErrNoMoreConnector{}
	}
	return c.Next.Upsert(ctx, ei, values)
}

// MultiUpsert calls Next
func (c *Connector) MultiUpsert(ctx context.Context, ei *dosa.EntityInfo, values []map[string]dosa.FieldValue) ([]error, error) {
	if c.Next == nil {
		return nil, ErrNoMoreConnector{}
	}
	return c.Next.MultiUpsert(ctx, ei, values)
}

// Remove calls Next
func (c *Connector) Remove(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	if c.Next == nil {
		return ErrNoMoreConnector{}
	}
	return c.Next.Remove(ctx, ei, values)
}

// RemoveRange calls Next.
func (c *Connector) RemoveRange(ctx context.Context, ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition) error {
	if c.Next == nil {
		return ErrNoMoreConnector{}
	}
	return c.Next.RemoveRange(ctx, ei, columnConditions)
}

// MultiRemove calls Next
func (c *Connector) MultiRemove(ctx context.Context, ei *dosa.EntityInfo, multiValues []map[string]dosa.FieldValue) ([]error, error) {
	if c.Next == nil {
		return nil, ErrNoMoreConnector{}
	}
	return c.Next.MultiRemove(ctx, ei, multiValues)
}

// Range calls Next
func (c *Connector) Range(ctx context.Context, ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition, minimumFields []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	if c.Next == nil {
		return nil, "", ErrNoMoreConnector{}
	}
	return c.Next.Range(ctx, ei, columnConditions, minimumFields, token, limit)
}

// Search calls Next
func (c *Connector) Search(ctx context.Context, ei *dosa.EntityInfo, fieldPairs dosa.FieldNameValuePair, minimumFields []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	if c.Next == nil {
		return nil, "", ErrNoMoreConnector{}
	}
	return c.Next.Search(ctx, ei, fieldPairs, minimumFields, token, limit)
}

// Scan calls Next
func (c *Connector) Scan(ctx context.Context, ei *dosa.EntityInfo, minimumFields []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	if c.Next == nil {
		return nil, "", ErrNoMoreConnector{}
	}
	return c.Next.Scan(ctx, ei, minimumFields, token, limit)
}

// CheckSchema calls Next
func (c *Connector) CheckSchema(ctx context.Context, scope, namePrefix string, ed []*dosa.EntityDefinition) (int32, error) {
	if c.Next == nil {
		return dosa.InvalidVersion, ErrNoMoreConnector{}
	}
	return c.Next.CheckSchema(ctx, scope, namePrefix, ed)
}

// UpsertSchema calls Next
func (c *Connector) UpsertSchema(ctx context.Context, scope, namePrefix string, ed []*dosa.EntityDefinition) (*dosa.SchemaStatus, error) {
	if c.Next == nil {
		return nil, ErrNoMoreConnector{}
	}
	return c.Next.UpsertSchema(ctx, scope, namePrefix, ed)
}

// CheckSchemaStatus calls Next
func (c *Connector) CheckSchemaStatus(ctx context.Context, scope string, namePrefix string, version int32) (*dosa.SchemaStatus, error) {
	if c.Next == nil {
		return nil, ErrNoMoreConnector{}
	}
	return c.Next.CheckSchemaStatus(ctx, scope, namePrefix, version)
}

// CreateScope calls Next
func (c *Connector) CreateScope(ctx context.Context, scope string) error {
	if c.Next == nil {
		return ErrNoMoreConnector{}
	}
	return c.Next.CreateScope(ctx, scope)
}

// TruncateScope calls Next
func (c *Connector) TruncateScope(ctx context.Context, scope string) error {
	if c.Next == nil {
		return ErrNoMoreConnector{}
	}
	return c.Next.TruncateScope(ctx, scope)
}

// DropScope calls Next
func (c *Connector) DropScope(ctx context.Context, scope string) error {
	if c.Next == nil {
		return ErrNoMoreConnector{}
	}
	return c.Next.DropScope(ctx, scope)
}

// ScopeExists calls Next
func (c *Connector) ScopeExists(ctx context.Context, scope string) (bool, error) {
	if c.Next == nil {
		return false, ErrNoMoreConnector{}
	}
	return c.Next.ScopeExists(ctx, scope)
}

// Shutdown always returns nil
func (c *Connector) Shutdown() error {
	if c.Next == nil {
		return ErrNoMoreConnector{}
	}
	return c.Next.Shutdown()
}

// Name returns the name of the connector
func Name() string {
	return name
}
