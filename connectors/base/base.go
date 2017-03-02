package base

import (
	"context"

	"github.com/uber-go/dosa"
)

// Connector always calls next connector in all the functions
type Connector struct {
	Next dosa.Connector
}

// NewConnector creates new base Connector
func NewConnector(next dosa.Connector) dosa.Connector {
	return &Connector{Next: next}
}

// CreateIfNotExists calls next
func (c *Connector) CreateIfNotExists(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	return c.Next.CreateIfNotExists(ctx, ei, values)
}

// Read calls next
func (c *Connector) Read(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue, fieldsToRead []string) (map[string]dosa.FieldValue, error) {
	return c.Next.Read(ctx, ei, values, fieldsToRead)
}

// MultiRead calls next
func (c *Connector) MultiRead(ctx context.Context, ei *dosa.EntityInfo, values []map[string]dosa.FieldValue, fieldsToRead []string) ([]*dosa.FieldValuesOrError, error) {
	return c.Next.MultiRead(ctx, ei	, values, fieldsToRead)
}

// Upsert calls next
func (c *Connector) Upsert(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	return c.Next.Upsert(ctx, ei, values)
}

// MultiUpsert calls next
func (c *Connector) MultiUpsert(ctx context.Context, ei *dosa.EntityInfo, values []map[string]dosa.FieldValue) ([]error, error) {
	return c.Next.MultiUpsert(ctx, ei, values)
}

// Remove calls next
func (c *Connector) Remove(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	return c.Next.Remove(ctx, ei, values)
}

// MultiRemove calls next
func (c *Connector) MultiRemove(ctx context.Context, ei *dosa.EntityInfo, multiValues []map[string]dosa.FieldValue) ([]error, error) {
	return c.Next.MultiRemove(ctx, ei, multiValues)
}

// Range calls next
func (c *Connector) Range(ctx context.Context, ei *dosa.EntityInfo, columnConditions map[string][]dosa.Condition, fieldsToRead []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	return c.Next.Range(ctx, ei, columnConditions, fieldsToRead, token, limit)
}

// Search calls next
func (c *Connector) Search(ctx context.Context, ei *dosa.EntityInfo, fieldPairs dosa.FieldNameValuePair, fieldsToRead []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	return c.Next.Search(ctx, ei, fieldPairs, fieldsToRead, token, limit)
}

// Scan calls next
func (c *Connector) Scan(ctx context.Context, ei *dosa.EntityInfo, fieldsToRead []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	return c.Next.Scan(ctx, ei, fieldsToRead, token, limit)
}

// CheckSchema calls next
func (c *Connector) CheckSchema(ctx context.Context, scope, namePrefix string, ed []*dosa.EntityDefinition) ([]int32, error) {
	return c.Next.CheckSchema(ctx, scope, namePrefix, ed)
}

// UpsertSchema calls next
func (c *Connector) UpsertSchema(ctx context.Context, scope, namePrefix string, ed []*dosa.EntityDefinition) ([]int32, error) {
	return c.Next.UpsertSchema(ctx, scope, namePrefix, ed)
}

// CreateScope calls next
func (c *Connector) CreateScope(ctx context.Context, scope string) error {
	return c.Next.CreateScope(ctx, scope)
}

// TruncateScope calls next
func (c *Connector) TruncateScope(ctx context.Context, scope string) error {
	return c.Next.TruncateScope(ctx, scope)
}

// DropScope calls next
func (c *Connector) DropScope(ctx context.Context, scope string) error {
	return c.Next.DropScope(ctx, scope)
}

// ScopeExists calls next
func (c *Connector) ScopeExists(ctx context.Context, scope string) (bool, error) {
	return c.Next.ScopeExists(ctx, scope)
}

// Shutdown always returns nil
func (c *Connector) Shutdown() error {
	return nil
}

