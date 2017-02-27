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

package dosa

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
)

// DomainObject is a marker interface method for an Entity
type DomainObject interface {
	// dummy marker interface method
	isDomainObject() bool
}

// Entity represents any object that can be persisted by DOSA
type Entity struct{}

// make entity a DomainObject
func (*Entity) isDomainObject() bool {
	return true
}

// Client defines the methods to operate with DOSA entities
type Client interface {
	// Initialize must be called before any data operation
	Initialize(context.Context) error

	// Create creates an entity; it fails if the entity already exists.
	// This is a relatively expensive operation. Use Upsert whenever possible.
	CreateIfNotExists(context.Context, DomainObject) error
	// Read fetches a row by primary key. A list of fields to read can be
	// specified. Use All() or nil for all fields.
	Read(context.Context, []string, DomainObject) error
	// MultiRead fetches several rows by primary key. A list of fields can be
	// specified. Use All() or nil for all fields.
	MultiRead(context.Context, []string, ...DomainObject) (MultiResult, error)
	// Upsert creates or update a row. A list of fields to update can be
	// specified. Use All() or nil for all fields.
	Upsert(context.Context, []string, DomainObject) error
	// MultiUpsert creates or updates multiple rows. A list of fields to
	// update can be specified. Use All() or nil for all fields.
	MultiUpsert(context.Context, []string, ...DomainObject) (MultiResult, error)
	// Delete removes a row by primary key. The passed-in entity should contain
	// the primary key field values.
	Delete(context.Context, DomainObject) error
	// MultiDelete removes multiple rows by primary key. The passed-in entity should
	// contain the primary key field values.
	MultiDelete(context.Context, ...DomainObject) (MultiResult, error)
	// Range fetches entities within a range
	Range(context.Context, *RangeOp) ([]DomainObject, string, error)
	// Search fetches entities by fields that have been marked "searchable"
	Search(context.Context, *SearchOp) ([]DomainObject, string, error)
	// ScanEverything fetches all entities of a type
	ScanEverything(context.Context, *ScanOp) ([]DomainObject, string, error)
}

// MultiResult contains the result for each entity operation in the case of
// MultiRead, MultiUpsert and MultiDelete. If the operation succeeded for
// an entity, the value for in the map will be nil; otherwise, the entity is
// untouched and error is not nil.
type MultiResult map[DomainObject]error

// All is used for "fields []string" to read/update all fields.
// It's a convenience function for code readability.
func All() []string { return nil }

// AdminClient has methods to manage schemas and scopes
type AdminClient interface {
	// CheckSchema checks the compatibility of schemas
	CheckSchema(ctx context.Context, fqns ...FQN) error
	// UpsertSchema upserts the schemas
	UpsertSchema(ctx context.Context, fqns ...FQN) error
	// CreateScope creates a new scope
	CreateScope(s string) error
	// TruncateScope keeps the scope and the schemas, but drops the data associated with the scope
	TruncateScope(s string) error
	// DropScope drops the scope and the data and schemas in the scope
	DropScope(s string) error
}

type client struct {
	initialized bool
	registrar   Registrar
	connector   Connector
}

// NewClient returns a new DOSA client for the registry and connector
// provided. This is currently only a partial implementation to demonstrate
// basic CRUD functionality.
// TODO: implement entire interface
func NewClient(reg Registrar, conn Connector) (Client, error) {
	return &client{
		registrar: reg,
		connector: conn,
	}, nil
}

// Initialize performs initial schema checks against all registered entities.
func (c *client) Initialize(ctx context.Context) error {
	if c.initialized {
		return nil
	}

	// check schema for all registered entities
	registered, err := c.registrar.FindAll()
	if err != nil {
		return err
	}
	eds := []*EntityDefinition{}
	for _, re := range registered {
		eds = append(eds, re.EntityDefinition())
	}

	// fetch latest version for all registered entities, assume order is preserved
	versions, err := c.connector.CheckSchema(ctx, c.registrar.Scope(), c.registrar.NamePrefix(), eds)
	if err != nil {
		return errors.Wrap(err, "CheckSchema failed")
	}

	// set version for all registered entities
	for idx, version := range versions {
		registered[idx].SetVersion(version)
	}
	c.initialized = true
	return nil
}

// CreateIfNotExists creates a row, but only if it does not exist. The entity
// provided must contain values for all components of its primary key for the
// operation to succeed.
func (c *client) CreateIfNotExists(context.Context, DomainObject) error {
	panic("not implemented")
}

// Read fetches an entity by primary key, The entity provided must contain
// values for all components of its primary key for the operation to succeed.
// If `fieldsToRead` is provided, only a subset of fields will be
// marshalled onto the given entity
func (c *client) Read(ctx context.Context, fieldsToRead []string, entity DomainObject) error {
	if !c.initialized {
		return fmt.Errorf("client is not initialized")
	}

	// lookup registered entity, registry will return error if registration
	// is not found
	re, err := c.registrar.Find(entity)
	if err != nil {
		return err
	}

	// translate entity field values to a map of primary key name/values pairs
	// required to perform a read
	fieldValues := re.KeyFieldValues(entity)

	// build a list of column names from a list of entities field names
	columnsToRead, err := re.ColumnNames(fieldsToRead)
	if err != nil {
		return err
	}

	results, err := c.connector.Read(ctx, re.EntityInfo(), fieldValues, columnsToRead)
	if err != nil {
		return err
	}

	// map results to entity fields
	if err := re.SetFieldValues(entity, results); err != nil {
		return err
	}

	return nil
}

// MultiRead fetches several entities by primary key, The entities provided
// must contain values for all components of its primary key for the operation
// to succeed. If `fieldsToRead` is provided, only a subset of fields will be
// marshalled onto the given entities.
func (c *client) MultiRead(context.Context, []string, ...DomainObject) (MultiResult, error) {
	panic("not implemented")
}

// Upsert updates some values of an entity, or creates it if it doesn't exist.
// The entity provided must contain values for all components of its primary
// key for the operation to succeed. If `fieldsToUpdate` is provided, only a
// subset of fields will be updated.
func (c *client) Upsert(ctx context.Context, fieldsToUpdate []string, entity DomainObject) error {
	if !c.initialized {
		return fmt.Errorf("client is not initialized")
	}

	// lookup registered entity, registry will return error if registration
	// is not found
	re, err := c.registrar.Find(entity)
	if err != nil {
		return err
	}

	// translate entity field values to a map of primary key name/values pairs
	// required to perform a read
	fieldValues := re.KeyFieldValues(entity)
	if err != nil {
		return err
	}

	return c.connector.Upsert(ctx, re.EntityInfo(), fieldValues)
}

// MultiUpsert updates several entities by primary key, The entities provided
// must contain values for all components of its primary key for the operation
// to succeed. If `fieldsToUpdate` is provided, only a subset of fields will be
// updated.
func (c *client) MultiUpsert(context.Context, []string, ...DomainObject) (MultiResult, error) {
	panic("not implemented")
}

// Delete deletes an entity by primary key, The entity provided must contain
// values for all components of its primary key for the operation to succeed.
func (c *client) Delete(context.Context, DomainObject) error {
	panic("not implemented")
}

// MultiDelete deletes several entities by primary key, The entities provided
// must contain values for all components of its primary key for the operation
// to succeed.
func (c *client) MultiDelete(context.Context, ...DomainObject) (MultiResult, error) {
	panic("not implemented")
}

// Range uses the connector to fetch DOSA entities for a given range.
func (c *client) Range(context.Context, *RangeOp) ([]DomainObject, string, error) {
	panic("not implemented")
}

// Search uses the connector to fetch DOSA entities by fields that have been marked "searchable".
func (c *client) Search(context.Context, *SearchOp) ([]DomainObject, string, error) {
	panic("not implemented")
}

// ScanEverything uses the connector to fetch all DOSA entities of the given type.
func (c *client) ScanEverything(context.Context, *ScanOp) ([]DomainObject, string, error) {
	panic("not implemented")
}
