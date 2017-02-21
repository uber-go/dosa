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
	// This is a relatively expensive operation.
	// Use Upsert whenever possible.
	CreateIfNotExists(context.Context, DomainObject) error

	// Read reads one DOSA entity. The passed-in entity should contain
	// the primary key field values.
	// Other fields are filled in from the datastore.
	// A list of fields can be specified. Use All() or nil for all fields.
	Read(context.Context, []string, DomainObject) error

	// BatchRead reads multiple DOSA entities.
	BatchRead(context.Context, []string, ...DomainObject) (BatchReadResult, error)

	// Upsert creates or update an Entity.
	// A list of fields to update can be specified. Use All() or nil for all fields.
	Upsert(context.Context, []string, ...DomainObject) error

	// Delete removes DOSA entities by primary key. The passed-in entity should contain
	// the primary key field values.
	Delete(context.Context, ...DomainObject) error

	// Range fetches entities within a range
	Range(context.Context, *RangeOp) ([]DomainObject, string, error)

	// Search fetches entities by fields that have been marked "searchable"
	Search(context.Context, *SearchOp) ([]DomainObject, string, error)

	// ScanEverything fetches all entities of a type
	ScanEverything(context.Context, *ScanOp) ([]DomainObject, string, error)
}

// BatchReadResult contains the result for individual entities.
// If the read succeeded for an entity, the entity
// is filled and the error is nil; otherwise, the entity is
// untouched and error is not nil.
type BatchReadResult map[DomainObject]error

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
	versions, err := c.connector.CheckSchema(ctx, c.registrar.Scope(), c.registrar.Prefix(), eds)

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

// CreateIfNotExists uses the connector to create a DOSA entity.
func (c *client) CreateIfNotExists(context.Context, DomainObject) error {
	panic("not implemented")
}

// Read uses the connector to read one DOSA entity.
func (c *client) Read(ctx context.Context, fieldsToRead []string, entity DomainObject) error {
	if !c.initialized {
		return fmt.Errorf("client is not initialized")
	}

	// lookup entity definition, use FQN to lookup schema reference
	reg, err := c.registrar.Find(entity)
	if err != nil {
		return err
	}
	table := reg.Table()
	info := reg.EntityInfo()

	/* TODO: uncomment when CheckSchema is building schemaRefIndex on init
	sr, ok := c.schemaRefIndex[fqn]
	if !ok {
		return fmt.Errorf("could not find schema reference for fqn %s", fqn)
	}
	*/

	// build map of values to read using entity parser
	fieldValues, err := reg.FieldValues(entity)
	if err != nil {
		return err
	}

	// translate fields to read
	fieldsToRead = translateToServerFields(fieldsToRead, table.FieldToCol)
	results, err := c.connector.Read(ctx, info, fieldValues, fieldsToRead)
	if err != nil {
		return err
	}

	// populate entity with results
	if err := reg.Populate(entity, results); err != nil {
		return err
	}

	return nil
}

// BatchRead uses the connector to read multiple DOSA entities.
func (c *client) BatchRead(context.Context, []string, ...DomainObject) (BatchReadResult, error) {
	panic("not implemented")
}

// Upsert uses the connector to create or update an Entity.
func (c *client) Upsert(ctx context.Context, fieldsToInsert []string, objects ...DomainObject) error {
	if !c.initialized {
		return fmt.Errorf("client is not initialized")
	}

	/*
		for _, entity := range objects {
			r := reflect.ValueOf(entity).Elem()
			ed, _, err := c.registrar.Find(entity)
			if err != nil {
				return err
			}
			fieldValues := make(map[string]FieldValue)
			for _, column := range ed.Columns {
				fieldName := ed.ColToField[column.Name]
				value := r.FieldByName(fieldName)
				if !value.IsValid() {
					// this should never happen
					panic("Field " + fieldName + " was not found in " + ed.Name)
				}
				fieldValues[column.Name] = value.Interface()
			}
			fieldsToInsert = translateToServerFields(fieldsToInsert, ed.FieldToCol)

			err = c.connector.Upsert(ctx, "", fieldValues, fieldsToInsert)
			if err != nil {
				return err
			}
		}
	*/
	return nil
}

// Delete uses the connector to delete DOSA entities by primary key.
func (c *client) Delete(context.Context, ...DomainObject) error {
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

func translateToServerFields(fieldsToRead []string, nameMap map[string]string) []string {
	newFields := make([]string, len(fieldsToRead))
	for i, field := range fieldsToRead {
		newFields[i] = nameMap[field]
	}
	return newFields
}
