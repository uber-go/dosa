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

package dosaclient

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/uber-go/dosa"
)

// Default is the default DOSA Client implementation which uses a
// `dosa.Registrar` and `dosa.Connector` to operate on DOSA entities.
type Default struct {
	initErr        error
	initOnce       sync.Once
	schemaRefIndex map[dosa.FQN]dosa.SchemaReference
	entityDefIndex map[dosa.FQN]*dosa.EntityDefinition
	registrar      dosa.Registrar
	connector      dosa.Connector
}

// NewDefault returns a new DOSA client for the registry and connector
// provided. This is currently only a partial implementation to demonstrate
// basic CRUD functionality.
// TODO: implement entire interface
func NewDefault(reg dosa.Registrar, conn dosa.Connector) (*Default, error) {
	return &Default{
		registrar: reg,
		connector: conn,
	}, nil
}

// ensureInit is a helper that all methods should call first to check schema
func (c *Default) ensureInit(ctx context.Context) error {
	// check all entity definitions once before any other RPC
	// note: this assumes that the registry will not be mutated after the
	// client has been initialized, we could potentially do this lazily based
	// on the calling context.
	c.initOnce.Do(func() {
		// for all entities registered, build an index for mapping FQN to
		// entity definition
		c.entityDefIndex = make(map[dosa.FQN]*dosa.EntityDefinition)
		entities := c.registrar.LookupAll()
		entityDefs := make([]*dosa.EntityDefinition, len(entities))
		for i, e := range entities {
			t, fqn, err := c.registrar.LookupByType(e)
			if err != nil {
				c.initErr = err
				return
			}
			// CheckSchema requires a list of entity definitions, so we need
			// to collect them along the way
			ed := &dosa.EntityDefinition{Name: t.Name, Key: t.Key, Columns: t.Columns}
			c.entityDefIndex[fqn] = ed
			entityDefs[i] = ed
		}

		// TODO: replace with CheckSchema when ready
		if err := c.connector.UpsertSchema(ctx, entityDefs); err != nil {
			c.initErr = err
			return
		}
		// this is a little ugly but we need a way to lookup schema refs by FQN
		// using the registry; this is essentially our bridge between the
		// registry and the connector
		// TODO: uncomment below after replacing UpsertSchema w/ CheckSchema
		/*
			c.schemaRefIndex = make(map[dosa.FQN]dosa.SchemaReference)
			for fqn := range c.entityDefIndex {
				for i, ref := range schemaRefs {
					if c.entityDefIndex[fqn] == entityDefs[i] {
						c.schemaRefIndex[fqn] = ref
					}
				}
			}
		*/
	})
	return c.initErr
}

// Initialize must be called before any data operation. An RPC to ensure schema
// compatibility happens on initial call.
func (c *Default) Initialize(ctx context.Context) error {
	// TODO: setup tracing
	return c.ensureInit(ctx)
}

// CreateIfNotExists uses the connector to create a DOSA entity.
func (c *Default) CreateIfNotExists(context.Context, dosa.DomainObject) error {
	panic("not implemented")
}

// Read uses the connector to read one DOSA entity.
func (c *Default) Read(ctx context.Context, fieldsToRead []string, entity dosa.DomainObject) error {
	r := reflect.ValueOf(entity).Elem()

	/* TODO: uncomment when CheckSchema is building schemaRefIndex on init
	if err := c.ensureInit(ctx); err != nil {
		return err
	}
	*/
	// lookup entity definition, use FQN to lookup schema reference
	ed, _, err := c.registrar.LookupByType(entity)
	if err != nil {
		return err
	}

	/* TODO: uncomment when CheckSchema is building schemaRefIndex on init
	sr, ok := c.schemaRefIndex[fqn]
	if !ok {
		return fmt.Errorf("could not find schema reference for fqn %s", fqn)
	}
	*/

	// build map of values to read using entity parser
	fieldValues := make(map[string]dosa.FieldValue)
	for _, pk := range ed.Key.PartitionKeys {
		colName := ed.ColToField[pk]
		value := r.FieldByName(colName)
		if !value.IsValid() {
			// this should never happen
			panic("Field " + colName + " was not found in " + ed.Name)
		}
		fieldValues[pk] = value.Interface()
	}

	// TODO: add clustering keys
	// TODO: replace hard-coded SchemaReference
	results, err := c.connector.Read(ctx, "", fieldValues, fieldsToRead)
	if err != nil {
		return err
	}

	// map values onto entity
	for name, value := range results {
		v := r.FieldByName(name)
		if !v.IsValid() {
			return fmt.Errorf("Invalid value for field %s: %s", name, value)
		}
		newValue := reflect.ValueOf(value)
		v.Set(newValue)
	}

	return nil
}

// BatchRead uses the connector to read multiple DOSA entities.
func (c *Default) BatchRead(context.Context, []string, ...dosa.DomainObject) (dosa.BatchReadResult, error) {
	panic("not implemented")
}

// Upsert uses the connector to create or update an Entity.
func (c *Default) Upsert(context.Context, []string, ...dosa.DomainObject) error {
	// TODO: implement me
	return nil
}

// Delete uses the connector to delete DOSA entities by primary key.
func (c *Default) Delete(context.Context, ...dosa.DomainObject) error {
	panic("not implemented")
}

// Range uses the connector to fetch DOSA entities for a given range.
func (c *Default) Range(context.Context, *dosa.RangeOp) ([]dosa.DomainObject, string, error) {
	panic("not implemented")
}

// Search uses the connector to fetch DOSA entities by fields that have been marked "searchable".
func (c *Default) Search(context.Context, *dosa.SearchOp) ([]dosa.DomainObject, string, error) {
	panic("not implemented")
}

// ScanEverything uses the connector to fetch all DOSA entities of the given type.
func (c *Default) ScanEverything(context.Context, *dosa.ScanOp) ([]dosa.DomainObject, string, error) {
	panic("not implemented")
}
