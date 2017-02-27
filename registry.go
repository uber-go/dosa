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
	"fmt"
	"reflect"

	"github.com/pkg/errors"
)

// RegisteredEntity is the structure that holds all information necessary for
// performing operations on an entity as well as helper methods for accessing
// type data so that reflection can be minimized.
type RegisteredEntity struct {
	scope  string
	prefix string
	table  *Table
	info   *EntityInfo
	typ    reflect.Type // optimization to avoid doing repetitive reflect.TypeOf
}

// NewRegisteredEntity is a constructor for creating a RegisteredEntity
func NewRegisteredEntity(scope, prefix string, table *Table) *RegisteredEntity {
	// build entity info up-front since version will need to be set soon after
	// the registry is initialized
	info := &EntityInfo{
		Ref: &SchemaRef{
			Scope:      scope,
			NamePrefix: prefix,
			EntityName: table.Name,
		},
		Def: &EntityDefinition{
			Name:    table.Name,
			Key:     table.Key,
			Columns: table.Columns,
		},
	}
	return &RegisteredEntity{
		scope:  scope,
		prefix: prefix,
		table:  table,
		info:   info,
	}
}

// SetVersion sets the current schema version on the registered entity.
func (e *RegisteredEntity) SetVersion(v int32) {
	e.info.Ref.Version = v
}

// EntityInfo is a helper for accessing the registered entity's EntityInfo
// instance which is required by clients to call connector methods.
func (e *RegisteredEntity) EntityInfo() *EntityInfo {
	return e.info
}

// SchemaRef is a helper for accessing the registered entity's
// SchemaRef instance.
func (e *RegisteredEntity) SchemaRef() *SchemaRef {
	return e.info.Ref
}

// EntityDefinition is a helper for accessing the registered entity's
// EntityDefinition instance.
func (e *RegisteredEntity) EntityDefinition() *EntityDefinition {
	return e.info.Def
}

// KeyFieldValues is a helper for generating a map of field values to be used in a query.
func (e *RegisteredEntity) KeyFieldValues(entity DomainObject) map[string]FieldValue {
	v := reflect.ValueOf(entity).Elem()
	fieldValues := make(map[string]FieldValue)

	// populate partition key values
	for _, pk := range e.table.Key.PartitionKeys {
		fieldName := e.table.ColToField[pk]
		value := v.FieldByName(fieldName)
		if !value.IsValid() {
			// this should never happen
			panic("Field " + fieldName + " is not a valid field for " + e.table.StructName)
		}
		fieldValues[pk] = value.Interface()
	}

	// populate clustering key values
	for _, ck := range e.table.Key.ClusteringKeys {
		fieldName := e.table.ColToField[ck.Name]
		value := v.FieldByName(fieldName)
		if !value.IsValid() {
			// this should never happen
			panic("Field " + fieldName + " is not a valid field for " + e.table.StructName)
		}
		fieldValues[ck.Name] = value.Interface()
	}

	return fieldValues
}

// ColumnNames translates field names to column names.
func (e *RegisteredEntity) ColumnNames(fieldNames []string) ([]string, error) {
	// empty should return error
	if len(fieldNames) == 0 {
		return nil, fmt.Errorf("Cannot provide empty list to ColumnNames")
	}

	columnNames := make([]string, len(fieldNames))
	for i, fieldName := range fieldNames {
		columnName, ok := e.table.FieldToCol[fieldName]
		if !ok {
			return nil, fmt.Errorf("%s is not a valid field for %s", fieldName, e.table.StructName)
		}
		columnNames[i] = columnName
	}
	return columnNames, nil
}

// SetFieldValues is a helper for populating a DOSA entity with the given
// fieldName->value map
func (e *RegisteredEntity) SetFieldValues(entity DomainObject, fieldValues map[string]FieldValue) error {
	r := reflect.ValueOf(entity).Elem()
	for columnName, fieldValue := range fieldValues {
		// column name may be different from the entity's field name, so we
		// have to look it up along the way.
		fieldName, ok := e.table.ColToField[columnName]
		if !ok {
			return fmt.Errorf("%s does not map to a valid field name in %s", columnName, e.table.StructName)
		}
		val := r.FieldByName(fieldName)
		if !val.IsValid() {
			panic("Field " + fieldName + " is is not a valid field for " + e.table.StructName)
		}
		val.Set(reflect.ValueOf(fieldValue))
	}
	return nil
}

// Registrar is the interface to register DOSA entities.
type Registrar interface {
	Scope() string
	NamePrefix() string
	Find(DomainObject) (*RegisteredEntity, error)
	FindAll() ([]*RegisteredEntity, error)
}

// prefixedRegistrar puts every entity under a prefix.
// This registrar is not threadsafe. However, Register step is done in
// bootstrap/client-init phase (usually with a single thread),
// and after this phase, multiple goroutines can safely read from this registrar
type prefixedRegistrar struct {
	scope     string
	prefix    string
	fqnIndex  map[FQN]*RegisteredEntity
	typeIndex map[reflect.Type]*RegisteredEntity
}

// NewRegistrar returns a new Registrar for the scope, name prefix and
// entities provided. `dosa.Client` implementations are intended to use scope
// and prefix to uniquely identify where entities should live but the
// registrar itself is only responsible for basic accounting of entities.
func NewRegistrar(scope, prefix string, entities ...DomainObject) (Registrar, error) {
	baseFQN, err := ToFQN(prefix)
	if err != nil {
		return nil, errors.Wrap(err, "failed to construct Registrar")
	}
	fqnIndex := make(map[FQN]*RegisteredEntity)
	typeIndex := make(map[reflect.Type]*RegisteredEntity)

	// index all entities by "FQN" (it's canonical namespace)
	// and by type.
	// TODO: when FQN changes, this will need to be updated
	for _, e := range entities {
		table, err := TableFromInstance(e)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to register entity")
		}

		// use table name (aka FQN) as an internal lookup key
		fqn, err := baseFQN.Child(table.Name)
		if err != nil {
			// shouldn't happen if TableFromInstance behave correctly
			return nil, errors.Wrap(err, "failed to register entity, this is most likely a bug in DOSA")
		}

		// use entity type as internal lookup key
		typ := reflect.TypeOf(e).Elem()

		// create instance and index it
		re := NewRegisteredEntity(scope, prefix, table)
		fqnIndex[fqn] = re
		typeIndex[typ] = re
	}

	return &prefixedRegistrar{
		scope:     scope,
		prefix:    prefix,
		fqnIndex:  fqnIndex,
		typeIndex: typeIndex,
	}, nil
}

// Scope returns the registrar's scope.
func (r *prefixedRegistrar) Scope() string {
	return r.scope
}

// NamePrefix returns the registrar's prefix.
func (r *prefixedRegistrar) NamePrefix() string {
	return r.prefix
}

// Find looks at its internal index to find a registration that matches the
// entity instance provided. Return an error when not found.
func (r *prefixedRegistrar) Find(entity DomainObject) (*RegisteredEntity, error) {
	t := reflect.TypeOf(entity).Elem()
	re, ok := r.typeIndex[t]
	if !ok {
		return nil, errors.Errorf("failed to find registration for given entity")
	}
	return re, nil
}

// FindAll returns all registered entities from its internal index.
func (r *prefixedRegistrar) FindAll() ([]*RegisteredEntity, error) {
	res := []*RegisteredEntity{}
	for _, re := range r.fqnIndex {
		res = append(res, re)
	}
	if len(res) == 0 {
		return nil, fmt.Errorf("registry.FindAll returned empty")
	}
	return res, nil
}
