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
	"reflect"

	"github.com/pkg/errors"
)

// RegisteredEntity is the structure that holds all information necessary for
// performing operations on an entity as well as helper methods for accessing
// type data so that reflection can be minimized.
type RegisteredEntity struct {
	table     *Table
	schemaRef *SchemaRef
}

// NewRegisteredEntity is a constructor for creating a RegisteredEntity
func NewRegisteredEntity(scope, namePrefix string, table *Table) *RegisteredEntity {
	return &RegisteredEntity{
		table: table,
		schemaRef: &SchemaRef{
			Scope:      scope,
			NamePrefix: namePrefix,
			EntityName: table.Name,
		},
	}
}

// SetVersion sets the current schema version on the registered entity.
func (e *RegisteredEntity) SetVersion(v int32) {
	e.schemaRef.Version = v
}

// EntityInfo is a helper for accessing the registered entity's EntityInfo
// instance which is required by clients to call connector methods.
func (e *RegisteredEntity) EntityInfo() *EntityInfo {
	return &EntityInfo{
		Ref: e.schemaRef,
		Def: &e.table.EntityDefinition,
		TTL: &e.table.TTL,
	}
}

// Table is a helper for accessing the registered entity's
// Table instance.
func (e *RegisteredEntity) Table() *Table {
	return e.table
}

// SchemaRef is a helper for accessing the registered entity's
// SchemaRef instance.
func (e *RegisteredEntity) SchemaRef() *SchemaRef {
	return e.schemaRef
}

// EntityDefinition is a helper for accessing the registered entity's
// EntityDefinition instance.
func (e *RegisteredEntity) EntityDefinition() *EntityDefinition {
	return &e.table.EntityDefinition
}

// KeyFieldValues is a helper for generating a map of field values to be used in a query.
func (e *RegisteredEntity) KeyFieldValues(entity DomainObject) map[string]FieldValue {
	v := reflect.ValueOf(entity).Elem()
	fieldValues := make(map[string]FieldValue)

	// populate partition key values
	for _, pk := range e.table.Key.PartitionKeys {
		fieldName := e.table.ColToField[pk]
		value := v.FieldByName(fieldName)
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

// OnlyFieldValues is a helper for generating a map of field values for a
// a subset of fields. If a field name provided does not map to an entity
// field, an error will be returned.
func (e *RegisteredEntity) OnlyFieldValues(entity DomainObject, fieldNames []string) (map[string]FieldValue, error) {
	if fieldNames == nil || len(fieldNames) == 0 {
		for _, field := range e.table.ColToField {
			fieldNames = append(fieldNames, field)
		}
	}
	v := reflect.ValueOf(entity).Elem()
	fieldValues := make(map[string]FieldValue)

	for _, fieldName := range fieldNames {
		columnName, ok := e.table.FieldToCol[fieldName]
		if !ok {
			return nil, errors.Errorf("%s is not a valid field for %s", fieldName, e.table.StructName)
		}
		value := v.FieldByName(fieldName)
		if !value.IsValid() {
			// this should never happen
			panic("Field " + fieldName + " is not a valid field for " + e.table.StructName)
		}
		fieldValues[columnName] = value.Interface()
	}
	return fieldValues, nil
}

// ColumnNames translates field names to column names.
func (e *RegisteredEntity) ColumnNames(fieldNames []string) ([]string, error) {
	if fieldNames == nil || len(fieldNames) == 0 {
		for _, field := range e.table.ColToField {
			fieldNames = append(fieldNames, field)
		}
	}
	columnNames := make([]string, len(fieldNames))
	for i, fieldName := range fieldNames {
		columnName, ok := e.table.FieldToCol[fieldName]
		if !ok {
			return nil, errors.Errorf("%s is not a valid field for %s", fieldName, e.table.StructName)
		}
		columnNames[i] = columnName
	}
	return columnNames, nil
}

// SetFieldValues is a helper for populating a DOSA entity with the given
// fieldName->value map
func (e *RegisteredEntity) SetFieldValues(entity DomainObject, fieldValues map[string]FieldValue, fieldsToRead []string) {
	r := reflect.ValueOf(entity).Elem()
	if fieldsToRead == nil {
		for columnName := range fieldValues {
			fieldsToRead = append(fieldsToRead, columnName)
		}
	}
	//for columnName, fieldValue := range fieldValues {
	for _, columnName := range fieldsToRead {
		// column name may be different from the entity's field name, so we
		// have to look it up along the way.
		fieldName, ok := e.table.ColToField[columnName]
		if !ok {
			continue // we ignore fields that we don't know about
		}
		fieldValue := fieldValues[columnName]
		val := r.FieldByName(fieldName)
		if !val.IsValid() {
			panic("Field " + fieldName + " is is not a valid field for " + e.table.StructName)
		}

		var fv reflect.Value
		if fieldValue != nil {
			fv = reflect.ValueOf(fieldValue)
		}
		if !fv.IsValid() || fv.Kind() == reflect.Ptr && fv.IsNil() {
			val.Set(reflect.Zero(val.Type()))
			continue
		}

		switch val.Type() {
		case uuidType, boolType, int64Type, stringType, int32Type, doubleType, timestampType, blobType:
			val.Set(reflect.Indirect(fv))
		case nullUUIDType, nullStringType, nullInt32Type, nullInt64Type, nullDoubleType, nullBoolType, nullTimeType:
			if fv.CanAddr() {
				val.Set(fv.Addr())
			} else {
				val.Set(fv)
			}
		}

	}
}

// Registrar is the interface to register DOSA entities.
type Registrar interface {
	Scope() string
	NamePrefix() string
	Find(DomainObject) (*RegisteredEntity, error)
	FindAll() []*RegisteredEntity
}

// prefixedRegistrar puts every entity under a name prefix.
// This registrar is not threadsafe. However, Register step is done in
// bootstrap/client-init phase (usually with a single thread),
// and after this phase, multiple goroutines can safely read from this registrar
type prefixedRegistrar struct {
	scope      string
	namePrefix string
	typeIndex  map[reflect.Type]*RegisteredEntity
}

// NewRegistrar returns a new Registrar for the scope, name prefix and
// entities provided. `dosa.Client` implementations are intended to use scope
// and prefix to uniquely identify where entities should live but the
// registrar itself is only responsible for basic accounting of entities.
func NewRegistrar(scope, namePrefix string, entities ...DomainObject) (Registrar, error) {
	if err := IsValidNamePrefix(namePrefix); err != nil {
		return nil, errors.Wrap(err, "failed to construct Registrar")
	}
	typeIndex := make(map[reflect.Type]*RegisteredEntity)

	for _, e := range entities {
		table, err := TableFromInstance(e)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to register entity")
		}

		// use entity type as internal lookup key
		typ := reflect.TypeOf(e).Elem()

		// create instance and index it
		re := NewRegisteredEntity(scope, namePrefix, table)
		typeIndex[typ] = re
	}

	return &prefixedRegistrar{
		scope:      scope,
		namePrefix: namePrefix,
		typeIndex:  typeIndex,
	}, nil
}

// Scope returns the registrar's scope.
func (r *prefixedRegistrar) Scope() string {
	return r.scope
}

// NamePrefix returns the registrar's prefix.
func (r *prefixedRegistrar) NamePrefix() string {
	return r.namePrefix
}

// Find looks at its internal index to find a registration that matches the
// entity instance provided. Return an error when not found.
func (r *prefixedRegistrar) Find(entity DomainObject) (*RegisteredEntity, error) {
	t := reflect.TypeOf(entity).Elem()
	re, ok := r.typeIndex[t]
	if !ok {
		return nil, errors.Errorf("failed to find registration for entity %q", t.Name())
	}
	return re, nil
}

// FindAll returns all registered entities from its internal index.
func (r *prefixedRegistrar) FindAll() []*RegisteredEntity {
	res := []*RegisteredEntity{}
	for _, re := range r.typeIndex {
		res = append(res, re)
	}
	return res
}
