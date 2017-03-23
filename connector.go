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

	"github.com/pkg/errors"
)

//go:generate stringer -type=Operator

// Operator defines an operator against some data for range scans
type Operator int

// order of appearance matter here
const (
	// Eq is the equals operator
	Eq Operator = iota + 1

	// Lt is the less than operator
	Lt

	// LtOrEq is the less than or equal operator
	LtOrEq

	// Gt is the greater than operator
	Gt

	// GtOrEq is the greater than or equal operator
	GtOrEq
)

// FieldNameValuePair is a field name and value
type FieldNameValuePair struct {
	Name  string
	Value FieldValue
}

// SchemaRef is a reference to the table and schema version of an object
type SchemaRef struct {
	Scope      string
	NamePrefix string
	EntityName string
	Version    int32
}

// EntityInfo is all the information about an entity, including the schema reference
// as well as the entity definition
type EntityInfo struct {
	Ref *SchemaRef
	Def *EntityDefinition
}

// FieldValue holds a field value. It's just a marker.
type FieldValue interface{}

// FieldValuesOrError either holds a slice of field values for a row, or an error
type FieldValuesOrError struct {
	Values map[string]FieldValue
	Error  error
}

// SchemaStatus saves the version and application status of a schema
type SchemaStatus struct {
	// the version of the schema
	Version int32
	// the application status of the schema
	Status string
}

// Connector is the interface that must be implemented for a backend service
// It can also be implemented using an RPC such as thrift (dosa-idl)
type Connector interface {
	// DML operations (CRUD + search)
	// CreateIfNotExists creates a row, but only if it does not exist.
	CreateIfNotExists(ctx context.Context, ei *EntityInfo, values map[string]FieldValue) error
	// Read fetches a row by primary key
	// If fieldsToRead is empty or nil, all non-key fields would be fetched.
	Read(ctx context.Context, ei *EntityInfo, keys map[string]FieldValue, fieldsToRead []string) (values map[string]FieldValue, err error)
	// MultiRead fetches several rows by primary key
	// If fieldsToRead is empty or nil, all non-key fields would be fetched.
	MultiRead(ctx context.Context, ei *EntityInfo, keys []map[string]FieldValue, fieldsToRead []string) (results []*FieldValuesOrError, err error)
	// Upsert updates some columns of a row, or creates a new one if it doesn't exist yet.
	Upsert(ctx context.Context, ei *EntityInfo, values map[string]FieldValue) error
	// MultiUpsert updates some columns of several rows, or creates a new ones if they doesn't exist yet
	MultiUpsert(ctx context.Context, ei *EntityInfo, multiValues []map[string]FieldValue) (result []error, err error)
	// Remove deletes a row
	Remove(ctx context.Context, ei *EntityInfo, keys map[string]FieldValue) error
	// MultiRemove removes multiple rows
	MultiRemove(ctx context.Context, ei *EntityInfo, multiKeys []map[string]FieldValue) (result []error, err error)
	// Range does a range scan using a set of conditions.
	// If fieldsToRead is empty or nil, all fields (including key fields) would be fetched.
	Range(ctx context.Context, ei *EntityInfo, columnConditions map[string][]*Condition, fieldsToRead []string, token string, limit int) ([]map[string]FieldValue, string, error)
	// Search does a search against a field marked 'searchable'
	// If fieldsToRead is empty or nil, all fields (including key fields) would be fetched.
	Search(ctx context.Context, ei *EntityInfo, fieldPairs FieldNameValuePair, fieldsToRead []string, token string, limit int) (multiValues []map[string]FieldValue, nextToken string, err error)
	// Scan reads the whole table, for doing a sequential search or dump/load use cases
	// If fieldsToRead is empty or nil, all fields (including key fields) would be fetched.
	Scan(ctx context.Context, ei *EntityInfo, fieldsToRead []string, token string, limit int) (multiValues []map[string]FieldValue, nextToken string, err error)

	// DDL operations (schema)
	// CheckSchema validates that the set of entities you have provided is valid and registered already
	// It returns a list of SchemaRef objects for use with later DML operations.
	CheckSchema(ctx context.Context, scope string, namePrefix string, ed []*EntityDefinition) (version int32, err error)
	// UpsertSchema updates the schema to match what you provide as entities, if possible
	UpsertSchema(ctx context.Context, scope string, namePrefix string, ed []*EntityDefinition) (version int32, err error)
	// CheckSchemaStatus checks the status of the schema whether it is accepted or in progress of application.
	CheckSchemaStatus(ctx context.Context, scope string, namePrefix string, version int32) (*SchemaStatus, error)

	// Datastore management
	// CreateScope creates a scope for storage of data, usually implemented by a keyspace for this data
	// This is usually followed by UpsertSchema
	CreateScope(ctx context.Context, scope string) error
	// TruncateScope keeps the scope around, but removes all the data
	TruncateScope(ctx context.Context, scope string) error
	// DropScope removes the scope and all of the data
	DropScope(ctx context.Context, scope string) error
	// ScopeExists checks whether a scope exists or not
	ScopeExists(ctx context.Context, scope string) (bool, error)

	// Shutdown finishes the connector to do clean up work
	Shutdown() error
}

// CreationFuncType is the type of a creation function that creates an instance of a registered connector
type CreationFuncType func(map[string]interface{}) (Connector, error)

var registeredConnectors map[string]CreationFuncType

func init() {
	// Can't seem to do this inline and make lint happy
	registeredConnectors = map[string]CreationFuncType{}
}

// RegisterConnector registers a connector given a name
func RegisterConnector(name string, creationFunc func(map[string]interface{}) (Connector, error)) {
	registeredConnectors[name] = creationFunc
}

// GetConnector gets a connector by name, along with some options
func GetConnector(name string, opts map[string]interface{}) (Connector, error) {
	if creationFunc, ok := registeredConnectors[name]; ok {
		return creationFunc(opts)
	}
	return nil, errors.Errorf("No such connector %q", name)
}
