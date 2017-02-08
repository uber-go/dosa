package dosa

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

import "context"

// Operator defines an operator against some data for range scans
type Operator int

const (
	// Eq is the equals operator
	Eq Operator = iota

	// Lt is the less than operator
	Lt

	// Gt is the greater than operator
	Gt

	// LtOrEq is the less than or equal operator
	LtOrEq

	// GtOrEq is the greater than or equal operator
	GtOrEq
)

// FieldNameValuePair is a field name and value
type FieldNameValuePair struct {
	Name  string
	Value FieldValue
}

// Condition is used to hold a field name, value, and operator
type Condition struct {
	FieldNameValuePair

	// Op defines the operator being used for the condition
	Op Operator
}

// SchemaReference is a reference to the table and schema version of an object
// These strings are returned when the entity is registered at startup
type SchemaReference string

// FieldValue holds a field value. It's just a marker.
type FieldValue interface{}

// Connector is the interface that must be implemented for a backend service
// It can also be implemented using an RPC such as thrift (dosa-idl)
type Connector interface {
	// DML operations (CRUD + search)
	// CreateIfNotExists creates a row, but only if it does not exist
	CreateIfNotExists(ctx context.Context, sr SchemaReference, values map[string]interface{}) error
	// Read fetches a row by primary key
	Read(ctx context.Context, sr SchemaReference, keys map[string]FieldValue, fieldsToRead []string) ([]FieldValue, error)
	// BatchRead fetches several rows by primary key
	BatchRead(ctx context.Context, sr SchemaReference, keys []map[string]FieldValue, fieldsToRead []string) ([][]FieldValue, error)
	// Upsert updates some columns of a row, or creates a new one if it doesn't exist yet
	Upsert(ctx context.Context, sr SchemaReference, keys map[string]FieldValue, fieldsToUpdate []string) error
	// Remove deletes a row
	Remove(ctx context.Context, sr SchemaReference, keys map[string]FieldValue) error
	// Range does a range scan using a set of conditions
	Range(ctx context.Context, sr SchemaReference, conditions []Condition, fieldsToRead []string, token string, limit int) ([]map[string]FieldValue, string, error)
	// Search does a search against a field marked 'searchable'
	Search(ctx context.Context, sr SchemaReference, FieldNameValuePair, fieldsToRead []string, token string, limit int) ([]map[string]FieldValue, string, error)
	// Scan reads the whole table, for doing a sequential search or dump/load use cases
	Scan(ctx context.Context, sr SchemaReference, fieldsToRead []string, token string, limit int) ([]map[string]FieldValue, error)

	// DDL operations (schema)
	// CheckSchema validates that the set of entities you have provided is valid and registered already
	// It returns a list of SchemaReference objects for use with later DML operations.
	CheckSchema(ctx context.Context, ed []EntityDefinition) ([]SchemaReference, error)
	// UpsertSchema says that this set of entity definitions is an updated set, and gets a new set of schema references
	// after the appropriate database changes have been made
	UpsertSchema(ctx context.Context, ed []EntityDefinition) ([]SchemaReference, error)

	// Datastore management
	// CreateScope creates a scope for storage of data, usually implemented by a keyspace for this data
	// This is usually followed by UpsertSchema
	CreateScope(ctx context.Context, scope string) error
	// TruncateScope keeps the scope around, but removes all the data
	TruncateScope(ctx context.Context, scope string) error
	// DropScope removes the scope and all of the data
	DropScope(ctx context.Context, scope string) error
}
