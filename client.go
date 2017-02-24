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

import "context"

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
