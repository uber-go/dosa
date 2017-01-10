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

// RangeOp is used to specify constraints to Range calls
type RangeOp struct{}

// String satisfies the Stringer interface
func (r *RangeOp) String() string {
	/* TODO */
	return ""
}

// Eq is used to express an equality constraint for a range query
func (r *RangeOp) Eq(string, interface{}) *RangeOp {
	/* TODO */
	return r
}

// Gt is used to express an "greater than" constraint for a range query
func (r *RangeOp) Gt(key string, value interface{}) *RangeOp {
	/* TODO */
	return r
}

// GtOrEq is used to express an "greater than or equal" constraint for a
// range query
func (r *RangeOp) GtOrEq(key string, value interface{}) *RangeOp {
	/* TODO */
	return r
}

// Lt is used to express a "less than" constraint for a range query
func (r *RangeOp) Lt(key string, value interface{}) *RangeOp {
	/* TODO */
	return r
}

// LtOrEq is used to express a "less than or equal" constraint for a
// range query
func (r *RangeOp) LtOrEq(key string, value interface{}) *RangeOp {
	/* TODO */
	return r
}

// Fields list the non-key fields users want to fetch. If not set, all fields would be fetched.
// PrimaryKey fields are always fetched.
func (r *RangeOp) Fields([]string) *RangeOp {
	/* TODO */
	return r
}

// Limit sets the number of rows returned per call. If not set, a default
// value would be applied
func (r *RangeOp) Limit(n int) *RangeOp {
	/* TODO */
	return r
}

// Offset sets the pagination token. If not set, an empty token would be used.
func (r *RangeOp) Offset(token string) *RangeOp {
	/* TODO */
	return r
}

// SearchOp represents the search query using a "searchable" field.
type SearchOp struct {
	/* TODO */
}

// NewSearchOp returns a new SearchOp instance
func NewSearchOp(DomainObject) *SearchOp {
	return &SearchOp{}
}

// String satisfies the stringer interface
func (s *SearchOp) String() string {
	/* TODO */
	return ""
}

// By indicates the "searchable" field name and its value.
func (s *SearchOp) By(fieldName string, fieldValue interface{}) *SearchOp {
	/* TODO */
	return s
}

// Limit sets the number of rows returned per call. Default is 128.
func (s *SearchOp) Limit(n int) *SearchOp {
	/* TODO */
	return s
}

// Offset sets the pagination token. If not set, an empty token would be used.
func (s *SearchOp) Offset(token string) *SearchOp {
	/* TODO */
	return s
}

// Fields list the non-key fields users want to fetch. If not set, all normalized fields
// (supplied with “storing” annotation) would be fetched.
// PrimaryKey fields are always fetched.
func (s *SearchOp) Fields([]string) *SearchOp {
	/* TODO */
	return s
}

// ScanOp represents the scan query
type ScanOp struct {
	/* TODO */
}

// Limit sets the number of rows returned per call. Default is 128.
func (s *ScanOp) Limit(n int) *ScanOp {
	/* TODO */
	return s
}

// Offset sets the pagination token. If not set, an empty token would be used.
func (s *ScanOp) Offset(token string) *ScanOp {
	/* TODO */
	return s
}

// Fields list the non-key fields users want to fetch.
// PrimaryKey fields are always fetched.
func (s *ScanOp) Fields([]string) *ScanOp {
	/* TODO */
	return s
}
