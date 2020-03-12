// Copyright (c) 2019 Uber Technologies, Inc.
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
	"sort"
	"strings"
	"time"
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

	// InvalidVersion is version which is less than 1
	InvalidVersion = -1
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
	IndexName  string
	Version    int32
}

// EntityInfo is all the information about an entity, including the schema reference
// as well as the entity definition
type EntityInfo struct {
	Ref *SchemaRef
	Def *EntityDefinition
	TTL *time.Duration
}

func (ei *EntityInfo) String() string {
	return fmt.Sprintf("[scope=%s prefix=%s %s]", ei.Ref.Scope, ei.Ref.NamePrefix, ei.Def.String())
}

// StringSet is a set of strings.
type StringSet map[string]struct{}

// ScopeMetadata is metadata about a scope. (JSON tags to support MD setting CLI tools.) The scope
// may be qualified by a prefix, as in "production.vsoffers".
type ScopeMetadata struct {
	Entity      `dosa:"primaryKey=(Name)" json:"-"`
	Name        string     `json:"name"`
	Owner       string     `json:"owner"` // Owning group name (or the same as Creator)
	Type        int32      `json:"type"`  // Production, Staging, or Development
	Flags       int64      `json:"flags"`
	Version     int32      `json:"version"`
	PrefixStr   string     `json:"prefix_str,omitempty"` // With ":" separators
	Cluster     string     `json:"cluster,omitempty"`    // Host DB cluster
	Creator     string     `json:"creator"`
	CreatedOn   time.Time  `json:"created_on"`
	ExpiresOn   *time.Time `json:"expires_on,omitempty"`
	ExtendCount int32      `json:"extend_count,omitempty"`
	NotifyCount int32      `json:"notify_count,omitempty"`
	ReadMaxRPS  int32      `json:"read_max_rps"`
	WriteMaxRPS int32      `json:"write_max_rps"`
	// This is for convenience only, not stored in the DB:
	Prefixes StringSet `dosa:"-" json:"-"`
}

// MetadataSchemaVersion is the version of the schema of the scope metadata
const MetadataSchemaVersion = 1

// ScopeType is the type of a scope
type ScopeType int32

// Scope types
const (
	// Development scope (also the default, so should be 0)
	Development ScopeType = iota
	// Staging doesn't really exist yet, but may in the future
	Staging
	// Production scope
	Production
)

// ScopeFlagType is a set of scope flags
type ScopeFlagType int64

// Scope flags (remember to update ScopeFlagType.String)
const (
	// AccessFromProd means access is allowed from production (only relevant for Dev scopes)
	AccessFromProd ScopeFlagType = 1 << iota
)

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
// When fields are returned from read/range/scan methods, it's legal for the connector
// to return more fields than originally requested. The caller of the connector should never mutate
// the returned columns either, in case they are from a cache
type Connector interface {
	// DML operations (CRUD + range + scan)
	// CreateIfNotExists creates a row, but only if it does not exist.
	CreateIfNotExists(ctx context.Context, ei *EntityInfo, values map[string]FieldValue) error
	// Read fetches a row by primary key
	// If minimumFields is empty or nil, all non-key fields would be fetched.
	Read(ctx context.Context, ei *EntityInfo, keys map[string]FieldValue, minimumFields []string) (values map[string]FieldValue, err error)
	// MultiRead fetches several rows by primary key
	// If minimumFields is empty or nil, all non-key fields would be fetched.
	MultiRead(ctx context.Context, ei *EntityInfo, keys []map[string]FieldValue, minimumFields []string) (results []*FieldValuesOrError, err error)
	// Upsert updates some columns of a row, or creates a new one if it doesn't exist yet.
	Upsert(ctx context.Context, ei *EntityInfo, values map[string]FieldValue) error
	// MultiUpsert updates some columns of several rows, or creates a new ones if they doesn't exist yet
	MultiUpsert(ctx context.Context, ei *EntityInfo, multiValues []map[string]FieldValue) (result []error, err error)
	// Remove deletes a row
	Remove(ctx context.Context, ei *EntityInfo, keys map[string]FieldValue) error
	// RemoveRange removes all entities in a particular range, only Primary Key queries are allowed.
	RemoveRange(ctx context.Context, ei *EntityInfo, columnConditions map[string][]*Condition) error
	// MultiRemove removes multiple rows
	MultiRemove(ctx context.Context, ei *EntityInfo, multiKeys []map[string]FieldValue) (result []error, err error)
	// Range does a range scan using a set of conditions.
	// If minimumFields is empty or nil, all fields (including key fields) would be fetched.
	Range(ctx context.Context, ei *EntityInfo, columnConditions map[string][]*Condition, minimumFields []string, token string, limit int) ([]map[string]FieldValue, string, error)
	// Scan reads the whole table, for doing a sequential search or dump/load use cases
	// If minimumFields is empty or nil, all fields (including key fields) would be fetched.
	Scan(ctx context.Context, ei *EntityInfo, minimumFields []string, token string, limit int) (multiValues []map[string]FieldValue, nextToken string, err error)

	// DDL (schema) operations
	// CheckSchema makes sure that the schema provided is compatible with the version on the database.
	// It returns the latest schema version for use with later DML operations.
	// This method should have been called something like 'IsSchemaCompatibleWithDB'.
	CheckSchema(ctx context.Context, scope string, namePrefix string, eds []*EntityDefinition) (version int32, err error)
	// CanUpsertSchema checks whether the new schema is backwards-compatible with the schema on the DB.
	// If compatible, the latest schema version is returned, which can be "invalid" (i.e. -1) if no schema
	// has been applied yet.
	CanUpsertSchema(ctx context.Context, scope string, namePrefix string, eds []*EntityDefinition) (version int32, err error)
	// UpsertSchema updates the schema to the one provided.
	UpsertSchema(ctx context.Context, scope string, namePrefix string, ed []*EntityDefinition) (status *SchemaStatus, err error)
	// CheckSchemaStatus checks the status of a schema upsert: whether it is accepted or in progress of application.
	CheckSchemaStatus(ctx context.Context, scope string, namePrefix string, version int32) (*SchemaStatus, error)
	// GetEntitySchema returns the entity info for a given entity in a given scope and prefix.
	GetEntitySchema(ctx context.Context, scope, namePrefix, entityName string, version int32) (*EntityDefinition, error)

	// Datastore management
	// CreateScope creates a scope for storage of data, usually implemented by a keyspace for this data
	// This is usually followed by UpsertSchema
	CreateScope(ctx context.Context, md *ScopeMetadata) error
	// TruncateScope keeps the scope around, but removes all the data
	TruncateScope(ctx context.Context, scope string) error
	// DropScope removes the scope and all of the data
	DropScope(ctx context.Context, scope string) error
	// ScopeExists checks whether a scope exists or not
	ScopeExists(ctx context.Context, scope string) (bool, error)

	// Shutdown finishes the connector to do clean up work
	Shutdown() error
}

func (t ScopeType) String() string {
	switch t {
	case Production:
		return "production"
	case Staging:
		return "staging"
	case Development:
		return "development"
	}
	return fmt.Sprintf("unknown scope type %d", t)
}

func (md *ScopeMetadata) String() string {
	s := fmt.Sprintf("<Scope %s (%s): owner=%s, creator=%s, created=%v", md.Name, ScopeType(md.Type),
		md.Owner, md.Creator, md.CreatedOn)
	if md.ExpiresOn != nil {
		s += fmt.Sprintf(", expires=%v", *md.ExpiresOn)
	}
	if len(md.Prefixes) > 0 {
		s += fmt.Sprintf(", prefixes=%s", md.Prefixes)
	}
	if len(md.PrefixStr) > 0 {
		s += fmt.Sprintf(", prefixes=%v", md.PrefixStr)
	}
	if len(md.Cluster) > 0 {
		s += fmt.Sprintf(", cluster=%v", md.Cluster)
	}
	if md.ExtendCount > 0 {
		s += fmt.Sprintf(", extended=%d", md.ExtendCount)
	}
	if md.NotifyCount > 0 {
		s += fmt.Sprintf(", notified=%d", md.ExtendCount)
	}
	return s + ">"
}

func (s StringSet) String() string {
	ss := make([]string, len(s))
	i := 0
	for k := range s {
		ss[i] = k
		i++
	}
	sort.Strings(ss)
	return "{" + strings.Join(ss, ", ") + "}"
}

func (f ScopeFlagType) String() string {
	fs := make([]string, 1)
	if f&AccessFromProd != 0 {
		fs = append(fs, "AccessFromProd")
	}
	return "{" + strings.Join(fs, ", ") + "}"
}
