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
	"os"
	"reflect"

	"bytes"
	"io"

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

// ErrNotInitialized is returned when a user didn't call Initialize
type ErrNotInitialized struct{}

// Error returns a constant string "client not initialized"
func (*ErrNotInitialized) Error() string {
	return "client not initialized"
}

// ErrorIsNotInitialized checks if the error is a "ErrNotInitialized"
// (possibly wrapped)
func ErrorIsNotInitialized(err error) bool {
	_, ok := errors.Cause(err).(*ErrNotInitialized)
	return ok
}

// ErrNotFound is an error when a row is not found (single or multiple)
type ErrNotFound struct{}

// Error returns a constant string "Not found" for this error
func (*ErrNotFound) Error() string {
	return "not found"
}

// ErrorIsNotFound checks if the error is a "ErrNotFound"
// (possibly wrapped)
func ErrorIsNotFound(err error) bool {
	_, ok := errors.Cause(err).(*ErrNotFound)
	return ok
}

// ErrAlreadyExists is an error returned when CreateIfNotExists but a row already exists
type ErrAlreadyExists struct{}

func (*ErrAlreadyExists) Error() string {
	return "already exists"
}

// ErrorIsAlreadyExists checks if the error is caused by "ErrAlreadyExists"
func ErrorIsAlreadyExists(err error) bool {
	_, ok := errors.Cause(err).(*ErrAlreadyExists)
	return ok
}

// Client defines the methods to operate with DOSA entities
type Client interface {
	// Initialize must be called before any data operation
	Initialize(ctx context.Context) error

	// Create creates an entity; it fails if the entity already exists.
	// You must fill in all of the fields of the DomainObject before
	// calling this method, or they will be inserted with the zero value
	// This is a relatively expensive operation. Use Upsert whenever possible.
	CreateIfNotExists(ctx context.Context, objectToCreate DomainObject) error

	// Read fetches a row by primary key. A list of fields to read can be
	// specified. Use All() or nil for all fields.
	// Before calling this method, fill in the DomainObject with ALL
	// of the primary key fields; the other field values will be populated
	// as a result of the read
	Read(ctx context.Context, fieldsToRead []string, objectToRead DomainObject) error

	// TODO: Coming in v2.1
	// MultiRead fetches several rows by primary key. A list of fields can be
	// specified. Use All() or nil for all fields.
	// MultiRead(context.Context, []string, ...DomainObject) (MultiResult, error)

	// Upsert creates or update a row. A list of fields to update can be
	// specified. Use All() or nil for all fields.
	// Before calling this method, fill in the DomainObject with ALL
	// of the primary key fields, along with whatever fields you specify
	// to update in fieldsToUpdate (or all the fields if you use dosa.All())
	Upsert(ctx context.Context, fieldsToUpdate []string, objectToUpdate DomainObject) error

	// TODO: Coming in v2.1
	// MultiUpsert creates or updates multiple rows. A list of fields to
	// update can be specified. Use All() or nil for all fields.
	// MultiUpsert(context.Context, []string, ...DomainObject) (MultiResult, error)

	// Remove removes a row by primary key. The passed-in entity should contain
	// the primary key field values, all other fields are ignored.
	Remove(ctx context.Context, objectToRemove DomainObject) error

	// TODO: Coming in v2.1
	// MultiRemove removes multiple rows by primary key. The passed-in entity should
	// contain the primary key field values.
	// MultiRemove(context.Context, ...DomainObject) (MultiResult, error)

	// Range fetches entities within a range
	// Before calling range, create a RangeOp and fill in the table
	// along with the partition key information. You will get back
	// an array of DomainObjects, which will be of the type you requested
	// in the rangeOp
	Range(ctx context.Context, rangeOp *RangeOp) ([]DomainObject, string, error)

	// Search fetches entities by fields that have been marked "searchable"
	// TODO: Coming in v2.1
	// Search(ctx context.Context, searchOp *SearchOp) ([]DomainObject, string, error)

	// ScanEverything fetches all entities of a type
	// Before calling ScanEverything, create a scanOp to specify the
	// table to scan. The return values are an array of objects, that
	// you can type-assert to the appropriate dosa.Entity, a string
	// that contains the continuation token, and any error.
	// To scan the next set of rows, modify the scanOp to provide
	// the string returned as an Offset()
	ScanEverything(ctx context.Context, scanOp *ScanOp) ([]DomainObject, string, error)
}

// MultiResult contains the result for each entity operation in the case of
// MultiRead, MultiUpsert and MultiRemove. If the operation succeeded for
// an entity, the value for in the map will be nil; otherwise, the entity is
// untouched and error is not nil.
type MultiResult map[DomainObject]error

// All is used for "fields []string" to read/update all fields.
// It's a convenience function for code readability.
func All() []string { return nil }

// AdminClient has methods to manage schemas and scopes
type AdminClient interface {
	// Directories sets admin client search path
	Directories(dirs []string) AdminClient
	// Excludes sets patters to exclude when searching for entities
	Excludes(excludes []string) AdminClient
	// Scope sets the admin client scope
	Scope(scope string) AdminClient
	// CheckSchema checks the compatibility of schemas
	CheckSchema(ctx context.Context, namePrefix string) (*SchemaStatus, error)
	// CheckSchemaStatus checks the status of schema application
	CheckSchemaStatus(ctx context.Context, namePrefix string, version int32) (*SchemaStatus, error)
	// UpsertSchema upserts the schemas
	UpsertSchema(ctx context.Context, namePrefix string) (*SchemaStatus, error)
	// GetSchema finds entity definitions
	GetSchema() ([]*EntityDefinition, error)
	// CreateScope creates a new scope
	CreateScope(ctx context.Context, s string) error
	// TruncateScope keeps the scope and the schemas, but drops the data associated with the scope
	TruncateScope(ctx context.Context, s string) error
	// DropScope drops the scope and the data and schemas in the scope
	DropScope(ctx context.Context, s string) error
}

type client struct {
	initialized bool
	registrar   Registrar
	connector   Connector
}

// NewClient returns a new DOSA client for the registry and connector
// provided. This is currently only a partial implementation to demonstrate
// basic CRUD functionality.
func NewClient(reg Registrar, conn Connector) Client {
	return &client{
		registrar: reg,
		connector: conn,
	}
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
	version, err := c.connector.CheckSchema(ctx, c.registrar.Scope(), c.registrar.NamePrefix(), eds)
	if err != nil {
		return errors.Wrap(err, "CheckSchema failed")
	}

	// set version for all registered entities
	for _, reg := range registered {
		reg.SetVersion(version)
	}
	c.initialized = true
	return nil
}

// CreateIfNotExists creates a row, but only if it does not exist. The entity
// provided must contain values for all components of its primary key for the
// operation to succeed.
func (c *client) CreateIfNotExists(ctx context.Context, entity DomainObject) error {
	return c.createOrUpsert(ctx, nil, entity, c.connector.CreateIfNotExists)
}

// Read fetches an entity by primary key, The entity provided must contain
// values for all components of its primary key for the operation to succeed.
// If `fieldsToRead` is provided, only a subset of fields will be
// marshalled onto the given entity
func (c *client) Read(ctx context.Context, fieldsToRead []string, entity DomainObject) error {
	if !c.initialized {
		return &ErrNotInitialized{}
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
	re.SetFieldValues(entity, results, columnsToRead)

	return nil
}

// MultiRead fetches several entities by primary key, The entities provided
// must contain values for all components of its primary key for the operation
// to succeed. If `fieldsToRead` is provided, only a subset of fields will be
// marshalled onto the given entities.
func (c *client) MultiRead(context.Context, []string, ...DomainObject) (MultiResult, error) {
	panic("not implemented")
}

type createOrUpsertType func(context.Context, *EntityInfo, map[string]FieldValue) error

// Upsert updates some values of an entity, or creates it if it doesn't exist.
// The entity provided must contain values for all components of its primary
// key for the operation to succeed. If `fieldsToUpdate` is provided, only a
// subset of fields will be updated.
func (c *client) Upsert(ctx context.Context, fieldsToUpdate []string, entity DomainObject) error {
	return c.createOrUpsert(ctx, fieldsToUpdate, entity, c.connector.Upsert)
}

func (c *client) createOrUpsert(ctx context.Context, fieldsToUpdate []string, entity DomainObject, fn createOrUpsertType) error {
	if !c.initialized {
		return &ErrNotInitialized{}
	}

	// lookup registered entity, registry will return error if registration
	// is not found
	re, err := c.registrar.Find(entity)
	if err != nil {
		return err
	}

	// translate entity field values to a map of primary key name/values pairs
	keyFieldValues := re.KeyFieldValues(entity)

	// translate remaining entity fields values to map of column name/value pairs
	fieldValues, err := re.OnlyFieldValues(entity, fieldsToUpdate)
	if err != nil {
		return err
	}

	// merge key and remaining values
	for k, v := range keyFieldValues {
		fieldValues[k] = v
	}

	return fn(ctx, re.EntityInfo(), fieldValues)
}

// MultiUpsert updates several entities by primary key, The entities provided
// must contain values for all components of its primary key for the operation
// to succeed. If `fieldsToUpdate` is provided, only a subset of fields will be
// updated.
func (c *client) MultiUpsert(context.Context, []string, ...DomainObject) (MultiResult, error) {
	panic("not implemented")
}

// Remove deletes an entity by primary key, The entity provided must contain
// values for all components of its primary key for the operation to succeed.
func (c *client) Remove(ctx context.Context, entity DomainObject) error {
	if !c.initialized {
		return &ErrNotInitialized{}
	}

	// lookup registered entity, registry will return error if registration
	// is not found
	re, err := c.registrar.Find(entity)
	if err != nil {
		return err
	}

	// translate entity field values to a map of primary key name/values pairs
	keyFieldValues := re.KeyFieldValues(entity)

	err = c.connector.Remove(ctx, re.EntityInfo(), keyFieldValues)
	return err
}

// MultiRemove deletes several entities by primary key, The entities provided
// must contain values for all components of its primary key for the operation
// to succeed.
func (c *client) MultiRemove(context.Context, ...DomainObject) (MultiResult, error) {
	panic("not implemented")
}

// Range uses the connector to fetch DOSA entities for a given range.
func (c *client) Range(ctx context.Context, r *RangeOp) ([]DomainObject, string, error) {
	if !c.initialized {
		return nil, "", &ErrNotInitialized{}
	}
	// look up the entity in the registry
	re, err := c.registrar.Find(r.sop.object)
	if err != nil {
		return nil, "", errors.Wrap(err, "Range")
	}

	// now convert the client range columns to server side column conditions structure
	columnConditions, err := convertRangeOpConditions(r, re.table)
	if err != nil {
		return nil, "", errors.Wrap(err, "Range")
	}

	// convert the fieldsToRead to the server side equivalent
	fieldsToRead, err := re.ColumnNames(r.sop.fieldsToRead)
	if err != nil {
		return nil, "", errors.Wrap(err, "Range")
	}

	// call the server side method
	values, token, err := c.connector.Range(ctx, re.info, columnConditions, fieldsToRead, r.sop.token, r.sop.limit)
	if err != nil {
		return nil, "", errors.Wrap(err, "Range")
	}

	objectArray := objectsFromValueArray(r.sop.object, values, re, nil)
	return objectArray, token, nil
}

func objectsFromValueArray(object DomainObject, values []map[string]FieldValue, re *RegisteredEntity, columnsToRead []string) []DomainObject {
	goType := reflect.TypeOf(object).Elem() // get the reflect.Type of the client entity
	doType := reflect.TypeOf((*DomainObject)(nil)).Elem()
	slice := reflect.MakeSlice(reflect.SliceOf(doType), 0, len(values)) // make a slice of these
	elements := reflect.New(slice.Type())
	elements.Elem().Set(slice)
	for _, flist := range values { // for each row returned
		newObject := reflect.New(goType).Interface()                             // make a new entity
		re.SetFieldValues(newObject.(DomainObject), flist, columnsToRead)        // fill it in from server values
		slice = reflect.Append(slice, reflect.ValueOf(newObject.(DomainObject))) // append to slice
	}
	return slice.Interface().([]DomainObject)
}

// Search uses the connector to fetch DOSA entities by fields that have been marked "searchable".
func (c *client) Search(context.Context, *SearchOp) ([]DomainObject, string, error) {
	panic("not implemented")
}

// ScanEverything uses the connector to fetch all DOSA entities of the given type.
func (c *client) ScanEverything(ctx context.Context, sop *ScanOp) ([]DomainObject, string, error) {
	if !c.initialized {
		return nil, "", &ErrNotInitialized{}
	}
	// look up the entity in the registry
	re, err := c.registrar.Find(sop.object)
	if err != nil {
		return nil, "", errors.Wrap(err, "Range")
	}
	// convert the fieldsToRead to the server side equivalent
	fieldsToRead, err := re.ColumnNames(sop.fieldsToRead)
	if err != nil {
		return nil, "", errors.Wrap(err, "Range")
	}

	// call the server side method
	values, token, err := c.connector.Scan(ctx, re.info, fieldsToRead, sop.token, sop.limit)
	if err != nil {
		return nil, "", err
	}
	objectArray := objectsFromValueArray(sop.object, values, re, nil)
	return objectArray, token, nil

}

type adminClient struct {
	scope     string
	dirs      []string
	excludes  []string
	connector Connector
}

// NewAdminClient returns a new DOSA admin client for the connector provided.
func NewAdminClient(conn Connector) AdminClient {
	return &adminClient{
		scope:     os.Getenv("USER"),
		dirs:      []string{"."},
		excludes:  []string{"_test.go"},
		connector: conn,
	}
}

// Directories sets the given paths to the client's list of file paths to scan
// during schema operations. Defaults to ["."].
func (c *adminClient) Directories(dirs []string) AdminClient {
	c.dirs = dirs
	return c
}

// Excludes sets the substrings used when considering filenames for inclusion
// when searching for DOSA entities. Defaults to ["_test.go"]
func (c *adminClient) Excludes(excludes []string) AdminClient {
	c.excludes = excludes
	return c
}

// Scope sets the scope used for schema operations. Defaults to $USER
func (c *adminClient) Scope(scope string) AdminClient {
	c.scope = scope
	return c
}

// CheckSchema first searches for entity definitions within configured
// directories before checking the compatibility of each entity for the givena
// the namePrefix. The client's scope and search directories should be
// configured on initialization and be non-empty when CheckSchema is called.
// An error is returned if client is misconfigured (eg. invalid scope) or if
// any of the entities found are incompatible, not found or not uniquely named.
// The definition of "incompatible" and "not found" may vary but is ultimately
// defined by the client connector implementation.
func (c *adminClient) CheckSchema(ctx context.Context, namePrefix string) (*SchemaStatus, error) {
	defs, err := c.GetSchema()
	if err != nil {
		return nil, errors.Wrapf(err, "GetSchema failed")
	}
	version, err := c.connector.CheckSchema(ctx, c.scope, namePrefix, defs)
	if err != nil {
		return nil, errors.Wrapf(err, "CheckSchema failed, directories: %s, excludes: %s, scope: %s", c.dirs, c.excludes, c.scope)
	}
	return &SchemaStatus{
		Version: version,
		Status:  "OK",
	}, nil
}

func (c *adminClient) CheckSchemaStatus(ctx context.Context, namePrefix string, version int32) (*SchemaStatus, error) {
	status, err := c.connector.CheckSchemaStatus(ctx, c.scope, namePrefix, version)
	if err != nil {
		return nil, errors.Wrapf(err, "CheckSchemaStatus status failed")
	}
	return status, nil
}

// UpsertSchema creates or updates the schema for entities in the given
// namespace. See CheckSchema for more detail about scope and namePrefix.
func (c *adminClient) UpsertSchema(ctx context.Context, namePrefix string) (*SchemaStatus, error) {
	defs, err := c.GetSchema()
	if err != nil {
		return nil, errors.Wrapf(err, "GetSchema failed")
	}
	status, err := c.connector.UpsertSchema(ctx, c.scope, namePrefix, defs)
	if err != nil {
		return nil, errors.Wrapf(err, "UpsertSchema failed, directories: %s, excludes: %s, scope: %s", c.dirs, c.excludes, c.scope)
	}
	return status, nil
}

// GetSchema returns the derived entity definitions that are found within the
// current search path of the client. GetSchema can be used to introspect the
// state of schema before further operations are performed. For example,
// GetSchema is called by both CheckSchema and UpsertSchema before their
// respective operations are performed. An error is returned when:
//   - invalid scope name (eg. length, invalid characters, see names.go)
//   - invalid directory (eg. path does not exist, is not a directory)
//   - unparseable entity (eg. invalid primary key)
//   - no entities were found
func (c *adminClient) GetSchema() ([]*EntityDefinition, error) {
	// prevent bogus scope names from reaching connectors
	if err := IsValidName(c.scope); err != nil {
		return nil, errors.Wrapf(err, "invalid scope name %q", c.scope)
	}
	// "warnings" mean entity was found but contained invalid annotations
	entities, warns, err := FindEntities(c.dirs, c.excludes)
	if len(warns) > 0 {
		return nil, NewEntityErrors(warns)
	}
	// I/O and AST parsing errors
	if err != nil {
		return nil, err
	}
	// prevent unnecessary connector calls when nothing was found
	if len(entities) == 0 {
		return nil, fmt.Errorf("no entities found; did you specify the right directories for your source?")
	}

	defs := make([]*EntityDefinition, len(entities))
	for idx, e := range entities {
		defs[idx] = &e.EntityDefinition
	}
	return defs, nil
}

// EntityErrors is a container for parse errors/warning.
type EntityErrors struct {
	warns []error
}

// NewEntityErrors returns a wrapper for errors encountered while parsing
// entity struct tags.
func NewEntityErrors(warns []error) *EntityErrors {
	return &EntityErrors{warns: warns}
}

// Error makes parse errors discernable to end-user.
func (ee *EntityErrors) Error() string {
	var str bytes.Buffer
	if _, err := io.WriteString(&str, "The following entities had warnings/errors:"); err != nil {
		// for linting, WriteString will never return error
		return "could not write errors to output buffer"
	}
	for _, err := range ee.warns {
		str.WriteByte('\n')
		if _, err := io.WriteString(&str, err.Error()); err != nil {
			// for linting, WriteString will never return error
			return "could not write errors to output buffer"
		}
	}
	return str.String()
}

// CreateScope creates a new scope
func (c *adminClient) CreateScope(ctx context.Context, s string) error {
	return c.connector.CreateScope(ctx, s)
}

// TruncateScope keeps the scope and the schemas, but drops the data associated with the scope
func (c *adminClient) TruncateScope(ctx context.Context, s string) error {
	return c.connector.TruncateScope(ctx, s)
}

// DropScope drops the scope and the data and schemas in the scope
func (c *adminClient) DropScope(ctx context.Context, s string) error {
	return c.connector.DropScope(ctx, s)
}
