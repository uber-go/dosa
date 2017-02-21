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

package yarpc

import (
	"context"

	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
	dosarpc "github.com/uber/dosa-idl/.gen/dosa"
	"github.com/uber/dosa-idl/.gen/dosa/dosaclient"
)

// Client holds the client-side RPC interface and some schema information
type Client struct {
	Client dosaclient.Interface
}

func entityInfoToSchemaRef(ei dosa.EntityInfo) *dosarpc.SchemaRef {
	scope := ei.Ref.Scope
	namePrefix := ei.Ref.NamePrefix
	entityName := ei.Ref.EntityName
	version := ei.Ref.Version
	sr := dosarpc.SchemaRef{
		Scope:      &scope,
		NamePrefix: &namePrefix,
		EntityName: &entityName,
		Version:    &version,
	}
	return &sr
}

func fieldValueMapFromClientMap(values map[string]dosa.FieldValue) dosarpc.FieldValueMap {
	fields := dosarpc.FieldValueMap{}
	for name, value := range values {
		rpcValue := &dosarpc.Value{ElemValue: RawValueFromInterface(value)}
		fields[name] = rpcValue
	}
	return fields
}

// CreateIfNotExists ...
func (y *Client) CreateIfNotExists(ctx context.Context, ei dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	createRequest := dosarpc.CreateRequest{
		Ref:          entityInfoToSchemaRef(ei),
		EntityValues: fieldValueMapFromClientMap(values),
	}
	return y.Client.CreateIfNotExists(ctx, &createRequest)
}

// Upsert inserts or updates your data
func (y *Client) Upsert(ctx context.Context, ei dosa.EntityInfo, values map[string]dosa.FieldValue, fieldsToUpdate []string) error {
	upsertRequest := dosarpc.UpsertRequest{
		Ref:          entityInfoToSchemaRef(ei),
		EntityValues: fieldValueMapFromClientMap(values),
	}
	return y.Client.Upsert(ctx, &upsertRequest)
}

// Read reads a single entity
func (y *Client) Read(ctx context.Context, ei dosa.EntityInfo, keys map[string]dosa.FieldValue, fieldsToRead []string) (map[string]dosa.FieldValue, error) {
	// Convert the fields from the client's map to a set of fields to read
	var rpcFieldsToRead map[string]struct{}
	if fieldsToRead != nil {
		rpcFieldsToRead = map[string]struct{}{}
		for _, field := range fieldsToRead {
			rpcFieldsToRead[field] = struct{}{}
		}
	}

	// convert the key values from interface{} to RPC's Value
	rpcFields := map[string]*dosarpc.Value{}

	for key, value := range keys {
		rpcValue := &dosarpc.Value{ElemValue: RawValueFromInterface(value)}
		rpcFields[key] = rpcValue
	}

	// perform the read request
	readRequest := dosarpc.ReadRequest{
		Ref:          entityInfoToSchemaRef(ei),
		KeyValues:    rpcFields,
		FieldsToRead: rpcFieldsToRead,
	}
	response, err := y.Client.Read(ctx, &readRequest)
	if err != nil {
		return nil, err
	}

	// no error, so for each column, transform it into the map of (col->value) items
	result := map[string]dosa.FieldValue{}
	// TODO: create a typemap to make this faster
	for name, value := range response.EntityValues {
		for _, col := range ei.Def.Columns {
			if col.Name == name {
				result[name] = RawValueAsInterface(*value.ElemValue, col.Type)
				break
			}
		}
	}
	return result, nil
}

// BatchRead is not yet implemented
func (y *Client) BatchRead(ctx context.Context, ei dosa.EntityInfo, keys []map[string]dosa.FieldValue, fieldsToRead []string) ([]dosa.FieldValuesOrError, error) {
	panic("not implemented")
}

// BatchUpsert is not yet implemented
func (y *Client) BatchUpsert(ctx context.Context, ei dosa.EntityInfo, values []map[string]dosa.FieldValue, fieldsToUpdate []string) ([]error, error) {
	panic("not implemented")
}

// Remove is not yet implemented
func (y *Client) Remove(ctx context.Context, ei dosa.EntityInfo, keys map[string]dosa.FieldValue) error {
	panic("not implemented")
}

// Range is not yet implemented
func (y *Client) Range(ctx context.Context, ei dosa.EntityInfo, conditions []dosa.Condition, fieldsToRead []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	panic("not implemented")
}

// Search is not yet implemented
func (y *Client) Search(ctx context.Context, ei dosa.EntityInfo, FieldNameValuePair []string, fieldsToRead []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	panic("not implemented")
}

// Scan is not yet implemented
func (y *Client) Scan(ctx context.Context, ei dosa.EntityInfo, fieldsToRead []string, token string, limit int) ([]map[string]dosa.FieldValue, error) {
	panic("not implemented")
}

// CheckSchema is one way to register a set of entities. This can be further validated by
// a schema service downstream.
func (y *Client) CheckSchema(ctx context.Context, eds []*dosa.EntityDefinition) ([]dosa.SchemaRef, error) {
	// convert the client EntityDefinition to the RPC EntityDefinition
	rpcEntityDefinition := make([]*dosarpc.EntityDefinition, len(eds))
	for i, ed := range eds {
		rpcEntityDefinition[i] = EntityDefinitionToThrift(ed)
	}
	csr := dosarpc.CheckSchemaRequest{EntityDefs: rpcEntityDefinition}

	_, err := y.Client.CheckSchema(ctx, &csr)

	if err != nil {
		return nil, err
	}

	// TODO: build up the EntityInfo objects

	return nil, nil
}

// UpsertSchema upserts the schema through RPC
func (y *Client) UpsertSchema(ctx context.Context, eds []*dosa.EntityDefinition) ([]dosa.SchemaRef, error) {
	rpcEds := make([]*dosarpc.EntityDefinition, len(eds))
	for i, ed := range eds {
		rpcEds[i] = EntityDefinitionToThrift(ed)
	}

	request := &dosarpc.UpsertSchemaRequest{
		EntityDefs: rpcEds,
	}

	_, err := y.Client.UpsertSchema(ctx, request)
	if err != nil {
		return nil, errors.Wrap(err, "rpc upsertSchema failed")
	}

	return nil, nil
}

// CreateScope is not implemented yet
func (y *Client) CreateScope(ctx context.Context, scope string) error {
	panic("not implemented")
}

// TruncateScope is not implemented yet
func (y *Client) TruncateScope(ctx context.Context, scope string) error {
	panic("not implemented")
}

// DropScope is not implemented yet
func (y *Client) DropScope(ctx context.Context, scope string) error {
	panic("not implemented")
}

func init() {
	// TODO: Actually create one of these connectors
	dosa.RegisterConnector("yarpc", func(map[string]interface{}) (dosa.Connector, error) {
		c := new(Client)
		c.Client = dosaclient.New(nil)
		return c, nil
	})
}
