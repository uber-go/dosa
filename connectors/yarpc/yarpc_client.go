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
	"fmt"

	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
	"github.com/uber-go/dosa"
	dosarpc "github.com/uber/dosa-idl/.gen/dosa"
	"github.com/uber/dosa-idl/.gen/dosa/dosaclient"
)

// SchemaReferenceInfo holds the info about a schema reference, which is what the client uses. This includes
// the schemaID to pass downstream as well as the map of column names to types
type SchemaReferenceInfo struct {
	schemaID dosarpc.SchemaID
	typeMap  map[string]dosa.Type
}

// Client holds the client-side RPC interface and some schema information
type Client struct {
	Client             dosaclient.Interface
	SchemaReferenceMap map[dosa.SchemaReference]SchemaReferenceInfo
}

func valueMapFromClientMap(values map[string]dosa.FieldValue) (fields map[string]*dosarpc.Value) {
	fields = map[string]*dosarpc.Value{}
	for name, value := range values {
		rpcValue := &dosarpc.Value{ElemValue: RawValueFromInterface(value)}
		fields[name] = rpcValue
	}
	return
}

// CreateIfNotExists ...
func (y *Client) CreateIfNotExists(ctx context.Context, sr dosa.SchemaReference, values map[string]dosa.FieldValue) error {
	// get the downstream schemaID
	schemaID := y.schemaIDFromReference(sr)
	if schemaID == nil {
		return fmt.Errorf("Invalid schema reference %q passed to CreateIfNotExists", sr)
	}

	// build the list of rpc Field objects from the raw name->value pairs
	fields := valueMapFromClientMap(values)

	// Create the RPC entity object and make the request
	entity := dosarpc.Entity{SchemaID: schemaID, Fields: fields}
	createRequest := dosarpc.CreateRequest{&entity}
	return y.Client.CreateIfNotExists(ctx, &createRequest)
}

// Upsert inserts or updates your data
func (y *Client) Upsert(ctx context.Context, sr dosa.SchemaReference, values map[string]dosa.FieldValue, fieldsToUpdate []string) error {

	// get the downstream schemaID
	schemaID := y.schemaIDFromReference(sr)
	if schemaID == nil {
		return fmt.Errorf("Invalid schema reference %q passed to Upsert", sr)
	}

	// build the list of rpc Field objects from the raw name->value pairs
	fields := valueMapFromClientMap(values)

	var updateFields map[string]struct{}
	if fieldsToUpdate != nil {
		updateFields = map[string]struct{}{}

		for _, name := range fieldsToUpdate {
			updateFields[name] = struct{}{}
		}
	}

	// Create the RPC entity object and make the request
	entity := dosarpc.Entity{SchemaID: schemaID, Fields: fields}
	upsertRequest := dosarpc.UpsertRequest{Entities: []*dosarpc.Entity{&entity}, FieldsToUpdate: updateFields}
	return y.Client.Upsert(ctx, &upsertRequest)

}

// Read reads a single entity
func (y *Client) Read(ctx context.Context, sr dosa.SchemaReference, keys map[string]dosa.FieldValue, fieldsToRead []string) (map[string]dosa.FieldValue, error) {

	// Given the client's SchemaReference, find the SchemaReferenceInformation to get the SchemaID and the type information for the columns
	/*
		sri, ok := y.SchemaReferenceMap[sr]
		if !ok {
			return nil, fmt.Errorf("Invalid schema reference %q passed to Read", sr)
		}
		schemaID := sri.schemaID
	*/
	fqn := dosarpc.FQN("myteam.service")
	version := dosarpc.Version(1)
	schemaID := dosarpc.SchemaID{
		Fqn:     &fqn,
		Version: &version,
	}
	sri := SchemaReferenceInfo{schemaID: schemaID, typeMap: map[string]dosa.Type{}}

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
	readRequest := dosarpc.ReadRequest{SchemaID: &schemaID, Key: rpcFields, FieldsToRead: rpcFieldsToRead}
	response, err := y.Client.Read(ctx, &readRequest)
	if err != nil {
		return nil, err
	}

	// no error, so for each column, transform it into the map of (col->value) items
	result := map[string]dosa.FieldValue{}
	for name, value := range response.Entity.Fields {
		if dosaType, ok := sri.typeMap[name]; ok {
			result[name] = RawValueAsInterface(*value.ElemValue, dosaType)
		} else {
			// skip fields that we do not know the type (continue is just for readability)
			continue
		}
	}
	return result, nil
}

// BatchRead is not yet implemented
func (y *Client) BatchRead(ctx context.Context, sr dosa.SchemaReference, keys []map[string]dosa.FieldValue, fieldsToRead []string) ([]dosa.FieldValuesOrError, error) {
	panic("not implemented")
}

// BatchUpsert is not yet implemented
func (y *Client) BatchUpsert(ctx context.Context, sr dosa.SchemaReference, values []map[string]dosa.FieldValue, fieldsToUpdate []string) ([]error, error) {
	panic("not implemented")
}

// Remove is not yet implemented
func (y *Client) Remove(ctx context.Context, sr dosa.SchemaReference, keys map[string]dosa.FieldValue) error {
	panic("not implemented")
}

// Range is not yet implemented
func (y *Client) Range(ctx context.Context, sr dosa.SchemaReference, conditions []dosa.Condition, fieldsToRead []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	panic("not implemented")
}

// Search is not yet implemented
func (y *Client) Search(ctx context.Context, sr dosa.SchemaReference, FieldNameValuePair []string, fieldsToRead []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	panic("not implemented")
}

// Scan is not yet implemented
func (y *Client) Scan(ctx context.Context, sr dosa.SchemaReference, fieldsToRead []string, token string, limit int) ([]map[string]dosa.FieldValue, error) {
	panic("not implemented")
}

// CheckSchema is one way to register a set of entities. This can be further validated by
// a schema service downstream.
func (y *Client) CheckSchema(ctx context.Context, ed []*dosa.EntityDefinition) ([]dosa.SchemaReference, error) {
	// convert the client EntityDefinition to the RPC EntityDefinition
	rpcEntityDefinition := make([]*dosarpc.EntityDefinition, len(ed))
	for i, def := range ed {
		ck := make([]*dosarpc.ClusteringKey, len(def.Key.ClusteringKeys))
		for ckinx, clusteringKey := range def.Key.ClusteringKeys {
			// TODO: The client uses 'descending' but the RPC uses 'ascending'? Fix this insanity!
			ascending := !clusteringKey.Descending
			name := clusteringKey.Name
			ck[ckinx] = &dosarpc.ClusteringKey{Name: &name, Asc: &ascending}
		}
		pk := dosarpc.PrimaryKey{PartitionKeys: def.Key.PartitionKeys, ClusteringKeys: ck}
		fd := make(map[string]*dosarpc.FieldDesc, len(def.Columns))
		for _, column := range def.Columns {
			rpcType := RPCTypeFromClientType(column.Type)
			fd[column.Name] = &dosarpc.FieldDesc{Type: &rpcType}
		}
		// TODO: set Fqn
		rpcEntityDefinition[i] = &dosarpc.EntityDefinition{PrimaryKey: &pk, FieldDescs: fd}
	}
	csr := dosarpc.CheckSchemaRequest{EntityDefs: rpcEntityDefinition}

	response, err := y.Client.CheckSchema(ctx, &csr)

	if err != nil {
		return nil, err
	}

	// hang on to type information and the SchemaID for future operations
	y.SchemaReferenceMap = map[dosa.SchemaReference]SchemaReferenceInfo{}
	srList := make([]dosa.SchemaReference, len(ed))
	for inx, ed := range ed {
		schemaID := response.SchemaIDs[inx]
		sri := SchemaReferenceInfo{schemaID: *schemaID, typeMap: map[string]dosa.Type{}}
		for _, columnDefinition := range ed.Columns {
			sri.typeMap[columnDefinition.Name] = columnDefinition.Type
		}
		// This uuid is the key to find the SchemaReferenceInfo
		uuidref := dosa.SchemaReference(uuid.NewV4().String())
		y.SchemaReferenceMap[uuidref] = sri
		srList[inx] = uuidref
	}
	return srList, nil
}

func (y *Client) schemaIDFromReference(sr dosa.SchemaReference) *dosarpc.SchemaID {
	schemaReference, ok := y.SchemaReferenceMap[sr]
	if !ok {
		return nil
	}
	schemaID := schemaReference.schemaID
	return &schemaID
}

// UpsertSchema upserts the schema through RPC
func (y *Client) UpsertSchema(ctx context.Context, eds []*dosa.EntityDefinition) error {
	// TODO: hard-coding for demo
	fqn := dosarpc.FQN("myteam.service")
	version := dosarpc.Version(1)

	y.SchemaReferenceMap = map[dosa.SchemaReference]SchemaReferenceInfo{}
	for _, ed := range eds {
		schemaID := &dosarpc.SchemaID{Fqn: &fqn, Version: &version}
		sri := SchemaReferenceInfo{schemaID: *schemaID, typeMap: map[string]dosa.Type{}}
		for _, columnDefinition := range ed.Columns {
			sri.typeMap[columnDefinition.Name] = columnDefinition.Type
		}
		// This uuid is the key to find the SchemaReferenceInfo
		uuidref := dosa.SchemaReference(uuid.NewV4().String())
		y.SchemaReferenceMap[uuidref] = sri
	}

	rpcEds := make([]*dosarpc.EntityDefinition, len(eds))
	for i, ed := range eds {
		rpcEds[i] = EntityDefinitionToThrift(ed)
	}

	request := &dosarpc.UpsertSchemaRequest{
		EntityDefs: rpcEds,
	}

	if err := y.Client.UpsertSchema(ctx, request); err != nil {
		return errors.Wrap(err, "rpc upsertSchema failed")
	}

	return nil
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
	dosa.RegisterConnector("yarpc", func() (dosa.Connector, error) {
		c := new(Client)
		c.Client = dosaclient.New(nil)
		return c, nil
	})
}
