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

package yarpc

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
	dosarpc "github.com/uber/dosa-idl/.gen/dosa"
	"github.com/uber/dosa-idl/.gen/dosa/dosaclient"
	rpc "go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/tchannel"
)

const (
	_version                    = "version"
	errCodeNotFound      int32  = 404
	errCodeAlreadyExists int32  = 409
	errConnectionRefused string = "getsockopt: connection refused"
)

// ErrConnectionRefused is used to help deliver a better error message when
// users have misconfigured the yarpc connector
type ErrConnectionRefused struct {
	cause error
}

// Error implements the error interface
func (e *ErrConnectionRefused) Error() string {
	return fmt.Sprintf("the gateway is not reachable, make sure the hostname and port are correct for your environment: %s", e.cause)
}

// ErrorIsConnectionRefused check if the error is "ErrConnectionRefused"
func ErrorIsConnectionRefused(err error) bool {
	return strings.Contains(err.Error(), errConnectionRefused)
}

// Config contains the YARPC connector parameters.
type Config struct {
	Host         string `yaml:"host"`
	Port         string `yaml:"port"`
	CallerName   string `yaml:"callerName"`
	ServiceName  string `yaml:"serviceName"`
	ExtraHeaders map[string]string
}

// Connector holds the client-side RPC interface and some schema information
type Connector struct {
	client     dosaclient.Interface
	dispatcher *rpc.Dispatcher
	headers    map[string]string
}

// NewConnector creates a new instance with user provided transport
func NewConnector(config Config) (*Connector, error) {
	// Ensure host, port, serviceName, and callerName are all specified.
	if config.Host == "" {
		return nil, errors.New("no host specified")
	}

	if config.Port == "" {
		return nil, errors.New("no port specified")
	}

	if config.ServiceName == "" {
		return nil, errors.New("no serviceName specified")
	}

	if config.CallerName == "" {
		return nil, errors.New("no callerName specified")
	}

	ycfg := rpc.Config{Name: config.CallerName}
	hostPort := fmt.Sprintf("%s:%s", config.Host, config.Port)
	// this looks wrong, BUT since it's a uni-directional tchannel
	// connection, we have to pass CallerName as the tchannel "ServiceName"
	// for source/destination to be reported correctly by RPC layer.
	ts, err := tchannel.NewChannelTransport(tchannel.ServiceName(config.CallerName))
	if err != nil {
		return nil, err
	}
	ycfg.Outbounds = rpc.Outbounds{
		config.ServiceName: {
			Unary: ts.NewSingleOutbound(hostPort),
		},
	}

	// important to note that this will panic if config contains invalid
	// values such as service name containing invalid characters
	dispatcher := rpc.NewDispatcher(ycfg)
	if err := dispatcher.Start(); err != nil {
		return nil, err
	}

	client := dosaclient.New(dispatcher.ClientConfig(config.ServiceName))

	return &Connector{
		dispatcher: dispatcher,
		client:     client,
		headers:    checkHeaders(config.ExtraHeaders, config.CallerName),
	}, nil
}

// checkHeaders ensures that X-Uber-Source is set.
func checkHeaders(headers map[string]string, caller string) map[string]string {
	if headers == nil {
		headers = map[string]string{}
	}
	// We don't just check to see if there's a value; it has to be non-empty.
	if headers["X-Uber-Source"] == "" {
		headers["X-Uber-Source"] = caller
	}
	return headers
}

// CreateIfNotExists ...
func (c *Connector) CreateIfNotExists(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	ev, err := fieldValueMapFromClientMap(values)
	if err != nil {
		return err
	}

	ttl := dosa.NoTTL().Nanoseconds()
	if ei.TTL != nil {
		ttl = ei.TTL.Nanoseconds()
	}

	createRequest := dosarpc.CreateRequest{
		Ref:          entityInfoToSchemaRef(ei),
		EntityValues: ev,
		TTL:          &ttl,
	}

	err = c.client.CreateIfNotExists(ctx, &createRequest, getHeaders(c.headers)...)
	if err != nil {
		if be, ok := err.(*dosarpc.BadRequestError); ok {
			if be.ErrorCode != nil && *be.ErrorCode == errCodeAlreadyExists {
				return errors.Wrap(&dosa.ErrAlreadyExists{}, "failed to create")
			}
		}

		if !dosarpc.Dosa_CreateIfNotExists_Helper.IsException(err) {
			return errors.Wrap(err, "failed to CreateIfNotExists due to network issue")
		}
	}
	return errors.Wrap(err, "failed to CreateIfNotExists")
}

// Upsert inserts or updates your data
func (c *Connector) Upsert(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	ev, err := fieldValueMapFromClientMap(values)
	if err != nil {
		return err
	}

	ttl := dosa.NoTTL().Nanoseconds()
	if ei.TTL != nil {
		ttl = ei.TTL.Nanoseconds()
	}

	upsertRequest := dosarpc.UpsertRequest{
		Ref:          entityInfoToSchemaRef(ei),
		EntityValues: ev,
		TTL:          &ttl,
	}

	err = c.client.Upsert(ctx, &upsertRequest, getHeaders(c.headers)...)

	if !dosarpc.Dosa_Upsert_Helper.IsException(err) {
		return errors.Wrap(err, "failed to Upsert due to network issue")
	}

	return errors.Wrap(err, "failed to Upsert")
}

// MultiUpsert upserts multiple entities at one time
func (c *Connector) MultiUpsert(ctx context.Context, ei *dosa.EntityInfo, multiValues []map[string]dosa.FieldValue) ([]error, error) {
	values, err := fieldValueMapsFromClientMaps(multiValues)
	if err != nil {
		return nil, err
	}

	// ttl := dosa.NoTTL().Nanoseconds()
	// if ei.TTL != nil {
	// 	ttl = ei.TTL.Nanoseconds()
	// }

	// perform the multi upsert request
	request := &dosarpc.MultiUpsertRequest{
		Ref:      entityInfoToSchemaRef(ei),
		Entities: values,
		// TTL:      &ttl, mgode@ has not yet committed origin/ttl-for-multi-upsert
	}

	response, err := c.client.MultiUpsert(ctx, request, getHeaders(c.headers)...)
	if err != nil {
		if !dosarpc.Dosa_MultiUpsert_Helper.IsException(err) {
			return nil, errors.Wrap(err, "failed to MultiUpsert due to network issue")
		}

		return nil, errors.Wrap(err, "failed to MultiUpsert")
	}

	rpcErrors := response.Errors
	results := make([]error, len(rpcErrors))
	for i, rpcError := range rpcErrors {
		if rpcError != nil {
			results[i] = wrapIDLError(rpcError)
		}
	}

	return results, nil
}

// Read reads a single entity
func (c *Connector) Read(ctx context.Context, ei *dosa.EntityInfo, keys map[string]dosa.FieldValue, minimumFields []string) (map[string]dosa.FieldValue, error) {
	// Convert the fields from the client's map to a set of fields to read
	var rpcMinimumFields map[string]struct{}
	if minimumFields != nil {
		rpcMinimumFields = map[string]struct{}{}
		for _, field := range minimumFields {
			rpcMinimumFields[field] = struct{}{}
		}
	}

	// convert the key values from interface{} to RPC's Value
	rpcFields := make(dosarpc.FieldValueMap)
	for key, value := range keys {
		rv, err := RawValueFromInterface(value)
		if err != nil {
			return nil, errors.Wrapf(err, "Key field %q", key)
		}
		if rv == nil {
			continue
		}
		rpcValue := &dosarpc.Value{ElemValue: rv}
		rpcFields[key] = rpcValue
	}

	// perform the read request
	readRequest := &dosarpc.ReadRequest{
		Ref:          entityInfoToSchemaRef(ei),
		KeyValues:    rpcFields,
		FieldsToRead: rpcMinimumFields,
	}

	response, err := c.client.Read(ctx, readRequest, getHeaders(c.headers)...)
	if err != nil {
		if be, ok := err.(*dosarpc.BadRequestError); ok {
			if be.ErrorCode != nil && *be.ErrorCode == errCodeNotFound {
				return nil, errors.Wrap(&dosa.ErrNotFound{}, "Read failed: not found")
			}
		}

		if !dosarpc.Dosa_Read_Helper.IsException(err) {
			return nil, errors.Wrap(err, "failed to Read due to network issue")
		}

		return nil, errors.Wrap(err, "failed to Read")
	}

	// no error, so for each column, transform it into the map of (col->value) items

	return decodeResults(ei, response.EntityValues), nil
}

// MultiRead reads multiple entities at one time
func (c *Connector) MultiRead(ctx context.Context, ei *dosa.EntityInfo, keys []map[string]dosa.FieldValue, minimumFields []string) ([]*dosa.FieldValuesOrError, error) {
	// Convert the fields from the client's map to a set of fields to read
	rpcMinimumFields := makeRPCminimumFields(minimumFields)

	// convert the keys to RPC's Value
	rpcFields := make([]dosarpc.FieldValueMap, len(keys))
	for i, kmap := range keys {
		rpcFields[i] = make(dosarpc.FieldValueMap)
		for key, value := range kmap {
			rv, err := RawValueFromInterface(value)
			if err != nil {
				return nil, err
			}
			if rv == nil {
				continue
			}
			rpcValue := &dosarpc.Value{ElemValue: rv}
			rpcFields[i][key] = rpcValue
		}
	}

	// perform the multi read request
	request := &dosarpc.MultiReadRequest{
		Ref:          entityInfoToSchemaRef(ei),
		KeyValues:    rpcFields,
		FieldsToRead: rpcMinimumFields,
	}

	response, err := c.client.MultiRead(ctx, request, getHeaders(c.headers)...)
	if err != nil {
		if !dosarpc.Dosa_MultiRead_Helper.IsException(err) {
			return nil, errors.Wrap(err, "failed to MultiRead due to network issue")
		}

		return nil, errors.Wrap(err, "failed to MultiRead")
	}

	rpcResults := response.Results
	results := make([]*dosa.FieldValuesOrError, len(rpcResults))
	for i, rpcResult := range rpcResults {
		results[i] = &dosa.FieldValuesOrError{Values: make(map[string]dosa.FieldValue), Error: nil}
		for name, value := range rpcResult.EntityValues {
			for _, col := range ei.Def.Columns {
				if col.Name == name {
					results[i].Values[name] = RawValueAsInterface(*value.ElemValue, col.Type)
					break
				}
			}
		}
		if rpcResult.Error != nil {
			results[i].Error = wrapIDLError(rpcResult.Error)
		}
	}

	return results, nil
}

// Remove marshals a request to the YARPC remove call
func (c *Connector) Remove(ctx context.Context, ei *dosa.EntityInfo, keys map[string]dosa.FieldValue) error {
	// convert the key values from interface{} to RPC's Value
	rpcFields, err := keyValuesToRPCValues(keys)
	if err != nil {
		return err
	}

	// perform the remove request
	removeRequest := &dosarpc.RemoveRequest{
		Ref:       entityInfoToSchemaRef(ei),
		KeyValues: rpcFields,
	}

	err = c.client.Remove(ctx, removeRequest, getHeaders(c.headers)...)
	if err != nil {
		if !dosarpc.Dosa_Remove_Helper.IsException(err) {
			return errors.Wrap(err, "failed to Remove due to network issue")
		}

		return errors.Wrap(err, "failed to Remove")
	}
	return nil
}

// MultiRemove is not yet implemented
func (c *Connector) MultiRemove(ctx context.Context, ei *dosa.EntityInfo, multiKeys []map[string]dosa.FieldValue) ([]error, error) {
	keyValues, err := multiKeyValuesToRPCValues(multiKeys)
	if err != nil {
		return nil, err
	}

	// perform the multi remove request
	request := &dosarpc.MultiRemoveRequest{
		Ref:       entityInfoToSchemaRef(ei),
		KeyValues: keyValues,
	}

	response, err := c.client.MultiRemove(ctx, request, getHeaders(c.headers)...)
	if err != nil {
		if !dosarpc.Dosa_MultiRemove_Helper.IsException(err) {
			return nil, errors.Wrap(err, "failed to MultiRemove due to network issue")
		}

		return nil, errors.Wrap(err, "failed to MultiRemove")
	}

	rpcErrors := response.Errors
	results := make([]error, len(rpcErrors))
	for i, rpcError := range rpcErrors {
		if rpcError != nil {
			results[i] = wrapIDLError(rpcError)
		}
	}

	return results, nil
}

// RemoveRange removes all entities within the range specified by the columnConditions.
func (c *Connector) RemoveRange(ctx context.Context, ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition) error {
	rpcConditions, err := createRPCConditions(columnConditions)
	if err != nil {
		return errors.Wrap(err, "RemoveRange failed: invalid column conditions")
	}

	request := &dosarpc.RemoveRangeRequest{
		Ref:        entityInfoToSchemaRef(ei),
		Conditions: rpcConditions,
	}

	if err := c.client.RemoveRange(ctx, request, getHeaders(c.headers)...); err != nil {
		if !dosarpc.Dosa_RemoveRange_Helper.IsException(err) {
			return errors.Wrap(err, "failed to RemoveRange due to network issue")
		}
		return errors.Wrap(err, "failed to RemoveRange")
	}
	return nil
}

// Range does a scan across a range
func (c *Connector) Range(ctx context.Context, ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition, minimumFields []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	limit32 := int32(limit)
	rpcMinimumFields := makeRPCminimumFields(minimumFields)
	rpcConditions, err := createRPCConditions(columnConditions)
	if err != nil {
		return nil, "", errors.Wrap(err, "failed to Range: invalid column conditions")
	}
	rangeRequest := dosarpc.RangeRequest{
		Ref:          entityInfoToSchemaRef(ei),
		Token:        &token,
		Limit:        &limit32,
		Conditions:   rpcConditions,
		FieldsToRead: rpcMinimumFields,
	}
	response, err := c.client.Range(ctx, &rangeRequest, getHeaders(c.headers)...)
	if err != nil {
		if !dosarpc.Dosa_Range_Helper.IsException(err) {
			return nil, "", errors.Wrap(err, "failed to Range due to network issue")
		}

		return nil, "", errors.Wrap(err, "failed to Range")
	}
	results := make([]map[string]dosa.FieldValue, len(response.Entities))
	for idx, entity := range response.Entities {
		results[idx] = decodeResults(ei, entity)
	}
	return results, *response.NextToken, nil
}

func createRPCConditions(columnConditions map[string][]*dosa.Condition) ([]*dosarpc.Condition, error) {
	rpcConditions := []*dosarpc.Condition{}
	for field, conditions := range columnConditions {
		// Warning: Don't remove this line.
		// field variable always has the same address. If we want to dereference it, we have to assign the value to a new variable.
		fieldName := field
		for _, condition := range conditions {
			rv, err := RawValueFromInterface(condition.Value)
			if err != nil {
				return nil, errors.Wrap(err, "Bad range value")
			}
			if rv == nil {
				continue
			}
			rpcConditions = append(rpcConditions, &dosarpc.Condition{
				Op:    encodeOperator(condition.Op),
				Field: &dosarpc.Field{Name: &fieldName, Value: &dosarpc.Value{ElemValue: rv}},
			})
		}
	}

	return rpcConditions, nil
}

// Scan marshals a scan request into YARPC
func (c *Connector) Scan(ctx context.Context, ei *dosa.EntityInfo, minimumFields []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	limit32 := int32(limit)
	rpcMinimumFields := makeRPCminimumFields(minimumFields)
	scanRequest := dosarpc.ScanRequest{
		Ref:          entityInfoToSchemaRef(ei),
		Token:        &token,
		Limit:        &limit32,
		FieldsToRead: rpcMinimumFields,
	}
	response, err := c.client.Scan(ctx, &scanRequest, getHeaders(c.headers)...)
	if err != nil {
		if !dosarpc.Dosa_Scan_Helper.IsException(err) {
			return nil, "", errors.Wrap(err, "failed to Scan due to network issue")
		}

		return nil, "", errors.Wrap(err, "failed to Scan")
	}
	results := make([]map[string]dosa.FieldValue, len(response.Entities))
	for idx, entity := range response.Entities {
		results[idx] = decodeResults(ei, entity)
	}
	return results, *response.NextToken, nil
}

// CheckSchema is one way to register a set of entities. This can be further validated by
// a schema service downstream.
func (c *Connector) CheckSchema(ctx context.Context, scope, namePrefix string, eds []*dosa.EntityDefinition) (int32, error) {
	// convert the client EntityDefinition to the RPC EntityDefinition
	rpcEntityDefinition := EntityDefsToThrift(eds)
	csr := dosarpc.CheckSchemaRequest{
		EntityDefs: rpcEntityDefinition,
		Scope:      &scope,
		NamePrefix: &namePrefix,
	}
	response, err := c.client.CheckSchema(ctx, &csr, getHeaders(c.headers)...)
	if err != nil {
		if !dosarpc.Dosa_CheckSchema_Helper.IsException(err) {
			return dosa.InvalidVersion, errors.Wrap(err, "failed to CheckSchema due to network issue")
		}

		return dosa.InvalidVersion, wrapError(err, "failed to CheckSchema", scope)
	}

	return *response.Version, nil
}

// CanUpsertSchema checks whether the provided entities are compatible with the latest applied schema.
// A non-nil error indicates the entities are not backward-compatible with the latest schema, thus will fail
// if they were to be upserted.
func (c *Connector) CanUpsertSchema(ctx context.Context, scope, namePrefix string, eds []*dosa.EntityDefinition) (int32, error) {
	// convert the client EntityDefinition to the RPC EntityDefinition
	rpcEntityDefinition := EntityDefsToThrift(eds)
	csr := dosarpc.CanUpsertSchemaRequest{
		EntityDefs: rpcEntityDefinition,
		Scope:      &scope,
		NamePrefix: &namePrefix,
	}
	response, err := c.client.CanUpsertSchema(ctx, &csr, getHeaders(c.headers)...)
	if err != nil {
		if !dosarpc.Dosa_CanUpsertSchema_Helper.IsException(err) {
			return dosa.InvalidVersion, errors.Wrap(err, "failed to CanUpsertSchema due to network issue")
		}
		return dosa.InvalidVersion, wrapError(err, "failed to CanUpsertSchema", scope)
	}

	return *response.Version, nil
}

// UpsertSchema upserts the schema through RPC
func (c *Connector) UpsertSchema(ctx context.Context, scope, namePrefix string, eds []*dosa.EntityDefinition) (*dosa.SchemaStatus, error) {
	rpcEds := EntityDefsToThrift(eds)
	request := &dosarpc.UpsertSchemaRequest{
		Scope:      &scope,
		NamePrefix: &namePrefix,
		EntityDefs: rpcEds,
	}

	response, err := c.client.UpsertSchema(ctx, request, getHeaders(c.headers)...)
	if err != nil {
		if !dosarpc.Dosa_UpsertSchema_Helper.IsException(err) {
			return nil, errors.Wrap(err, "failed to UpsertSchema due to network issue")
		}
		return nil, wrapError(err, "failed to UpsertSchema", scope)
	}

	status := ""
	if response.Status != nil {
		status = *response.Status
	}

	if response.Version == nil {
		return nil, errors.New("failed to UpsertSchema: server returns version nil")
	}

	return &dosa.SchemaStatus{
		Version: *response.Version,
		Status:  status,
	}, nil
}

// CheckSchemaStatus checks the status of specific version of schema
func (c *Connector) CheckSchemaStatus(ctx context.Context, scope, namePrefix string, version int32) (*dosa.SchemaStatus, error) {
	request := dosarpc.CheckSchemaStatusRequest{Scope: &scope, NamePrefix: &namePrefix, Version: &version}
	response, err := c.client.CheckSchemaStatus(ctx, &request, getHeaders(c.headers)...)

	if err != nil {
		if !dosarpc.Dosa_CheckSchemaStatus_Helper.IsException(err) {
			return nil, errors.Wrap(err, "failed to CheckSchemaStatus due to network issue")
		}

		return nil, wrapError(err, "failed to CheckSchemaStatus", scope)
	}

	status := ""
	if response.Status != nil {
		status = *response.Status
	}

	if response.Version == nil {
		return nil, errors.New("failed to ChecksShemaStatus: server returns version nil")
	}

	return &dosa.SchemaStatus{
		Version: *response.Version,
		Status:  status,
	}, nil
}

// GetEntitySchema gets the schema for the specified entity.
func (c *Connector) GetEntitySchema(ctx context.Context, scope, namePrefix, entityName string, version int32) (*dosa.EntityDefinition, error) {
	// We're not going to implement this at the moment since it's not needed. However, it could be easily
	// implemented by adding a new method to the thrift idl
	panic("Not implemented")
}

// CreateScope creates the scope specified
func (c *Connector) CreateScope(ctx context.Context, md *dosa.ScopeMetadata) error {
	bytes, err := json.Marshal(*md)
	if err != nil {
		return errors.Wrap(err, "could not encode metadata into JSON")
	}
	mds := string(bytes)

	request := &dosarpc.CreateScopeRequest{
		Name:      &(md.Name),
		Requester: &(md.Creator),
		Metadata:  &mds,
	}

	if err = c.client.CreateScope(ctx, request, getHeaders(c.headers)...); err != nil {
		if !dosarpc.Dosa_CreateScope_Helper.IsException(err) {
			return errors.Wrap(err, "failed to CreateScope due to network issue")
		}

		return errors.Wrap(err, "failed to CreateScope")
	}

	return nil
}

// TruncateScope truncates all data in the scope specified
func (c *Connector) TruncateScope(ctx context.Context, scope string) error {
	// A better authn story is needed -- an evildoer could just craft a request with a
	// bogus owner name and send it directly, bypassing this client.
	request := &dosarpc.TruncateScopeRequest{
		Name:      &scope,
		Requester: dosa.GetUsername(),
	}

	if err := c.client.TruncateScope(ctx, request, getHeaders(c.headers)...); err != nil {
		if !dosarpc.Dosa_TruncateScope_Helper.IsException(err) {
			return errors.Wrap(err, "failed to TruncateScope due to network issue")
		}

		return errors.Wrap(err, "failed to TruncateScope")
	}

	return nil
}

// DropScope removes the scope specified
func (c *Connector) DropScope(ctx context.Context, scope string) error {
	// A better authn story is needed -- an evildoer could just craft a request with a
	// bogus owner name and send it directly, bypassing this client.
	request := &dosarpc.DropScopeRequest{
		Name:      &scope,
		Requester: dosa.GetUsername(),
	}

	if err := c.client.DropScope(ctx, request, getHeaders(c.headers)...); err != nil {
		if !dosarpc.Dosa_DropScope_Helper.IsException(err) {
			return errors.Wrap(err, "failed to DropScope due to network issue")
		}

		return errors.Wrap(err, "failed to DropScope")
	}

	return nil
}

// ScopeExists is not implemented yet
func (c *Connector) ScopeExists(ctx context.Context, scope string) (bool, error) {
	panic("not implemented")
}

// Shutdown stops the dispatcher and drains client
func (c *Connector) Shutdown() error {
	return c.dispatcher.Stop()
}

func wrapError(err error, message, scope string) error {
	if ErrorIsConnectionRefused(err) {
		err = &ErrConnectionRefused{err}
	}
	return errors.Wrap(err, message)
}

func wrapIDLError(err *dosarpc.Error) error {
	if err.ErrCode != nil && *err.ErrCode == errCodeNotFound {
		return errors.Wrap(&dosa.ErrNotFound{}, "read failed: not found")
	}
	// TODO check other fields in the thrift error object such as ShouldRetry
	return errors.New(*err.Msg)
}
