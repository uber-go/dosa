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
	"strings"

	"os"

	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/base"
	dosarpc "github.com/uber/dosa-idl/.gen/dosa"
	"github.com/uber/dosa-idl/.gen/dosa/dosaclient"
	rpc "go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/transport/http"
	"go.uber.org/yarpc/transport/tchannel"
)

const (
	_version                    = "version"
	_defaultServiceName         = "dosa-gateway"
	errCodeNotFound      int32  = 404
	errCodeAlreadyExists int32  = 409
	errInvalidHandler    string = "no handler for service"
	errConnectionRefused string = "getsockopt: connection refused"
)

// ErrInvalidHandler is used to help deliver a better error message when
// users have misconfigured the yarpc connector
type ErrInvalidHandler struct {
	service string
	scope   string
}

// Error implements the error interface
func (e *ErrInvalidHandler) Error() string {
	return fmt.Sprintf("the gateway %q refused to handle scope %q; probably because of a mismatch between gateway and scope in your configuration or initialization", e.service, e.scope)
}

// ErrorIsInvalidHandler check if the error is "ErrInvalidHandler"
func ErrorIsInvalidHandler(err error) bool {
	return strings.Contains(err.Error(), errInvalidHandler)
}

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

// Config contains the YARPC client parameters
type Config struct {
	Transport    string                  `yaml:"transport"`
	Host         string                  `yaml:"host"`
	Port         string                  `yaml:"port"`
	CallerName   string                  `yaml:"callerName"`
	ServiceName  string                  `yaml:"serviceName"`
	ClientConfig *transport.ClientConfig `yaml:"clientConfig"`
}

// Connector holds the client-side RPC interface and some schema information
type Connector struct {
	base.Connector
	Client     dosaclient.Interface
	Config     *Config
	dispatcher *rpc.Dispatcher
}

// NewConnectorWithTransport creates a new instance with user provided transport
func NewConnectorWithTransport(cc transport.ClientConfig) *Connector {
	client := dosaclient.New(cc)
	config := &Config{
		CallerName:   cc.Caller(),
		ServiceName:  cc.Service(),
		ClientConfig: &cc,
	}
	return &Connector{
		Client: client,
		Config: config,
	}
}

// NewConnectorWithChannel creates a new instance using the given tchannel.
// Note that the method refers to the YARPC tchannel interface which should
// be satisfied in order to be used with this connector (and YARPC). Use this
// if you are using a raw TChannel configuration instead of YARPC directly.
func NewConnectorWithChannel(ch tchannel.Channel) (*Connector, error) {
	ts, err := tchannel.NewChannelTransport(tchannel.WithChannel(ch))
	if err != nil {
		return nil, err
	}
	// use the channel's service name for "caller" and use default for outbound
	// configuration since we just need a consistent mapping to get client.
	ycfg := rpc.Config{
		Name: ts.Channel().ServiceName(),
		Outbounds: rpc.Outbounds{
			_defaultServiceName: {
				Unary: ts.NewOutbound(),
			},
		},
	}

	dispatcher := rpc.NewDispatcher(ycfg)
	if err := dispatcher.Start(); err != nil {
		// this should never happen since we're always providing sane defaults
		return nil, err
	}

	client := dosaclient.New(dispatcher.ClientConfig(_defaultServiceName))
	config := &Config{
		CallerName:  ycfg.Name,
		ServiceName: _defaultServiceName,
	}
	return &Connector{
		Client:     client,
		Config:     config,
		dispatcher: dispatcher,
	}, nil
}

// NewConnector returns a new YARPC connector with the given configuration.
func NewConnector(cfg *Config) (*Connector, error) {
	ycfg := rpc.Config{Name: cfg.CallerName}

	// host and port are required
	if cfg.Host == "" {
		return nil, errors.New("invalid host")
	}

	if cfg.Port == "" {
		return nil, errors.New("invalid port")
	}

	// service name is not required, defaults to "dosa-gateway"
	if cfg.ServiceName == "" {
		cfg.ServiceName = _defaultServiceName
	}

	switch cfg.Transport {
	case "http":
		uri := fmt.Sprintf("http://%s:%s", cfg.Host, cfg.Port)
		ts := http.NewTransport()
		ycfg.Outbounds = rpc.Outbounds{
			cfg.ServiceName: {
				Unary: ts.NewSingleOutbound(uri),
			},
		}
	case "tchannel":
		hostPort := fmt.Sprintf("%s:%s", cfg.Host, cfg.Port)
		// this looks wrong, BUT since it's a uni-directional tchannel
		// connection, we have to pass CallerName as the tchannel "ServiceName"
		// for source/destination to be reported correctly by RPC layer.
		ts, err := tchannel.NewChannelTransport(tchannel.ServiceName(cfg.CallerName))
		if err != nil {
			return nil, err
		}
		ycfg.Outbounds = rpc.Outbounds{
			cfg.ServiceName: {
				Unary: ts.NewSingleOutbound(hostPort),
			},
		}
	default:
		return nil, errors.New("invalid transport (only http or tchannel supported)")
	}

	// important to note that this will panic if config contains invalid
	// values such as service name containing invalid characters
	dispatcher := rpc.NewDispatcher(ycfg)
	if err := dispatcher.Start(); err != nil {
		return nil, err
	}

	client := dosaclient.New(dispatcher.ClientConfig(cfg.ServiceName))
	return &Connector{
		Client:     client,
		Config:     cfg,
		dispatcher: dispatcher,
	}, nil
}

// CreateIfNotExists ...
func (c *Connector) CreateIfNotExists(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	ev, err := fieldValueMapFromClientMap(values)
	if err != nil {
		return err
	}
	createRequest := dosarpc.CreateRequest{
		Ref:          entityInfoToSchemaRef(ei),
		EntityValues: ev,
	}

	err = c.Client.CreateIfNotExists(ctx, &createRequest, VersionHeader())
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
	upsertRequest := dosarpc.UpsertRequest{
		Ref:          entityInfoToSchemaRef(ei),
		EntityValues: ev,
	}

	err = c.Client.Upsert(ctx, &upsertRequest, VersionHeader())

	if !dosarpc.Dosa_Upsert_Helper.IsException(err) {
		return errors.Wrap(err, "failed to Upsert due to network issue")
	}

	return errors.Wrap(err, "failed to Upsert")
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

	response, err := c.Client.Read(ctx, readRequest, VersionHeader())
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

	response, err := c.Client.MultiRead(ctx, request, VersionHeader())
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
			// TODO check other fields in the thrift error object such as ShouldRetry
			results[i].Error = errors.New(*rpcResult.Error.Msg)
		}
	}

	return results, nil
}

// MultiUpsert is not yet implemented
func (c *Connector) MultiUpsert(ctx context.Context, ei *dosa.EntityInfo, multiValues []map[string]dosa.FieldValue) ([]error, error) {
	panic("not implemented")
}

// Remove marshals a request to the YARPC remove call
func (c *Connector) Remove(ctx context.Context, ei *dosa.EntityInfo, keys map[string]dosa.FieldValue) error {
	// convert the key values from interface{} to RPC's Value
	rpcFields := make(dosarpc.FieldValueMap)
	for key, value := range keys {
		rv, err := RawValueFromInterface(value)
		if err != nil {
			return errors.Wrapf(err, "Key field %q", key)
		}
		if rv == nil {
			continue
		}
		rpcValue := &dosarpc.Value{ElemValue: rv}
		rpcFields[key] = rpcValue
	}

	// perform the remove request
	removeRequest := &dosarpc.RemoveRequest{
		Ref:       entityInfoToSchemaRef(ei),
		KeyValues: rpcFields,
	}

	err := c.Client.Remove(ctx, removeRequest, VersionHeader())
	if err != nil {
		if !dosarpc.Dosa_Remove_Helper.IsException(err) {
			return errors.Wrap(err, "failed to Remove due to network issue")
		}

		return errors.Wrap(err, "failed to Remove")
	}
	return nil
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

	if err := c.Client.RemoveRange(ctx, request, VersionHeader()); err != nil {
		if !dosarpc.Dosa_RemoveRange_Helper.IsException(err) {
			return errors.Wrap(err, "failed to RemoveRange due to network issue")
		}
		return errors.Wrap(err, "failed to RemoveRange")
	}
	return nil
}

// MultiRemove is not yet implemented
func (c *Connector) MultiRemove(ctx context.Context, ei *dosa.EntityInfo, multiKeys []map[string]dosa.FieldValue) ([]error, error) {
	panic("not implemented")
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
	response, err := c.Client.Range(ctx, &rangeRequest, VersionHeader())
	if err != nil {
		if !dosarpc.Dosa_Range_Helper.IsException(err) {
			return nil, "", errors.Wrap(err, "failed to Range due to network issue")
		}

		return nil, "", errors.Wrap(err, "failed to Range")
	}
	results := []map[string]dosa.FieldValue{}
	for _, entity := range response.Entities {
		results = append(results, decodeResults(ei, entity))
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
	response, err := c.Client.Scan(ctx, &scanRequest, VersionHeader())
	if err != nil {
		if !dosarpc.Dosa_Scan_Helper.IsException(err) {
			return nil, "", errors.Wrap(err, "failed to Scan due to network issue")
		}

		return nil, "", errors.Wrap(err, "failed to Scan")
	}
	results := []map[string]dosa.FieldValue{}
	for _, entity := range response.Entities {
		results = append(results, decodeResults(ei, entity))
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
	response, err := c.Client.CheckSchema(ctx, &csr, VersionHeader())
	if err != nil {
		if !dosarpc.Dosa_CheckSchema_Helper.IsException(err) {
			return dosa.InvalidVersion, errors.Wrap(err, "failed to CheckSchema due to network issue")
		}

		return dosa.InvalidVersion, wrapError(err, "failed to CheckSchema", scope, c.Config.ServiceName)
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
	response, err := c.Client.CanUpsertSchema(ctx, &csr, VersionHeader())
	if err != nil {
		if !dosarpc.Dosa_CanUpsertSchema_Helper.IsException(err) {
			return dosa.InvalidVersion, errors.Wrap(err, "failed to CanUpsertSchema due to network issue")
		}
		return dosa.InvalidVersion, wrapError(err, "failed to CanUpsertSchema", scope, c.Config.ServiceName)
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

	response, err := c.Client.UpsertSchema(ctx, request, VersionHeader())
	if err != nil {
		if !dosarpc.Dosa_UpsertSchema_Helper.IsException(err) {
			return nil, errors.Wrap(err, "failed to UpsertSchema due to network issue")
		}
		return nil, wrapError(err, "failed to UpsertSchema", scope, c.Config.ServiceName)
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
	response, err := c.Client.CheckSchemaStatus(ctx, &request, VersionHeader())

	if err != nil {
		if !dosarpc.Dosa_CheckSchemaStatus_Helper.IsException(err) {
			return nil, errors.Wrap(err, "failed to CheckSchemaStatus due to network issue")
		}

		return nil, wrapError(err, "failed to CheckSchemaStatus", scope, c.Config.ServiceName)
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

// CreateScope creates the scope specified
func (c *Connector) CreateScope(ctx context.Context, scope string, md *dosa.ScopeMetadata) error {
	request := &dosarpc.CreateScopeRequest{
		Name:      &scope,
		Requester: &(md.Creator),
		Owner:     &(md.Owner),
		Type:      &(md.Type),
	}

	if err := c.Client.CreateScope(ctx, request, VersionHeader()); err != nil {
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

	if err := c.Client.TruncateScope(ctx, request, VersionHeader()); err != nil {
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

	if err := c.Client.DropScope(ctx, request, VersionHeader()); err != nil {
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
	// instances w/ mocked client shouldn't require a dispatcher
	if c.dispatcher == nil {
		return nil
	}
	return c.dispatcher.Stop()
}

func wrapError(err error, message, scope, service string) error {
	if ErrorIsInvalidHandler(err) {
		err = &ErrInvalidHandler{scope: scope, service: service}
	}
	if ErrorIsConnectionRefused(err) {
		err = &ErrConnectionRefused{err}
	}
	return errors.Wrap(err, message)
}

func getWithDefault(args map[string]interface{}, elem string, def string) string {
	v, ok := args[elem]
	if ok {
		return v.(string)
	}
	return def

}

func init() {
	dosa.RegisterConnector("yarpc", func(args dosa.CreationArgs) (dosa.Connector, error) {
		host, ok := args["host"]
		if !ok {
			return nil, errors.New("Missing connector host value")
		}

		port, ok := args["port"]
		if !ok {
			return nil, errors.New("Missing connector port value")
		}

		trans := getWithDefault(args, "transport", "tchannel")
		callername := getWithDefault(args, "callername", os.Getenv("USER"))
		servicename := getWithDefault(args, "servicename", "test")
		cfg := Config{
			Transport:   trans,
			Host:        host.(string),
			Port:        port.(string),
			CallerName:  callername,
			ServiceName: servicename,
		}
		return NewConnector(&cfg)
	})
}
