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

package yarpc_test

import (
	"context"
	"testing"
	"time"

	"go.uber.org/yarpc/api/transport/transporttest"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/yarpc"
	drpc "github.com/uber/dosa-idl/.gen/dosa"
	"github.com/uber/dosa-idl/.gen/dosa/dosatest"
	tchan "github.com/uber/tchannel-go"
)

func testInt64Ptr(i int64) *int64 {
	return &i
}

func testInt32Ptr(i int32) *int32 {
	return &i
}

func testFloat64Ptr(f float64) *float64 {
	return &f
}

func testStringPtr(s string) *string {
	return &s
}

func testBoolPtr(b bool) *bool {
	return &b
}

var testEi = &dosa.EntityInfo{
	Ref: &testSchemaRef,
	Def: &dosa.EntityDefinition{
		Columns: []*dosa.ColumnDefinition{
			{Name: "f1", Type: dosa.String},
			{Name: "c1", Type: dosa.Int64},
			{Name: "c2", Type: dosa.Double},
			{Name: "c3", Type: dosa.String},
			{Name: "c4", Type: dosa.Blob},
			{Name: "c5", Type: dosa.Bool},
			{Name: "c6", Type: dosa.Int32},
		},
		Key: &dosa.PrimaryKey{
			PartitionKeys: []string{"f1"},
		},
		Name: "t1",
	},
}

var testSchemaRef = dosa.SchemaRef{
	Scope:      "scope1",
	NamePrefix: "namePrefix",
	EntityName: "eName",
	Version:    12345,
}

var testRPCSchemaRef = drpc.SchemaRef{
	Scope:      testStringPtr("scope1"),
	NamePrefix: testStringPtr("namePrefix"),
	EntityName: testStringPtr("eName"),
	Version:    testInt32Ptr(12345),
}

var ctx = context.Background()

func TestYaRPCClient_NewConnectorWithTransport(t *testing.T) {
	ctrl := gomock.NewController(t)
	cc := transporttest.NewMockClientConfig(ctrl)
	assert.NotNil(t, yarpc.NewConnectorWithTransport(cc))
}

func TestYaRPCClient_NewConnectorWithChannel(t *testing.T) {
	// if we can call this with a real tchannel instance, only errors can occur
	// when trying to initialize the dispatcher, which also shouldn't return
	// an error since we're providing a known, compatible configuration.
	ch, err := tchan.NewChannel("mysvc", &tchan.ChannelOptions{
		ProcessName: "pname",
	})
	assert.NoError(t, err)
	assert.NotNil(t, ch)
	conn, err := yarpc.NewConnectorWithChannel(ch)
	assert.NoError(t, err)
	assert.NotNil(t, conn)
}

func TestYaRPCClient_NewConnector(t *testing.T) {
	cases := []struct {
		cfg     yarpc.Config
		isErr   bool
		isPanic bool
	}{
		{
			// invalid host
			cfg:   yarpc.Config{},
			isErr: true,
		}, {
			// invalid port
			cfg: yarpc.Config{
				Host: "localhost",
			},
			isErr: true,
		}, {
			// invalid transport
			cfg: yarpc.Config{
				Host: "localhost",
				Port: "8080",
			},
			isErr: true,
		}, {
			// dispatcher start error (panic)
			cfg: yarpc.Config{
				Transport:   "http",
				Host:        "localhost",
				Port:        "8080",
				CallerName:  "-",
				ServiceName: "dosa-gateway",
			},
			isPanic: true,
		}, {
			// success
			cfg: yarpc.Config{
				Transport:   "http",
				Host:        "localhost",
				Port:        "8080",
				CallerName:  "dosa-test",
				ServiceName: "dosa-gateway",
			},
		}, {
			// success
			cfg: yarpc.Config{
				Transport:   "tchannel",
				Host:        "localhost",
				Port:        "8080",
				CallerName:  "dosa-test",
				ServiceName: "dosa-gateway",
			},
		},
	}

	for _, c := range cases {
		if c.isPanic {
			assert.Panics(t, func() {
				yarpc.NewConnector(&c.cfg)
			})
			continue
		}

		conn, err := yarpc.NewConnector(&c.cfg)
		if c.isErr {
			assert.Error(t, err)
			assert.Nil(t, conn)
			continue
		}
		assert.NoError(t, err)
		assert.NotNil(t, conn)
	}
}

// Test a happy path read of one column and specify the primary key
func TestYaRPCClient_Read(t *testing.T) {
	// build a mock RPC client
	ctrl := gomock.NewController(t)
	mockedClient := dosatest.NewMockClient(ctrl)

	// set up the parameters
	readRequest := &drpc.ReadRequest{
		Ref:          &testRPCSchemaRef,
		KeyValues:    map[string]*drpc.Value{"f1": {ElemValue: &drpc.RawValue{Int64Value: testInt64Ptr(5)}}},
		FieldsToRead: map[string]struct{}{"f1": {}},
	}

	// we expect a single call to Read, and we return back two fields, f1 which is in the typemap and another field that is not
	mockedClient.EXPECT().Read(ctx, readRequest).Return(&drpc.ReadResponse{drpc.FieldValueMap{
		"c1":               {ElemValue: &drpc.RawValue{Int64Value: testInt64Ptr(1)}},
		"fieldNotInSchema": {ElemValue: &drpc.RawValue{Int64Value: testInt64Ptr(5)}},
		"c2":               {ElemValue: &drpc.RawValue{DoubleValue: testFloat64Ptr(2.2)}},
		"c3":               {ElemValue: &drpc.RawValue{StringValue: testStringPtr("f3value")}},
		"c4":               {ElemValue: &drpc.RawValue{BinaryValue: []byte{'b', 'i', 'n', 'a', 'r', 'y'}}},
		"c5":               {ElemValue: &drpc.RawValue{BoolValue: testBoolPtr(false)}},
		"c6":               {ElemValue: &drpc.RawValue{Int32Value: testInt32Ptr(1)}},
	}}, nil)

	// Prepare the dosa client interface using the mocked RPC layer
	sut := yarpc.Connector{Client: mockedClient}

	// perform the read
	values, err := sut.Read(ctx, testEi, map[string]dosa.FieldValue{"f1": dosa.FieldValue(int64(5))}, []string{"f1"})
	assert.Nil(t, err)                      // not an error
	assert.NotNil(t, values)                // found some values
	assert.Equal(t, int64(1), values["c1"]) // the mapped field is found, and is the right type
	assert.Equal(t, float64(2.2), values["c2"])
	assert.Equal(t, "f3value", values["c3"])
	assert.Equal(t, []byte{'b', 'i', 'n', 'a', 'r', 'y'}, values["c4"])
	assert.Equal(t, false, values["c5"])
	assert.Equal(t, int32(1), values["c6"])
	assert.Empty(t, values["fieldNotInSchema"]) // the unknown field is not present

	errCode := int32(404)
	mockedClient.EXPECT().Read(ctx, readRequest).Return(nil, &drpc.BadRequestError{ErrorCode: &errCode})
	_, err = sut.Read(ctx, testEi, map[string]dosa.FieldValue{"f1": dosa.FieldValue(int64(5))}, []string{"f1"})
	assert.True(t, dosa.ErrorIsNotFound(err))

	// make sure we actually called Read on the interface
	ctrl.Finish()
}

func TestYaRPCClient_MultiRead(t *testing.T) {
	// build a mock RPC client
	ctrl := gomock.NewController(t)
	mockedClient := dosatest.NewMockClient(ctrl)

	// Prepare the dosa client interface using the mocked RPC layer
	sut := yarpc.Connector{Client: mockedClient}

	data := []struct {
		Request     *drpc.MultiReadRequest
		Response    *drpc.MultiReadResponse
		ResponseErr error
	}{
		{
			Request: &drpc.MultiReadRequest{
				Ref: &testRPCSchemaRef,
				KeyValues: []drpc.FieldValueMap{
					{
						"f1": &drpc.Value{ElemValue: &drpc.RawValue{Int64Value: testInt64Ptr(5)}},
					},
					{
						"f2": &drpc.Value{ElemValue: &drpc.RawValue{Int64Value: testInt64Ptr(6)}},
					},
				},
				FieldsToRead: map[string]struct{}{"f1": {}},
			},
			Response: &drpc.MultiReadResponse{
				Results: []*drpc.EntityOrError{
					{
						EntityValues: drpc.FieldValueMap{
							"c1":               {ElemValue: &drpc.RawValue{Int64Value: testInt64Ptr(1)}},
							"fieldNotInSchema": {ElemValue: &drpc.RawValue{Int64Value: testInt64Ptr(5)}},
							"c2":               {ElemValue: &drpc.RawValue{DoubleValue: testFloat64Ptr(2.2)}},
							"c3":               {ElemValue: &drpc.RawValue{StringValue: testStringPtr("f3value")}},
							"c4":               {ElemValue: &drpc.RawValue{BinaryValue: []byte{'b', 'i', 'n', 'a', 'r', 'y'}}},
							"c5":               {ElemValue: &drpc.RawValue{BoolValue: testBoolPtr(false)}},
							"c6":               {ElemValue: &drpc.RawValue{Int32Value: testInt32Ptr(1)}},
						},
						Error: nil,
					},
					{
						EntityValues: drpc.FieldValueMap{
							"c1":               {ElemValue: &drpc.RawValue{Int64Value: testInt64Ptr(2)}},
							"fieldNotInSchema": {ElemValue: &drpc.RawValue{Int64Value: testInt64Ptr(15)}},
							"c2":               {ElemValue: &drpc.RawValue{DoubleValue: testFloat64Ptr(12.2)}},
							"c3":               {ElemValue: &drpc.RawValue{StringValue: testStringPtr("f3value1")}},
							"c4":               {ElemValue: &drpc.RawValue{BinaryValue: []byte{'a', 'i', '1', 'a', 'r', 'y'}}},
							"c5":               {ElemValue: &drpc.RawValue{BoolValue: testBoolPtr(true)}},
							"c6":               {ElemValue: &drpc.RawValue{Int32Value: testInt32Ptr(2)}},
						},
						Error: nil,
					},
				},
			},
			ResponseErr: nil,
		},
		{
			Request: &drpc.MultiReadRequest{
				Ref: &testRPCSchemaRef,
				KeyValues: []drpc.FieldValueMap{
					{
						"f1": &drpc.Value{ElemValue: &drpc.RawValue{Int64Value: testInt64Ptr(5)}},
					},
					{
						"f2": &drpc.Value{ElemValue: &drpc.RawValue{Int64Value: testInt64Ptr(6)}},
					},
				},
				FieldsToRead: map[string]struct{}{"f1": {}},
			},
			Response:    nil,
			ResponseErr: errors.New("test error"),
		},
		{
			Request: &drpc.MultiReadRequest{
				Ref: &testRPCSchemaRef,
				KeyValues: []drpc.FieldValueMap{
					{
						"f1": &drpc.Value{ElemValue: &drpc.RawValue{Int64Value: testInt64Ptr(5)}},
					},
					{
						"f2": &drpc.Value{ElemValue: &drpc.RawValue{Int64Value: testInt64Ptr(6)}},
					},
				},
				FieldsToRead: map[string]struct{}{"f1": {}},
			},
			Response: &drpc.MultiReadResponse{
				Results: []*drpc.EntityOrError{
					{
						EntityValues: drpc.FieldValueMap{
							"c1":               {ElemValue: &drpc.RawValue{Int64Value: testInt64Ptr(1)}},
							"fieldNotInSchema": {ElemValue: &drpc.RawValue{Int64Value: testInt64Ptr(5)}},
							"c2":               {ElemValue: &drpc.RawValue{DoubleValue: testFloat64Ptr(2.2)}},
							"c3":               {ElemValue: &drpc.RawValue{StringValue: testStringPtr("f3value")}},
							"c4":               {ElemValue: &drpc.RawValue{BinaryValue: []byte{'b', 'i', 'n', 'a', 'r', 'y'}}},
							"c5":               {ElemValue: &drpc.RawValue{BoolValue: testBoolPtr(false)}},
							"c6":               {ElemValue: &drpc.RawValue{Int32Value: testInt32Ptr(1)}},
						},
						Error: nil,
					},
					{
						Error: &drpc.Error{Msg: testStringPtr("not found")},
					},
				},
			},
			ResponseErr: nil,
		},
	}

	for _, d := range data {
		mockedClient.EXPECT().MultiRead(ctx, d.Request).Return(d.Response, d.ResponseErr)
		// perform the multi read
		values, err := sut.MultiRead(ctx, testEi, []map[string]dosa.FieldValue{{"f1": dosa.FieldValue(int64(5))}, {"f2": dosa.FieldValue(int64(6))}}, []string{"f1"})
		if d.ResponseErr == nil {
			assert.Nil(t, err)       // not an error
			assert.NotNil(t, values) // found some values
			for i, v := range values {
				if v.Error != nil {
					assert.Contains(t, v.Error.Error(), *d.Response.Results[i].Error.Msg)
					continue
				}
				assert.Equal(t, v.Values["c1"], *d.Response.Results[i].EntityValues["c1"].ElemValue.Int64Value)
				assert.Empty(t, v.Values["fieldNotInSchema"])
				assert.Equal(t, v.Values["c2"], *d.Response.Results[i].EntityValues["c2"].ElemValue.DoubleValue)
				assert.Equal(t, v.Values["c3"], *d.Response.Results[i].EntityValues["c3"].ElemValue.StringValue)
				assert.Equal(t, v.Values["c4"], d.Response.Results[i].EntityValues["c4"].ElemValue.BinaryValue)
				assert.Equal(t, v.Values["c5"], *d.Response.Results[i].EntityValues["c5"].ElemValue.BoolValue)
				assert.Equal(t, v.Values["c6"], *d.Response.Results[i].EntityValues["c6"].ElemValue.Int32Value)
			}
			continue
		}

		assert.Error(t, err)
		assert.Contains(t, err.Error(), d.ResponseErr.Error())
	}

	// make sure we actually called Read on the interface
	ctrl.Finish()
}

func TestYaRPCClient_CreateIfNotExists(t *testing.T) {
	// build a mock RPC client
	ctrl := gomock.NewController(t)
	mockedClient := dosatest.NewMockClient(ctrl)

	// here are the data types to test; the names are random
	vals := []struct {
		Name  string
		Value interface{}
	}{
		{"c1", int64(1)},
		{"c2", float64(2.2)},
		{"c3", "string"},
		{"c4", []byte{'b', 'i', 'n', 'a', 'r', 'y'}},
		{"c5", false},
		{"c6", int32(2)},
		{"c7", time.Now()},
	}

	// build up the input field list and the output field list
	// the layout is quite different; inputs are a simple map but the actual RPC call expects a messier format
	inFields := map[string]dosa.FieldValue{}
	outFields := drpc.FieldValueMap{}
	for _, item := range vals {
		inFields[item.Name] = item.Value
		outFields[item.Name] = &drpc.Value{ElemValue: yarpc.RawValueFromInterface(item.Value)}
	}

	mockedClient.EXPECT().CreateIfNotExists(ctx, &drpc.CreateRequest{Ref: &testRPCSchemaRef, EntityValues: outFields})

	// create the YaRPCClient and give it the mocked RPC interface
	// see https://en.wiktionary.org/wiki/SUT for the reason this is called sut
	sut := yarpc.Connector{Client: mockedClient}

	// and run the test
	err := sut.CreateIfNotExists(ctx, testEi, inFields)
	assert.Nil(t, err)

	errCode := int32(409)
	mockedClient.EXPECT().CreateIfNotExists(ctx, &drpc.CreateRequest{Ref: &testRPCSchemaRef, EntityValues: outFields}).Return(
		&drpc.BadRequestError{ErrorCode: &errCode},
	)

	err = sut.CreateIfNotExists(ctx, testEi, inFields)
	assert.True(t, dosa.ErrorIsAlreadyExists(err))
	// make sure we actually called CreateIfNotExists on the interface
	ctrl.Finish()

	assert.NoError(t, sut.Shutdown())
}

func TestYaRPCClient_Upsert(t *testing.T) {
	// build a mock RPC client
	ctrl := gomock.NewController(t)
	mockedClient := dosatest.NewMockClient(ctrl)

	// here are the data types to test; the names are random
	vals := []struct {
		Name  string
		Value interface{}
	}{
		{"c1", int64(1)},
		{"c2", float64(2.2)},
		{"c3", "string"},
		{"c4", []byte{'b', 'i', 'n', 'a', 'r', 'y'}},
		{"c5", false},
		{"c6", int32(2)},
		{"c7", time.Now()},
	}

	// build up the input field list and the output field list
	// the layout is quite different; inputs are a simple map but the actual RPC call expects a messier format
	inFields := map[string]dosa.FieldValue{}
	outFields := map[string]*drpc.Value{}
	for _, item := range vals {
		inFields[item.Name] = item.Value
		outFields[item.Name] = &drpc.Value{ElemValue: yarpc.RawValueFromInterface(item.Value)}
	}

	mockedClient.EXPECT().Upsert(ctx, &drpc.UpsertRequest{
		Ref:          &testRPCSchemaRef,
		EntityValues: outFields,
	})

	// create the YaRPCClient and give it the mocked RPC interface
	// see https://en.wiktionary.org/wiki/SUT for the reason this is called sut
	sut := yarpc.Connector{Client: mockedClient}

	// and run the test, first with a nil FieldsToUpdate, then with a specific list
	err := sut.Upsert(ctx, testEi, inFields)
	assert.Nil(t, err)

	// make sure we actually called CreateIfNotExists on the interface
	ctrl.Finish()
}

type TestDosaObject struct {
	dosa.Entity `dosa:"primaryKey=(F1, F2)"`
	F1          int64
	F2          int32
}

func TestClient_CheckSchema(t *testing.T) {
	// build a mock RPC client
	ctrl := gomock.NewController(t)
	mockedClient := dosatest.NewMockClient(ctrl)
	sp := "scope"
	prefix := "prefix"

	sut := yarpc.Connector{Client: mockedClient}

	ed, err := dosa.TableFromInstance(&TestDosaObject{})
	assert.NoError(t, err)
	expectedRequest := &drpc.CheckSchemaRequest{
		Scope:      &sp,
		NamePrefix: &prefix,
		EntityDefs: []*drpc.EntityDefinition{yarpc.EntityDefinitionToThrift(&ed.EntityDefinition)},
	}
	v := int32(1)
	mockedClient.EXPECT().CheckSchema(ctx, gomock.Any()).Do(func(_ context.Context, request *drpc.CheckSchemaRequest) {
		assert.Equal(t, expectedRequest, request)
	}).Return(&drpc.CheckSchemaResponse{Version: &v}, nil)

	sr, err := sut.CheckSchema(ctx, sp, prefix, []*dosa.EntityDefinition{&ed.EntityDefinition})
	assert.NoError(t, err)
	assert.Equal(t, v, sr)
}

func TestClient_CheckSchemaStatus(t *testing.T) {
	// build a mock RPC client
	ctrl := gomock.NewController(t)
	mockedClient := dosatest.NewMockClient(ctrl)
	sp := "scope"
	prefix := "prefix"
	version := int32(1)
	sut := yarpc.Connector{Client: mockedClient}

	expectedRequest := &drpc.CheckSchemaStatusRequest{
		Scope:      &sp,
		NamePrefix: &prefix,
		Version:    &version,
	}

	mockedClient.EXPECT().CheckSchemaStatus(ctx, gomock.Any()).Do(func(_ context.Context, request *drpc.CheckSchemaStatusRequest) {
		assert.Equal(t, expectedRequest, request)
	}).Return(&drpc.CheckSchemaStatusResponse{Version: &version}, nil)

	sr, err := sut.CheckSchemaStatus(ctx, sp, prefix, version)
	assert.NoError(t, err)
	assert.Equal(t, version, sr.Version)
}

func TestClient_UpsertSchema(t *testing.T) {
	// build a mock RPC client
	ctrl := gomock.NewController(t)
	mockedClient := dosatest.NewMockClient(ctrl)
	sut := yarpc.Connector{Client: mockedClient}

	ed, err := dosa.TableFromInstance(&TestDosaObject{})
	assert.NoError(t, err)
	sp := "scope"
	prefix := "prefix"

	expectedRequest := &drpc.UpsertSchemaRequest{
		Scope:      &sp,
		NamePrefix: &prefix,
		EntityDefs: []*drpc.EntityDefinition{yarpc.EntityDefinitionToThrift(&ed.EntityDefinition)},
	}
	v := int32(1)
	mockedClient.EXPECT().UpsertSchema(ctx, gomock.Any()).Do(func(_ context.Context, request *drpc.UpsertSchemaRequest) {
		assert.Equal(t, expectedRequest, request)
	}).Return(&drpc.UpsertSchemaResponse{Version: &v}, nil)
	result, err := sut.UpsertSchema(ctx, sp, prefix, []*dosa.EntityDefinition{&ed.EntityDefinition})
	assert.NoError(t, err)
	assert.Equal(t, &dosa.SchemaStatus{Version: v}, result)

	mockedClient.EXPECT().UpsertSchema(ctx, gomock.Any()).Return(nil, errors.New("test error"))
	_, err = sut.UpsertSchema(ctx, sp, prefix, []*dosa.EntityDefinition{&ed.EntityDefinition})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "test error")
}

func TestClient_CreateScope(t *testing.T) {
	// build a mock RPC client
	ctrl := gomock.NewController(t)
	mockedClient := dosatest.NewMockClient(ctrl)
	sut := yarpc.Connector{Client: mockedClient}

	mockedClient.EXPECT().CreateScope(ctx, gomock.Any()).Return(nil)
	err := sut.CreateScope(ctx, "scope")
	assert.NoError(t, err)

	mockedClient.EXPECT().CreateScope(ctx, gomock.Any()).Return(errors.New("test error"))
	err = sut.CreateScope(ctx, "scope")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "test error")
}

func TestClient_TruncateScope(t *testing.T) {
	// build a mock RPC client
	ctrl := gomock.NewController(t)
	mockedClient := dosatest.NewMockClient(ctrl)
	sut := yarpc.Connector{Client: mockedClient}

	mockedClient.EXPECT().TruncateScope(ctx, gomock.Any()).Return(nil)
	err := sut.TruncateScope(ctx, "scope")
	assert.NoError(t, err)

	mockedClient.EXPECT().TruncateScope(ctx, gomock.Any()).Return(errors.New("test error"))
	err = sut.TruncateScope(ctx, "scope")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "test error")
}

func TestClient_DropScope(t *testing.T) {
	// build a mock RPC client
	ctrl := gomock.NewController(t)
	mockedClient := dosatest.NewMockClient(ctrl)
	sut := yarpc.Connector{Client: mockedClient}

	mockedClient.EXPECT().DropScope(ctx, gomock.Any()).Return(nil)
	err := sut.DropScope(ctx, "scope")
	assert.NoError(t, err)

	mockedClient.EXPECT().DropScope(ctx, gomock.Any()).Return(errors.New("test error"))
	err = sut.DropScope(ctx, "scope")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "test error")
}

func TestConnector_Range(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockedClient := dosatest.NewMockClient(ctrl)

	testToken := "testToken"
	responseToken := "responseToken"
	testLimit := int32(32)
	// set up the parameters
	op := drpc.OperatorEq
	fieldName := "c1"
	fieldName1 := "c2"
	field := drpc.Field{&fieldName, &drpc.Value{ElemValue: &drpc.RawValue{Int64Value: testInt64Ptr(10)}}}
	field1 := drpc.Field{&fieldName1, &drpc.Value{ElemValue: &drpc.RawValue{Int64Value: testInt64Ptr(10)}}}

	// Prepare the dosa client interface using the mocked RPC layer
	sut := yarpc.Connector{Client: mockedClient}

	// successful call, return results
	mockedClient.EXPECT().Range(ctx, gomock.Any()).Do(func(_ context.Context, request *drpc.RangeRequest) {
		assert.Equal(t, map[string]struct{}{"c1": {}}, request.FieldsToRead)
		assert.Equal(t, testLimit, *request.Limit)
		assert.Equal(t, testRPCSchemaRef, *request.Ref)
		assert.Equal(t, testToken, *request.Token)
		for _, c := range request.Conditions {
			assert.Equal(t, c.Op, &op)
			if *c.Field.Name == fieldName {
				assert.Equal(t, c.Field, &field)
			} else {
				assert.Equal(t, c.Field, &field1)
			}
		}
		assert.Equal(t, len(request.Conditions), 2)
	}).Return(&drpc.RangeResponse{
		Entities: []drpc.FieldValueMap{
			{
				"c1":               {ElemValue: &drpc.RawValue{Int64Value: testInt64Ptr(1)}},
				"fieldNotInSchema": {ElemValue: &drpc.RawValue{Int64Value: testInt64Ptr(5)}},
				"c2":               {ElemValue: &drpc.RawValue{DoubleValue: testFloat64Ptr(2.2)}},
			},
		},
		NextToken: &responseToken,
	}, nil)

	values, token, err := sut.Range(ctx, testEi, map[string][]*dosa.Condition{
		"c1": {&dosa.Condition{
			Value: int64(10),
			Op:    dosa.Eq,
		}},
		"c2": {&dosa.Condition{
			Value: int64(10),
			Op:    dosa.Eq,
		}},
	}, []string{"c1"}, testToken, 32)
	assert.NoError(t, err)
	assert.Equal(t, responseToken, token)
	assert.NotNil(t, values)
	assert.Equal(t, 1, len(values))
	assert.Equal(t, int64(1), values[0]["c1"])
	assert.Equal(t, float64(2.2), values[0]["c2"])

	// perform a not found request
	mockedClient.EXPECT().Range(ctx, gomock.Any()).
		Return(nil, &dosa.ErrNotFound{}).Times(1)
	values, token, err = sut.Range(ctx, testEi, map[string][]*dosa.Condition{"c2": {&dosa.Condition{
		Value: float64(3.3),
		Op:    dosa.Eq,
	}}}, nil, "", 64)
	assert.Nil(t, values)
	assert.Empty(t, token)
	assert.Error(t, err)
	assert.True(t, dosa.ErrorIsNotFound(err))

	// perform a generic error request
	mockedClient.EXPECT().Range(ctx, gomock.Any()).
		Return(nil, errors.New("test error")).Times(1)
	values, token, err = sut.Range(ctx, testEi, map[string][]*dosa.Condition{"c2": {&dosa.Condition{
		Value: float64(3.3),
		Op:    dosa.Eq,
	}}}, nil, "", 64)
	assert.Nil(t, values)
	assert.Empty(t, token)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "test error")
}

func TestConnector_Scan(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockedClient := dosatest.NewMockClient(ctrl)

	testToken := "testToken"
	responseToken := "responseToken"
	testLimit := int32(32)
	// set up the parameters
	sr := &drpc.ScanRequest{
		Ref:          &testRPCSchemaRef,
		Token:        &testToken,
		Limit:        &testLimit,
		FieldsToRead: map[string]struct{}{"c1": {}},
	}
	// successful call, return results
	mockedClient.EXPECT().Scan(ctx, sr).
		Do(func(_ context.Context, r *drpc.ScanRequest) {
			assert.Equal(t, sr, r)
		}).
		Return(&drpc.ScanResponse{
			Entities: []drpc.FieldValueMap{
				{
					"c1":               {ElemValue: &drpc.RawValue{Int64Value: testInt64Ptr(1)}},
					"fieldNotInSchema": {ElemValue: &drpc.RawValue{Int64Value: testInt64Ptr(5)}},
					"c2":               {ElemValue: &drpc.RawValue{DoubleValue: testFloat64Ptr(2.2)}},
				},
			},
			NextToken: &responseToken,
		}, nil)
	// failed call, return error
	mockedClient.EXPECT().Scan(ctx, gomock.Any()).
		Return(nil, errors.New("test error")).Times(1)
	// no results, make sure error is exact
	mockedClient.EXPECT().Scan(ctx, gomock.Any()).
		Return(nil, &dosa.ErrNotFound{})

	// Prepare the dosa client interface using the mocked RPC layer
	sut := yarpc.Connector{Client: mockedClient}

	// perform the successful request
	values, token, err := sut.Scan(ctx, testEi, []string{"c1"}, testToken, 32)
	assert.NoError(t, err)
	assert.Equal(t, responseToken, token)
	assert.NotNil(t, values)
	assert.Equal(t, 1, len(values))
	assert.Equal(t, int64(1), values[0]["c1"])
	assert.Equal(t, float64(2.2), values[0]["c2"])

	// perform a not found request
	values, token, err = sut.Scan(ctx, testEi, nil, "", 64)
	assert.Nil(t, values)
	assert.Empty(t, token)
	assert.Error(t, err)
	assert.True(t, dosa.ErrorIsNotFound(err))

	// perform a generic error request
	values, token, err = sut.Scan(ctx, testEi, nil, "", 64)
	assert.Nil(t, values)
	assert.Empty(t, token)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "test error")
}

func TestConnector_Remove(t *testing.T) {
	// build a mock RPC client
	ctrl := gomock.NewController(t)
	mockedClient := dosatest.NewMockClient(ctrl)

	// set up the parameters
	removeRequest := &drpc.RemoveRequest{
		Ref:       &testRPCSchemaRef,
		KeyValues: map[string]*drpc.Value{"f1": {ElemValue: &drpc.RawValue{Int64Value: testInt64Ptr(5)}}},
	}

	// we expect a single call to Read, and we return back two fields, f1 which is in the typemap and another field that is not
	mockedClient.EXPECT().Remove(ctx, removeRequest).Return(nil)

	// Prepare the dosa client interface using the mocked RPC layer
	sut := yarpc.Connector{Client: mockedClient}

	// perform the read
	err := sut.Remove(ctx, testEi, map[string]dosa.FieldValue{"f1": dosa.FieldValue(int64(5))})
	assert.Nil(t, err) // not an error

	// make sure we actually called Read on the interface
	ctrl.Finish()
}

// TestPanic is an unimplemented method test for coverage, remove these as they are implemented
func TestPanic(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockedClient := dosatest.NewMockClient(ctrl)

	sut := yarpc.Connector{Client: mockedClient}

	assert.Panics(t, func() {
		sut.MultiUpsert(ctx, testEi, nil)
	})

	assert.Panics(t, func() {
		sut.MultiRemove(ctx, testEi, nil)
	})

	assert.Panics(t, func() {
		sut.Search(ctx, testEi, dosa.FieldNameValuePair{}, nil, "", 0)
	})

	assert.Panics(t, func() {
		sut.ScopeExists(ctx, "")
	})
}
