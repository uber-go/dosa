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

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/yarpc"
	drpc "github.com/uber/dosa-idl/.gen/dosa"
	"github.com/uber/dosa-idl/.gen/dosa/dosatest"
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

// Test a happy path read of one column and specify the primary key
func TestYaRPCClient_Read(t *testing.T) {
	// build a mock RPC client
	ctrl := gomock.NewController(t)
	mockedClient := dosatest.NewMockClient(ctrl)

	// set up the parameters
	ctx := context.TODO()
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

	// make sure we actually called Read on the interface
	ctrl.Finish()
}

func TestYaRPCClient_MultiRead(t *testing.T) {
	// build a mock RPC client
	ctrl := gomock.NewController(t)
	mockedClient := dosatest.NewMockClient(ctrl)

	// set up the parameters
	ctx := context.TODO()
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
	ctx := context.TODO()

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

	// make sure we actually called CreateIfNotExists on the interface
	ctrl.Finish()

	assert.NoError(t, sut.Shutdown())
}

func TestYaRPCClient_Upsert(t *testing.T) {
	// build a mock RPC client
	ctrl := gomock.NewController(t)
	mockedClient := dosatest.NewMockClient(ctrl)
	ctx := context.TODO()

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
	ctx := context.TODO()

	mockedClient.EXPECT().CheckSchema(ctx, gomock.Any()).Return(&drpc.CheckSchemaResponse{[]int32{}}, nil)

	sut := yarpc.Connector{Client: mockedClient}

	ed, err := dosa.TableFromInstance(&TestDosaObject{})
	assert.NoError(t, err)

	sr, err := sut.CheckSchema(ctx, "scope", "prefix", []*dosa.EntityDefinition{&ed.EntityDefinition})
	assert.NoError(t, err)
	assert.NotNil(t, sr)
}

func TestClient_UpsertSchema(t *testing.T) {
	// build a mock RPC client
	ctrl := gomock.NewController(t)
	mockedClient := dosatest.NewMockClient(ctrl)
	sut := yarpc.Connector{Client: mockedClient}

	ctx := context.Background()

	ed, err := dosa.TableFromInstance(&TestDosaObject{})
	assert.NoError(t, err)
	mockedClient.EXPECT().UpsertSchema(ctx, gomock.Any()).Return(&drpc.UpsertSchemaResponse{Versions: []int32{1, 2, 3}}, nil)
	_, err = sut.UpsertSchema(ctx, "scope", "prefix", []*dosa.EntityDefinition{&ed.EntityDefinition})
	assert.NoError(t, err)

	mockedClient.EXPECT().UpsertSchema(ctx, gomock.Any()).Return(nil, errors.New("test error"))
	_, err = sut.UpsertSchema(ctx, "scope", "prefix", []*dosa.EntityDefinition{&ed.EntityDefinition})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "test error")
}

// TestPanic is an unimplemented method test for coverage, remove these as they are implemented
func TestPanic(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockedClient := dosatest.NewMockClient(ctrl)
	ctx := context.TODO()

	sut := yarpc.Connector{Client: mockedClient}

	assert.Panics(t, func() {
		sut.MultiUpsert(ctx, testEi, nil)
	})

	assert.Panics(t, func() {
		sut.MultiRemove(ctx, testEi, nil)
	})

	assert.Panics(t, func() {
		sut.CreateScope(ctx, "")
	})

	assert.Panics(t, func() {
		sut.DropScope(ctx, "")
	})

	assert.Panics(t, func() {
		sut.Remove(ctx, testEi, nil)
	})

	assert.Panics(t, func() {
		sut.TruncateScope(ctx, "")
	})

	assert.Panics(t, func() {
		sut.Range(ctx, testEi, nil, nil, "", 0)
	})

	assert.Panics(t, func() {
		sut.Search(ctx, testEi, dosa.FieldNameValuePair{}, nil, "", 0)
	})

	assert.Panics(t, func() {
		sut.Scan(ctx, testEi, nil, "", 0)
	})

	assert.Panics(t, func() {
		sut.ScopeExists(ctx, "")
	})
}
