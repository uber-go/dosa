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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/testutil"
	drpc "github.com/uber/dosa-idl/.gen/dosa"
	"github.com/uber/dosa-idl/.gen/dosa/dosatest"
	yarpc2 "go.uber.org/yarpc"
)

var (
	nt = dosa.NoTTL().Nanoseconds()

	testEi = newTestEi()

	testSchemaRef = dosa.SchemaRef{
		Scope:      "scope1",
		NamePrefix: "namePrefix",
		EntityName: "eName",
		Version:    12345,
	}

	testRPCSchemaRef = drpc.SchemaRef{
		Scope:      testutil.TestStringPtr("scope1"),
		NamePrefix: testutil.TestStringPtr("namePrefix"),
		EntityName: testutil.TestStringPtr("eName"),
		Version:    testutil.TestInt32Ptr(12345),
	}

	ctx = context.Background()
)

func newTestEi() *dosa.EntityInfo {
	return &dosa.EntityInfo{
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
				{Name: "c7", Type: dosa.TUUID},
			},
			Key: &dosa.PrimaryKey{
				PartitionKeys: []string{"f1"},
			},
			Name: "t1",
		},
	}
}

func testAssert(t *testing.T) testutil.TestAssertFn {
	return func(a, b interface{}) {
		assert.Equal(t, a, b)
	}
}

// Test a happy path read of one column and specify the primary key
func TestYARPCClient_Read(t *testing.T) {
	// build a mock RPC client
	ctrl := gomock.NewController(t)
	mockedClient := dosatest.NewMockClient(ctrl)

	// set up the parameters
	readRequest := &drpc.ReadRequest{
		Ref:          &testRPCSchemaRef,
		KeyValues:    map[string]*drpc.Value{"f1": {ElemValue: &drpc.RawValue{Int64Value: testutil.TestInt64Ptr(5)}}},
		FieldsToRead: map[string]struct{}{"f1": {}},
	}

	// we expect a single call to Read, and we return back two fields, f1 which is in the typemap and another field that is not
	mockedClient.EXPECT().Read(ctx, readRequest, gomock.Any()).Return(&drpc.ReadResponse{drpc.FieldValueMap{
		"c1":               {ElemValue: &drpc.RawValue{Int64Value: testutil.TestInt64Ptr(1)}},
		"fieldNotInSchema": {ElemValue: &drpc.RawValue{Int64Value: testutil.TestInt64Ptr(5)}},
		"c2":               {ElemValue: &drpc.RawValue{DoubleValue: testutil.TestFloat64Ptr(2.2)}},
		"c3":               {ElemValue: &drpc.RawValue{StringValue: testutil.TestStringPtr("f3value")}},
		"c4":               {ElemValue: &drpc.RawValue{BinaryValue: []byte{'b', 'i', 'n', 'a', 'r', 'y'}}},
		"c5":               {ElemValue: &drpc.RawValue{BoolValue: testutil.TestBoolPtr(false)}},
		"c6":               {ElemValue: &drpc.RawValue{Int32Value: testutil.TestInt32Ptr(1)}},
	}}, nil)

	// Prepare the dosa client interface using the mocked RPC layer
	sut := Connector{client: mockedClient}

	// perform the read
	values, err := sut.Read(ctx, testEi, map[string]dosa.FieldValue{"f1": dosa.FieldValue(int64(5))}, []string{"f1"})
	assert.Nil(t, err)                                                 // not an error
	assert.NotNil(t, values)                                           // found some values
	testutil.AssertEqForPointer(testAssert(t), int64(1), values["c1"]) // the mapped field is found, and is the right type
	testutil.AssertEqForPointer(testAssert(t), float64(2.2), values["c2"])
	testutil.AssertEqForPointer(testAssert(t), "f3value", values["c3"])
	assert.Equal(t, []byte{'b', 'i', 'n', 'a', 'r', 'y'}, values["c4"])
	testutil.AssertEqForPointer(testAssert(t), false, values["c5"])
	testutil.AssertEqForPointer(testAssert(t), int32(1), values["c6"])
	assert.Empty(t, values["fieldNotInSchema"]) // the unknown field is not present

	errCode := int32(404)
	mockedClient.EXPECT().Read(ctx, readRequest, gomock.Any()).Return(nil, &drpc.BadRequestError{ErrorCode: &errCode})
	_, err = sut.Read(ctx, testEi, map[string]dosa.FieldValue{"f1": dosa.FieldValue(int64(5))}, []string{"f1"})
	assert.True(t, dosa.ErrorIsNotFound(err))

	// make sure we actually called Read on the interface
	ctrl.Finish()
}

func TestYARPCClient_MultiRead(t *testing.T) {
	// build a mock RPC client
	ctrl := gomock.NewController(t)
	mockedClient := dosatest.NewMockClient(ctrl)

	// Prepare the dosa client interface using the mocked RPC layer
	sut := Connector{client: mockedClient}

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
						"f1": &drpc.Value{ElemValue: &drpc.RawValue{Int64Value: testutil.TestInt64Ptr(5)}},
					},
					{
						"f2": &drpc.Value{ElemValue: &drpc.RawValue{Int64Value: testutil.TestInt64Ptr(6)}},
					},
				},
				FieldsToRead: map[string]struct{}{"f1": {}},
			},
			Response: &drpc.MultiReadResponse{
				Results: []*drpc.EntityOrError{
					{
						EntityValues: drpc.FieldValueMap{
							"c1":               {ElemValue: &drpc.RawValue{Int64Value: testutil.TestInt64Ptr(1)}},
							"fieldNotInSchema": {ElemValue: &drpc.RawValue{Int64Value: testutil.TestInt64Ptr(5)}},
							"c2":               {ElemValue: &drpc.RawValue{DoubleValue: testutil.TestFloat64Ptr(2.2)}},
							"c3":               {ElemValue: &drpc.RawValue{StringValue: testutil.TestStringPtr("f3value")}},
							"c4":               {ElemValue: &drpc.RawValue{BinaryValue: []byte{'b', 'i', 'n', 'a', 'r', 'y'}}},
							"c5":               {ElemValue: &drpc.RawValue{BoolValue: testutil.TestBoolPtr(false)}},
							"c6":               {ElemValue: &drpc.RawValue{Int32Value: testutil.TestInt32Ptr(1)}},
						},
						Error: nil,
					},
					{
						EntityValues: drpc.FieldValueMap{
							"c1":               {ElemValue: &drpc.RawValue{Int64Value: testutil.TestInt64Ptr(2)}},
							"fieldNotInSchema": {ElemValue: &drpc.RawValue{Int64Value: testutil.TestInt64Ptr(15)}},
							"c2":               {ElemValue: &drpc.RawValue{DoubleValue: testutil.TestFloat64Ptr(12.2)}},
							"c3":               {ElemValue: &drpc.RawValue{StringValue: testutil.TestStringPtr("f3value1")}},
							"c4":               {ElemValue: &drpc.RawValue{BinaryValue: []byte{'a', 'i', '1', 'a', 'r', 'y'}}},
							"c5":               {ElemValue: &drpc.RawValue{BoolValue: testutil.TestBoolPtr(true)}},
							"c6":               {ElemValue: &drpc.RawValue{Int32Value: testutil.TestInt32Ptr(2)}},
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
						"f1": &drpc.Value{ElemValue: &drpc.RawValue{Int64Value: testutil.TestInt64Ptr(5)}},
					},
					{
						"f2": &drpc.Value{ElemValue: &drpc.RawValue{Int64Value: testutil.TestInt64Ptr(6)}},
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
						"f1": &drpc.Value{ElemValue: &drpc.RawValue{Int64Value: testutil.TestInt64Ptr(5)}},
					},
					{
						"f2": &drpc.Value{ElemValue: &drpc.RawValue{Int64Value: testutil.TestInt64Ptr(6)}},
					},
				},
				FieldsToRead: map[string]struct{}{"f1": {}},
			},
			Response: &drpc.MultiReadResponse{
				Results: []*drpc.EntityOrError{
					{
						EntityValues: drpc.FieldValueMap{
							"c1":               {ElemValue: &drpc.RawValue{Int64Value: testutil.TestInt64Ptr(1)}},
							"fieldNotInSchema": {ElemValue: &drpc.RawValue{Int64Value: testutil.TestInt64Ptr(5)}},
							"c2":               {ElemValue: &drpc.RawValue{DoubleValue: testutil.TestFloat64Ptr(2.2)}},
							"c3":               {ElemValue: &drpc.RawValue{StringValue: testutil.TestStringPtr("f3value")}},
							"c4":               {ElemValue: &drpc.RawValue{BinaryValue: []byte{'b', 'i', 'n', 'a', 'r', 'y'}}},
							"c5":               {ElemValue: &drpc.RawValue{BoolValue: testutil.TestBoolPtr(false)}},
							"c6":               {ElemValue: &drpc.RawValue{Int32Value: testutil.TestInt32Ptr(1)}},
						},
						Error: nil,
					},
					{
						Error: &drpc.Error{Msg: testutil.TestStringPtr("error fetching entity")},
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
						"f1": &drpc.Value{ElemValue: &drpc.RawValue{Int64Value: testutil.TestInt64Ptr(5)}},
					},
					{
						"f2": &drpc.Value{ElemValue: &drpc.RawValue{Int64Value: testutil.TestInt64Ptr(6)}},
					},
				},
				FieldsToRead: map[string]struct{}{"f1": {}},
			},
			Response: &drpc.MultiReadResponse{
				Results: []*drpc.EntityOrError{
					{
						EntityValues: drpc.FieldValueMap{
							"c1":               {ElemValue: &drpc.RawValue{Int64Value: testutil.TestInt64Ptr(1)}},
							"fieldNotInSchema": {ElemValue: &drpc.RawValue{Int64Value: testutil.TestInt64Ptr(5)}},
							"c2":               {ElemValue: &drpc.RawValue{DoubleValue: testutil.TestFloat64Ptr(2.2)}},
							"c3":               {ElemValue: &drpc.RawValue{StringValue: testutil.TestStringPtr("f3value")}},
							"c4":               {ElemValue: &drpc.RawValue{BinaryValue: []byte{'b', 'i', 'n', 'a', 'r', 'y'}}},
							"c5":               {ElemValue: &drpc.RawValue{BoolValue: testutil.TestBoolPtr(false)}},
							"c6":               {ElemValue: &drpc.RawValue{Int32Value: testutil.TestInt32Ptr(1)}},
						},
						Error: nil,
					},
					{
						Error: &drpc.Error{
							Msg:         testutil.TestStringPtr("entity not found"),
							ShouldRetry: testutil.TestBoolPtr(false),
							ErrCode:     testutil.TestInt32Ptr(404),
						},
					},
				},
			},
			ResponseErr: nil,
		},
	}

	for _, d := range data {
		mockedClient.EXPECT().MultiRead(ctx, d.Request, gomock.Any()).Return(d.Response, d.ResponseErr)
		// perform the multi read
		values, err := sut.MultiRead(ctx, testEi, []map[string]dosa.FieldValue{{"f1": dosa.FieldValue(int64(5))}, {"f2": dosa.FieldValue(int64(6))}}, []string{"f1"})
		if d.ResponseErr == nil {
			assert.Nil(t, err)       // not an error
			assert.NotNil(t, values) // found some values
			for i, v := range values {
				if v.Error != nil {
					if d.Response.Results[i].Error.ErrCode != nil &&
						*d.Response.Results[i].Error.ErrCode == 404 {
						assert.True(t, dosa.ErrorIsNotFound(v.Error))
					} else {
						assert.Contains(t, v.Error.Error(), *d.Response.Results[i].Error.Msg)
					}
					continue
				}
				testutil.AssertEqForPointer(testAssert(t), *d.Response.Results[i].EntityValues["c1"].ElemValue.Int64Value, v.Values["c1"])
				assert.Empty(t, v.Values["fieldNotInSchema"])
				testutil.AssertEqForPointer(testAssert(t), *d.Response.Results[i].EntityValues["c2"].ElemValue.DoubleValue, v.Values["c2"])
				testutil.AssertEqForPointer(testAssert(t), *d.Response.Results[i].EntityValues["c3"].ElemValue.StringValue, v.Values["c3"])
				assert.Equal(t, d.Response.Results[i].EntityValues["c4"].ElemValue.BinaryValue, v.Values["c4"])
				testutil.AssertEqForPointer(testAssert(t), *d.Response.Results[i].EntityValues["c5"].ElemValue.BoolValue, v.Values["c5"])
				testutil.AssertEqForPointer(testAssert(t), *d.Response.Results[i].EntityValues["c6"].ElemValue.Int32Value, v.Values["c6"])
			}
			continue
		}

		assert.Error(t, err)
		assert.Contains(t, err.Error(), d.ResponseErr.Error())
	}

	// make sure we actually called Read on the interface
	ctrl.Finish()
}

func TestYARPCClient_CreateIfNotExists(t *testing.T) {
	// build a mock RPC client
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	valss := getStubbedUpsertRequests()

	for _, vals := range valss {
		mockedClient := dosatest.NewMockClient(ctrl)
		// build up the input field list and the output field list
		// the layout is quite different; inputs are a simple map but the actual RPC call expects a messier format
		inFields := map[string]dosa.FieldValue{}
		outFields := drpc.FieldValueMap{}
		for _, item := range vals {
			inFields[item.Name] = item.Value
			rv, _ := RawValueFromInterface(item.Value)
			outFields[item.Name] = &drpc.Value{ElemValue: rv}
		}

		mockedClient.EXPECT().CreateIfNotExists(ctx, &drpc.CreateRequest{Ref: &testRPCSchemaRef, EntityValues: outFields, TTL: &nt}, gomock.Any())

		// create the YARPCClient and give it the mocked RPC interface
		// see https://en.wiktionary.org/wiki/SUT for the reason this is called sut
		sut := Connector{client: mockedClient}

		// and run the test
		err := sut.CreateIfNotExists(ctx, testEi, inFields)
		assert.Nil(t, err)

		errCode := int32(409)
		mockedClient.EXPECT().CreateIfNotExists(ctx, &drpc.CreateRequest{Ref: &testRPCSchemaRef, EntityValues: outFields, TTL: &nt}, gomock.Any()).Return(
			&drpc.BadRequestError{ErrorCode: &errCode},
		)

		err = sut.CreateIfNotExists(ctx, testEi, inFields)
		assert.True(t, dosa.ErrorIsAlreadyExists(err))

		// cover the conversion error case
		err = sut.CreateIfNotExists(ctx, testEi, map[string]dosa.FieldValue{"c7": dosa.UUID("")})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "\"c7\"")               // must contain name of bad field
		assert.Contains(t, err.Error(), ErrInCorrectUUIDLength) // must mention that the uuid is too short
	}
}

func TestYARPCClient_CreateIfNotExistsWithTTL(t *testing.T) {
	// build a mock RPC client
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
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

	ei := newTestEi()

	ttls := []int64{int64(-1), int64(0), 435}
	for _, ttl := range ttls {
		// build up the input field list and the output field list
		// the layout is quite different; inputs are a simple map but the actual RPC call expects a messier format
		tl := time.Duration(ttl)
		ei.TTL = &tl
		inFields := map[string]dosa.FieldValue{}
		outFields := drpc.FieldValueMap{}
		for _, item := range vals {
			inFields[item.Name] = item.Value
			rv, _ := RawValueFromInterface(item.Value)
			outFields[item.Name] = &drpc.Value{ElemValue: rv}
		}

		mockedClient.EXPECT().CreateIfNotExists(ctx, &drpc.CreateRequest{Ref: &testRPCSchemaRef, EntityValues: outFields, TTL: &ttl}, gomock.Any())

		// create the YARPCClient and give it the mocked RPC interface
		// see https://en.wiktionary.org/wiki/SUT for the reason this is called sut
		sut := Connector{client: mockedClient}

		// and run the test
		err := sut.CreateIfNotExists(ctx, ei, inFields)
		assert.Nil(t, err)
	}
}

func TestYARPCClient_Upsert(t *testing.T) {
	// build a mock RPC client
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockedClient := dosatest.NewMockClient(ctrl)

	valss := getStubbedUpsertRequests()

	for _, vals := range valss {
		// build up the input field list and the output field list
		// the layout is quite different; inputs are a simple map but the actual RPC call expects a messier format
		inFields := map[string]dosa.FieldValue{}
		outFields := map[string]*drpc.Value{}
		for _, item := range vals {
			inFields[item.Name] = item.Value
			rv, _ := RawValueFromInterface(item.Value)
			outFields[item.Name] = &drpc.Value{ElemValue: rv}
		}

		mockedClient.EXPECT().Upsert(ctx, &drpc.UpsertRequest{
			Ref:          &testRPCSchemaRef,
			EntityValues: outFields,
			TTL:          &nt,
		}, gomock.Any())

		// create the YARPCClient and give it the mocked RPC interface
		// see https://en.wiktionary.org/wiki/SUT for the reason this is called sut
		sut := Connector{client: mockedClient}

		// and run the test, first with a nil FieldsToUpdate, then with a specific list
		err := sut.Upsert(ctx, testEi, inFields)
		assert.Nil(t, err)

		// cover the conversion error case
		err = sut.Upsert(ctx, testEi, map[string]dosa.FieldValue{"c7": dosa.UUID("")})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "\"c7\"")                // must contain name of bad field
		assert.Contains(t, err.Error(), "incorrect UUID length") // must mention that the uuid is too short
	}
}

func TestYARPCClient_UpsertWithTTL(t *testing.T) {
	// build a mock RPC client
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockedClient := dosatest.NewMockClient(ctrl)

	// here are the data types to test; the names are random
	vals := []struct {
		Name  string
		Value interface{}
	}{
		{"c1", testutil.TestInt64Ptr(1)},
		{"c2", testutil.TestFloat64Ptr(2.2)},
		{"c3", testutil.TestStringPtr("string")},
		{"c4", []byte{'b', 'i', 'n', 'a', 'r', 'y'}},
		{"c5", testutil.TestBoolPtr(false)},
		{"c6", testutil.TestInt32Ptr(2)},
		{"c7", testutil.TestTimePtr(time.Now())},
	}

	ei := newTestEi()

	ttls := []int64{int64(-1), int64(0), 435}
	for _, ttl := range ttls {
		// build up the input field list and the output field list
		// the layout is quite different; inputs are a simple map but the actual RPC call expects a messier format
		inFields := map[string]dosa.FieldValue{}
		outFields := map[string]*drpc.Value{}
		for _, item := range vals {
			inFields[item.Name] = item.Value
			rv, _ := RawValueFromInterface(item.Value)
			outFields[item.Name] = &drpc.Value{ElemValue: rv}
		}

		tl := time.Duration(ttl)
		ei.TTL = &tl
		mockedClient.EXPECT().Upsert(ctx, &drpc.UpsertRequest{
			Ref:          &testRPCSchemaRef,
			EntityValues: outFields,
			TTL:          &ttl,
		}, gomock.Any())

		// create the YARPCClient and give it the mocked RPC interface
		// see https://en.wiktionary.org/wiki/SUT for the reason this is called sut
		sut := Connector{client: mockedClient}

		// and run the test, first with a nil FieldsToUpdate, then with a specific list
		err := sut.Upsert(ctx, ei, inFields)
		assert.Nil(t, err)
	}
}

func TestYARPCClient_MultiUpsert(t *testing.T) {
	// build a mock RPC client
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockedClient := dosatest.NewMockClient(ctrl)

	testErrMsg := "response error"

	ttlVal := int64(1)

	testCases := []struct {
		NetworkError  error
		ResponseError *drpc.Error
		UpsertRequest []struct {
			Name  string
			Value interface{}
		}
		TTL *int64
	}{
		{
			nil,
			nil,
			getStubbedUpsertRequests()[0],
			&ttlVal,
		},
		{
			nil,
			nil,
			getStubbedUpsertRequests()[1],
			nil,
		},
		{
			errors.New("an error"),
			nil,
			getStubbedUpsertRequests()[0],
			nil,
		},
		{
			nil,
			&drpc.Error{
				Msg: &testErrMsg,
			},
			getStubbedUpsertRequests()[0],
			nil,
		},
	}

	for _, testCase := range testCases {
		// build up the input field list and the output field list
		// the layout is quite different; inputs are a simple map but the actual RPC call expects a messier format
		inFields := map[string]dosa.FieldValue{}
		outFields := map[string]*drpc.Value{}
		for _, item := range testCase.UpsertRequest {
			inFields[item.Name] = item.Value
			rv, _ := RawValueFromInterface(item.Value)
			outFields[item.Name] = &drpc.Value{ElemValue: rv}
		}

		ei := newTestEi()

		expectedTTL := testCase.TTL
		if expectedTTL == nil {
			noTTL := dosa.NoTTL().Nanoseconds()
			expectedTTL = &noTTL
		}

		durationTTL := time.Duration(*expectedTTL)
		ei.TTL = &durationTTL

		mockedClient.EXPECT().MultiUpsert(ctx, &drpc.MultiUpsertRequest{
			Ref:      &testRPCSchemaRef,
			Entities: []drpc.FieldValueMap{outFields},
			// TTL:      expectedTTL,
		}, gomock.Any()).Return(&drpc.MultiUpsertResponse{Errors: []*drpc.Error{testCase.ResponseError}}, testCase.NetworkError).Times(1)

		// create the YARPCClient and give it the mocked RPC interface
		// see https://en.wiktionary.org/wiki/SUT for the reason this is called sut
		sut := Connector{client: mockedClient}

		retErrors, err := sut.MultiUpsert(ctx, ei, []map[string]dosa.FieldValue{inFields})
		if testCase.NetworkError != nil {
			assert.Error(t, err)
		} else if testCase.ResponseError != nil {
			assert.NotNil(t, retErrors[0])
		} else {
			assert.Nil(t, err)
			assert.Len(t, retErrors, 1)
		}
	}
}

func TestYARPCClient_MultiUpsertInvalidDataType(t *testing.T) {
	sut := Connector{}

	_, err := sut.MultiUpsert(ctx, testEi, []map[string]dosa.FieldValue{{"c7": dosa.UUID("")}})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "\"c7\"")                // must contain name of bad field
	assert.Contains(t, err.Error(), "incorrect UUID length") // must mention that the uuid is too short
}

func TestYARPCClient_MultiRemove(t *testing.T) {
	// build a mock RPC client
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockedClient := dosatest.NewMockClient(ctrl)

	testErrMsg := "response error"

	testCases := []struct {
		NetworkError  error
		ResponseError *drpc.Error
		RemoveRequest map[string]dosa.FieldValue
	}{
		{
			nil,
			nil,
			getStubbedRemoveRequest(),
		},
		{
			errors.New("an error"),
			nil,
			getStubbedRemoveRequest(),
		},
		{
			nil,
			&drpc.Error{
				Msg: &testErrMsg,
			},
			getStubbedRemoveRequest(),
		},
	}

	for _, testCase := range testCases {
		mockedClient.EXPECT().MultiRemove(ctx, &drpc.MultiRemoveRequest{
			Ref:       &testRPCSchemaRef,
			KeyValues: []drpc.FieldValueMap{getStubbedRemoveDOSARequest()},
		}, gomock.Any()).Return(&drpc.MultiRemoveResponse{Errors: []*drpc.Error{testCase.ResponseError}}, testCase.NetworkError).Times(1)

		// create the YARPCClient and give it the mocked RPC interface
		// see https://en.wiktionary.org/wiki/SUT for the reason this is called sut
		sut := Connector{client: mockedClient}

		retErrors, err := sut.MultiRemove(ctx, testEi, []map[string]dosa.FieldValue{testCase.RemoveRequest})
		if testCase.NetworkError != nil {
			assert.Error(t, err)
		} else if testCase.ResponseError != nil {
			assert.NotNil(t, retErrors[0])
		} else {
			assert.Nil(t, err)
			assert.Len(t, retErrors, 1)
		}
	}
}

func TestYARPCClient_MultiRemoveInvalidDataType(t *testing.T) {
	sut := Connector{}

	_, err := sut.MultiRemove(ctx, testEi, []map[string]dosa.FieldValue{{"c7": dosa.UUID("")}})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "\"c7\"")                // must contain name of bad field
	assert.Contains(t, err.Error(), "incorrect UUID length") // must mention that the uuid is too short
}

type TestDosaObject struct {
	dosa.Entity `dosa:"primaryKey=(F1, F2), etl=on"`
	F1          int64
	F2          int32
}

func TestClient_CheckSchema(t *testing.T) {
	// build a mock RPC client
	ctrl := gomock.NewController(t)
	mockedClient := dosatest.NewMockClient(ctrl)
	sp := "scope"
	prefix := "prefix"

	sut := Connector{client: mockedClient}

	ed, err := dosa.TableFromInstance(&TestDosaObject{})
	assert.NoError(t, err)
	assert.Equal(t, ed.ETL, dosa.EtlOn)
	expectedRequest := &drpc.CheckSchemaRequest{
		Scope:      &sp,
		NamePrefix: &prefix,
		EntityDefs: EntityDefsToThrift([]*dosa.EntityDefinition{&ed.EntityDefinition}),
	}
	assert.Equal(t, ETLStateToThrift(ed.ETL), *expectedRequest.EntityDefs[0].Etl)
	expectedRequest2 := &drpc.CanUpsertSchemaRequest{
		Scope:      &sp,
		NamePrefix: &prefix,
		EntityDefs: EntityDefsToThrift([]*dosa.EntityDefinition{&ed.EntityDefinition}),
	}
	v := int32(1)

	mockedClient.EXPECT().CheckSchema(ctx, gomock.Any(), gomock.Any()).Do(func(_ context.Context, request *drpc.CheckSchemaRequest, opts yarpc2.CallOption) {
		assert.Equal(t, expectedRequest, request)
		assert.NotNil(t, request.EntityDefs[0].Etl)
	}).Return(&drpc.CheckSchemaResponse{Version: &v}, nil)
	sr, err := sut.CheckSchema(ctx, sp, prefix, []*dosa.EntityDefinition{&ed.EntityDefinition})
	assert.NoError(t, err)
	assert.Equal(t, v, sr)

	mockedClient.EXPECT().CanUpsertSchema(ctx, gomock.Any(), gomock.Any()).Do(func(_ context.Context, request *drpc.CanUpsertSchemaRequest, opts yarpc2.CallOption) {
		assert.Equal(t, expectedRequest2, request)
	}).Return(&drpc.CanUpsertSchemaResponse{Version: &v}, nil)
	sr, err = sut.CanUpsertSchema(ctx, sp, prefix, []*dosa.EntityDefinition{&ed.EntityDefinition})
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
	sut := Connector{client: mockedClient}

	expectedRequest := &drpc.CheckSchemaStatusRequest{
		Scope:      &sp,
		NamePrefix: &prefix,
		Version:    &version,
	}

	mockedClient.EXPECT().CheckSchemaStatus(ctx, gomock.Any(), gomock.Any()).Do(func(_ context.Context, request *drpc.CheckSchemaStatusRequest, opts yarpc2.CallOption) {
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
	sut := Connector{client: mockedClient}

	ed, err := dosa.TableFromInstance(&TestDosaObject{})
	assert.NoError(t, err)
	sp := "scope"
	prefix := "prefix"

	expectedRequest := &drpc.UpsertSchemaRequest{
		Scope:      &sp,
		NamePrefix: &prefix,
		EntityDefs: EntityDefsToThrift([]*dosa.EntityDefinition{&ed.EntityDefinition}),
	}
	v := int32(1)
	mockedClient.EXPECT().UpsertSchema(ctx, gomock.Any(), gomock.Any()).Do(func(_ context.Context, request *drpc.UpsertSchemaRequest, option yarpc2.CallOption) {
		assert.Equal(t, expectedRequest, request)
	}).Return(&drpc.UpsertSchemaResponse{Version: &v}, nil)
	result, err := sut.UpsertSchema(ctx, sp, prefix, []*dosa.EntityDefinition{&ed.EntityDefinition})
	assert.NoError(t, err)
	assert.Equal(t, &dosa.SchemaStatus{Version: v}, result)

	mockedClient.EXPECT().UpsertSchema(ctx, gomock.Any(), gomock.Any()).Return(nil, errors.New("test error"))
	_, err = sut.UpsertSchema(ctx, sp, prefix, []*dosa.EntityDefinition{&ed.EntityDefinition})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "test error")
}

func TestClient_CreateScope(t *testing.T) {
	// build a mock RPC client
	ctrl := gomock.NewController(t)
	mockedClient := dosatest.NewMockClient(ctrl)

	sut := Connector{client: mockedClient}
	mockedClient.EXPECT().CreateScope(ctx, gomock.Any(), gomock.Any()).Return(nil)
	err := sut.CreateScope(ctx, &dosa.ScopeMetadata{Name: "scope"})
	assert.NoError(t, err)

	mockedClient.EXPECT().CreateScope(ctx, gomock.Any(), gomock.Any()).Return(errors.New("test error"))
	err = sut.CreateScope(ctx, &dosa.ScopeMetadata{Name: "scope"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "test error")
}

func TestClient_TruncateScope(t *testing.T) {
	// build a mock RPC client
	ctrl := gomock.NewController(t)
	mockedClient := dosatest.NewMockClient(ctrl)
	sut := Connector{client: mockedClient}

	mockedClient.EXPECT().TruncateScope(ctx, gomock.Any(), gomock.Any()).Return(nil)
	err := sut.TruncateScope(ctx, "scope")
	assert.NoError(t, err)

	mockedClient.EXPECT().TruncateScope(ctx, gomock.Any(), gomock.Any()).Return(errors.New("test error"))
	err = sut.TruncateScope(ctx, "scope")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "test error")
}

func TestClient_DropScope(t *testing.T) {
	// build a mock RPC client
	ctrl := gomock.NewController(t)
	mockedClient := dosatest.NewMockClient(ctrl)
	sut := Connector{client: mockedClient}

	mockedClient.EXPECT().DropScope(ctx, gomock.Any(), gomock.Any()).Return(nil)
	err := sut.DropScope(ctx, "scope")
	assert.NoError(t, err)

	mockedClient.EXPECT().DropScope(ctx, gomock.Any(), gomock.Any()).Return(errors.New("test error"))
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
	field := drpc.Field{&fieldName, &drpc.Value{ElemValue: &drpc.RawValue{Int64Value: testutil.TestInt64Ptr(10)}}}
	field1 := drpc.Field{&fieldName1, &drpc.Value{ElemValue: &drpc.RawValue{Int64Value: testutil.TestInt64Ptr(10)}}}

	// Prepare the dosa client interface using the mocked RPC layer
	sut := Connector{client: mockedClient}

	// successful call, return results
	mockedClient.EXPECT().Range(ctx, gomock.Any(), gomock.Any()).Do(func(_ context.Context, request *drpc.RangeRequest, opts yarpc2.CallOption) {
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
				"c1":               {ElemValue: &drpc.RawValue{Int64Value: testutil.TestInt64Ptr(1)}},
				"fieldNotInSchema": {ElemValue: &drpc.RawValue{Int64Value: testutil.TestInt64Ptr(5)}},
				"c2":               {ElemValue: &drpc.RawValue{DoubleValue: testutil.TestFloat64Ptr(2.2)}},
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
	testutil.AssertEqForPointer(testAssert(t), int64(1), values[0]["c1"])
	testutil.AssertEqForPointer(testAssert(t), float64(2.2), values[0]["c2"])

	// perform a not found request
	mockedClient.EXPECT().Range(ctx, gomock.Any(), gomock.Any()).
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
	mockedClient.EXPECT().Range(ctx, gomock.Any(), gomock.Any()).
		Return(nil, errors.New("test error")).Times(1)
	values, token, err = sut.Range(ctx, testEi, map[string][]*dosa.Condition{"c2": {&dosa.Condition{
		Value: float64(3.3),
		Op:    dosa.Eq,
	}}}, nil, "", 64)
	assert.Nil(t, values)
	assert.Empty(t, token)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "test error")

	// perform remove range with a bad field value
	_, _, err = sut.Range(ctx, testEi, map[string][]*dosa.Condition{
		"c7": {&dosa.Condition{
			Value: dosa.UUID("baduuid"),
			Op:    dosa.Eq,
		}},
	}, nil, "", 64)
	assert.Error(t, err)
	assert.EqualError(t, errors.Cause(err), fmt.Sprintf("%s: baduuid", ErrInCorrectUUIDLength))
}

func TestConnector_RemoveRange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockedClient := dosatest.NewMockClient(ctrl)

	sut := Connector{client: mockedClient}
	fieldName := "c1"
	field := drpc.Field{&fieldName, &drpc.Value{ElemValue: &drpc.RawValue{Int64Value: testutil.TestInt64Ptr(10)}}}
	op := drpc.OperatorEq

	mockedClient.EXPECT().RemoveRange(ctx, gomock.Any(), gomock.Any()).Do(func(_ context.Context, request *drpc.RemoveRangeRequest, option yarpc2.CallOption) {
		assert.Equal(t, testRPCSchemaRef, *request.Ref)
		assert.Equal(t, len(request.Conditions), 1)
		condition := request.Conditions[0]
		assert.Equal(t, fieldName, *condition.Field.Name)
		assert.Equal(t, field.Value, condition.Field.Value)
		assert.Equal(t, &op, condition.Op)
	}).Return(nil)

	err := sut.RemoveRange(ctx, testEi, map[string][]*dosa.Condition{
		"c1": {&dosa.Condition{
			Value: int64(10),
			Op:    dosa.Eq,
		}},
	})
	assert.NoError(t, err)

	// perform a generic error request
	mockedClient.EXPECT().RemoveRange(ctx, gomock.Any(), gomock.Any()).Return(errors.New("test error")).Times(1)
	err = sut.RemoveRange(ctx, testEi, map[string][]*dosa.Condition{"c2": {&dosa.Condition{
		Value: 3.3,
		Op:    dosa.Eq,
	}}})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "test error")

	// perform remove range with a bad field value
	err = sut.RemoveRange(ctx, testEi, map[string][]*dosa.Condition{
		"c7": {&dosa.Condition{
			Value: dosa.UUID("baduuid"),
			Op:    dosa.Eq,
		}},
	})
	assert.Error(t, err)
	assert.EqualError(t, errors.Cause(err), "uuid: incorrect UUID length: baduuid")
}

func TestConnector_Scan(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockedClient := dosatest.NewMockClient(ctrl)

	testToken := "testToken"
	responseToken := "responseToken"
	testLimit := int32(32)
	// set up the parameters for success request
	sr := &drpc.ScanRequest{
		Ref:          &testRPCSchemaRef,
		Token:        &testToken,
		Limit:        &testLimit,
		FieldsToRead: map[string]struct{}{"c1": {}},
	}

	// set up the parameters for notfound request
	srNotFound := &drpc.ScanRequest{
		Ref:          &testRPCSchemaRef,
		Token:        &testToken,
		Limit:        &testLimit,
		FieldsToRead: map[string]struct{}{"c2": {}},
	}

	// set up the parameters for error request
	srErr := &drpc.ScanRequest{
		Ref:          &testRPCSchemaRef,
		Token:        &testToken,
		Limit:        &testLimit,
		FieldsToRead: map[string]struct{}{"c3": {}},
	}

	// successful call, return results
	mockedClient.EXPECT().Scan(ctx, sr, gomock.Any()).
		Do(func(_ context.Context, r *drpc.ScanRequest, option yarpc2.CallOption) {
			assert.Equal(t, sr, r)
		}).
		Return(&drpc.ScanResponse{
			Entities: []drpc.FieldValueMap{
				{
					"c1":               {ElemValue: &drpc.RawValue{Int64Value: testutil.TestInt64Ptr(1)}},
					"fieldNotInSchema": {ElemValue: &drpc.RawValue{Int64Value: testutil.TestInt64Ptr(5)}},
					"c2":               {ElemValue: &drpc.RawValue{DoubleValue: testutil.TestFloat64Ptr(2.2)}},
				},
			},
			NextToken: &responseToken,
		}, nil)
	// failed call, return error
	mockedClient.EXPECT().Scan(ctx, srErr, gomock.Any()).
		Return(nil, errors.New("test error"))
	// no results, make sure error is exact
	mockedClient.EXPECT().Scan(ctx, srNotFound, gomock.Any()).
		Return(nil, &dosa.ErrNotFound{})

	// Prepare the dosa client interface using the mocked RPC layer
	sut := Connector{client: mockedClient}

	// perform the successful request
	values, token, err := sut.Scan(ctx, testEi, []string{"c1"}, testToken, 32)
	assert.NoError(t, err)
	assert.Equal(t, responseToken, token)
	assert.NotNil(t, values)
	assert.Equal(t, 1, len(values))
	testutil.AssertEqForPointer(testAssert(t), int64(1), values[0]["c1"])
	testutil.AssertEqForPointer(testAssert(t), float64(2.2), values[0]["c2"])

	// perform a not found request
	values, token, err = sut.Scan(ctx, testEi, []string{"c2"}, testToken, 32)
	assert.Nil(t, values)
	assert.Empty(t, token)
	assert.Error(t, err)
	assert.True(t, dosa.ErrorIsNotFound(err))

	// perform a generic error request
	values, token, err = sut.Scan(ctx, testEi, []string{"c3"}, testToken, 32)
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
		KeyValues: getStubbedRemoveDOSARequest(),
	}

	// we expect a single call to Read, and we return back two fields, f1 which is in the typemap and another field that is not
	mockedClient.EXPECT().Remove(ctx, removeRequest, gomock.Any()).Return(nil)

	// Prepare the dosa client interface using the mocked RPC layer
	sut := Connector{client: mockedClient}

	// perform the read
	err := sut.Remove(ctx, testEi, getStubbedRemoveRequest())
	assert.Nil(t, err) // not an error

	// cover the conversion error case
	err = sut.Remove(ctx, testEi, map[string]dosa.FieldValue{"c7": dosa.UUID("321")})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "\"c7\"")               // must contain name of bad field
	assert.Contains(t, err.Error(), ErrInCorrectUUIDLength) // must mention that the uuid is too short

	// make sure we actually called Read on the interface
	ctrl.Finish()
}

func getStubbedUpsertRequests() [][]struct {
	Name  string
	Value interface{}
} {
	return [][]struct {
		Name  string
		Value interface{}
	}{
		{
			{"c1", int64(1)},
			{"c2", float64(2.2)},
			{"c3", "string"},
			{"c4", []byte{'b', 'i', 'n', 'a', 'r', 'y'}},
			{"c5", false},
			{"c6", int32(2)},
			{"c7", time.Now()},
		},
		{
			{"c1", testutil.TestInt64Ptr(1)},
			{"c2", testutil.TestFloat64Ptr(2.2)},
			{"c3", testutil.TestStringPtr("string")},
			{"c4", []byte{'b', 'i', 'n', 'a', 'r', 'y'}},
			{"c5", testutil.TestBoolPtr(false)},
			{"c6", testutil.TestInt32Ptr(2)},
			{"c7", testutil.TestTimePtr(time.Now())},
		},
	}
}

func getStubbedRemoveDOSARequest() map[string]*drpc.Value {
	return map[string]*drpc.Value{"f1": {ElemValue: &drpc.RawValue{Int64Value: testutil.TestInt64Ptr(5)}}}
}

func getStubbedRemoveRequest() map[string]dosa.FieldValue {
	return map[string]dosa.FieldValue{"f1": dosa.FieldValue(int64(5))}
}

// TestPanic is an unimplemented method test for coverage, remove these as they are implemented
func TestPanic(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockedClient := dosatest.NewMockClient(ctrl)

	sut := Connector{client: mockedClient}

	assert.Panics(t, func() {
		sut.ScopeExists(ctx, "")
	})
}
