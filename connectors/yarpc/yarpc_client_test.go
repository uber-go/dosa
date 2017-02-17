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
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	drpc "github.com/uber/dosa-idl/.gen/dosa"
	"github.com/uber/dosa-idl/.gen/dosa/dosatest"
)

const testSchemaReference = dosa.SchemaReference("test")

func getTestSchemaReferenceMap() map[dosa.SchemaReference]SchemaReferenceInfo {
	return map[dosa.SchemaReference]SchemaReferenceInfo{testSchemaReference: {schemaID: drpc.SchemaID{}, typeMap: map[string]dosa.Type{
		"c1": dosa.Int64,
		"c2": dosa.Double,
		"c3": dosa.String,
		"c4": dosa.Blob,
		"c5": dosa.Bool,
		"c6": dosa.Int32,
	}}}
}

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

// Test a happy path read of one column and specify the primary key
func TestYaRPCClient_Read(t *testing.T) {
	// build a mock RPC client
	ctrl := gomock.NewController(t)
	mockedClient := dosatest.NewMockClient(ctrl)

	// set up the parameters
	ctx := context.TODO()
	sr := testSchemaReference
	readRequest := drpc.ReadRequest{
		SchemaID: &drpc.SchemaID{},
		Key:      map[string]*drpc.Value{"f1": {ElemValue: &drpc.RawValue{Int64Value: testInt64Ptr(5)}}}, FieldsToRead: map[string]struct{}{"f1": {}},
	}

	// we expect a single call to Read, and we return back two fields, f1 which is in the typemap and another field that is not
	mockedClient.EXPECT().Read(ctx, &readRequest).Return(&drpc.ReadResponse{&drpc.Entity{Fields: map[string]*drpc.Value{
		"c1":               {ElemValue: &drpc.RawValue{Int64Value: testInt64Ptr(1)}},
		"fieldNotInSchema": {ElemValue: &drpc.RawValue{Int64Value: testInt64Ptr(5)}},
		"c2":               {ElemValue: &drpc.RawValue{DoubleValue: testFloat64Ptr(2.2)}},
		"c3":               {ElemValue: &drpc.RawValue{StringValue: testStringPtr("f3value")}},
		"c4":               {ElemValue: &drpc.RawValue{BinaryValue: []byte{'b', 'i', 'n', 'a', 'r', 'y'}}},
		"c5":               {ElemValue: &drpc.RawValue{BoolValue: testBoolPtr(false)}},
		"c6":               {ElemValue: &drpc.RawValue{Int32Value: testInt32Ptr(1)}},
	}}}, nil)

	// Prepare the dosa client interface using the mocked RPC layer
	sut := Client{Client: mockedClient, SchemaReferenceMap: getTestSchemaReferenceMap()}

	// perform the read
	values, err := sut.Read(ctx, sr, map[string]dosa.FieldValue{"f1": dosa.FieldValue(int64(5))}, []string{"f1"})
	assert.Nil(t, err)       // not an error
	assert.NotNil(t, values) // found some values
	fmt.Printf("%v", values)
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

// TestBadSchemaRef covers the case where a bad schema reference is sent to a method. This should get caught early,
// so not much setup is done for each method.
func TestBadSchemaRef(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockedClient := dosatest.NewMockClient(ctrl)
	ctx := context.TODO()

	sut := Client{Client: mockedClient}
	const badReference = dosa.SchemaReference("Bad")
	for i := 1; i < 10; i++ {
		var err error
		switch i {
		case 1:
			_, err = sut.Read(ctx, badReference, nil, nil)
		case 2:
			err = sut.CreateIfNotExists(ctx, badReference, nil)
		case 3:
			err = sut.Upsert(ctx, badReference, nil, nil)
		default:
			continue
		}
		assert.Error(t, err, "case %d", i)
		assert.Contains(t, err.Error(), `"Bad"`)
	}

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
	outFields := map[string]*drpc.Value{}
	for _, item := range vals {
		inFields[item.Name] = item.Value
		outFields[item.Name] = &drpc.Value{ElemValue: RawValueFromInterface(item.Value)}
	}

	mockedClient.EXPECT().CreateIfNotExists(ctx, &drpc.CreateRequest{Entity: &drpc.Entity{SchemaID: &drpc.SchemaID{}, Fields: outFields}})

	// create the YaRPCClient and give it the mocked RPC interface
	// see https://en.wiktionary.org/wiki/SUT for the reason this is called sut
	sut := Client{Client: mockedClient, SchemaReferenceMap: getTestSchemaReferenceMap()}

	// and run the test
	err := sut.CreateIfNotExists(ctx, dosa.SchemaReference("test"), inFields)
	assert.Nil(t, err)

	// make sure we actually called CreateIfNotExists on the interface
	ctrl.Finish()
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
		outFields[item.Name] = &drpc.Value{ElemValue: RawValueFromInterface(item.Value)}
	}

	mockedClient.EXPECT().Upsert(ctx, &drpc.UpsertRequest{
		Entities:       []*drpc.Entity{{SchemaID: &drpc.SchemaID{}, Fields: outFields}},
		FieldsToUpdate: nil,
	})
	mockedClient.EXPECT().Upsert(ctx, &drpc.UpsertRequest{
		Entities:       []*drpc.Entity{{SchemaID: &drpc.SchemaID{}, Fields: outFields}},
		FieldsToUpdate: map[string]struct{}{"c1": {}, "c2": {}},
	})

	// create the YaRPCClient and give it the mocked RPC interface
	// see https://en.wiktionary.org/wiki/SUT for the reason this is called sut
	sut := Client{Client: mockedClient, SchemaReferenceMap: getTestSchemaReferenceMap()}

	// and run the test, first with a nil FieldsToUpdate, then with a specific list
	err := sut.Upsert(ctx, dosa.SchemaReference("test"), inFields, nil)
	assert.Nil(t, err)
	err = sut.Upsert(ctx, dosa.SchemaReference("test"), inFields, []string{"c1", "c2"})
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

	mockedClient.EXPECT().CheckSchema(ctx, gomock.Any()).Return(&drpc.CheckSchemaResponse{[]*drpc.SchemaID{{}, {}}}, nil)

	sut := Client{Client: mockedClient}

	ed, err := dosa.TableFromInstance(&TestDosaObject{})
	assert.NoError(t, err)

	sr, err := sut.CheckSchema(ctx, []*dosa.EntityDefinition{&ed.EntityDefinition})
	assert.NoError(t, err)
	assert.NotNil(t, sr)
}

// TestPanic is an unimplemented method test for coverage, remove these as they are implemented
func TestPanic(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockedClient := dosatest.NewMockClient(ctrl)
	ctx := context.TODO()

	sut := Client{Client: mockedClient}

	assert.Panics(t, func() {
		sut.BatchRead(ctx, testSchemaReference, nil, nil)
	})

	assert.Panics(t, func() {
		sut.BatchUpsert(ctx, testSchemaReference, nil, nil)
	})

	assert.Panics(t, func() {
		sut.CreateScope(ctx, "")
	})

	assert.Panics(t, func() {
		sut.UpsertSchema(ctx, nil)
	})

	assert.Panics(t, func() {
		sut.DropScope(ctx, "")
	})

	assert.Panics(t, func() {
		sut.Remove(ctx, testSchemaReference, nil)
	})

	assert.Panics(t, func() {
		sut.TruncateScope(ctx, "")
	})

	assert.Panics(t, func() {
		sut.Range(ctx, testSchemaReference, nil, nil, "", 0)
	})

	assert.Panics(t, func() {
		sut.Search(ctx, testSchemaReference, nil, nil, "", 0)
	})

	assert.Panics(t, func() {
		sut.Scan(ctx, testSchemaReference, nil, "", 0)
	})
}
