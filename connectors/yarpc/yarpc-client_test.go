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
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors"
	drpc "github.com/uber/dosa-idl/.gen/dosa"
	"github.com/uber/dosa-idl/.gen/dosa/dosatest"
	"testing"
	"time"
)

func getTestSchemaReferenceMap() map[dosa.SchemaReference]SchemaReferenceInfo {
	return map[dosa.SchemaReference]SchemaReferenceInfo{dosa.SchemaReference("test"): {schemaID: drpc.SchemaID{}, typeMap: nil}}
}

func TestYaRPCClient_Read(t *testing.T) {
	// build a mock RPC client
	ctrl := gomock.NewController(t)
	mockedClient := dosatest.NewMockClient(ctrl)
	ctx := context.TODO()

	readRequest := drpc.ReadRequest{SchemaID: &drpc.SchemaID{}, Key: map[string]*drpc.Value{}, FieldsToRead: map[string]struct{}{"f1": {}}}
	mockedClient.EXPECT().Read(ctx, &readRequest).Return(&drpc.ReadResponse{&drpc.Entity{Fields: nil}}, nil)

	sut := Client{Client: mockedClient, SchemaReferenceMap: getTestSchemaReferenceMap()}

	values, err := sut.Read(ctx, dosa.SchemaReference("test"), nil, []string{"f1"})
	assert.Nil(t, err)
	assert.NotNil(t, values)

	// make sure we actually called Read on the interface
	ctrl.Finish()
}
func TestBadSchemaRef(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockedClient := dosatest.NewMockClient(ctrl)
	ctx := context.TODO()

	sut := Client{Client: mockedClient}
	const badReference = dosa.SchemaReference("Bad")
	for i := 1; i < 2; i++ {
		var err error
		switch i {
		case 1:
			_, err = sut.Read(ctx, badReference, nil, nil)
		case 2:
			err = sut.CreateIfNotExists(ctx, badReference, nil)
			// TODO: Add tests for other methods that can take a bad schema reference
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
	inFields := map[string]interface{}{}
	outFields := map[string]*drpc.Value{}
	for _, item := range vals {
		inFields[item.Name] = item.Value
		outFields[item.Name] = &drpc.Value{ElemValue: connector.RawValueFromInterface(item.Value)}
	}

	mockedClient.EXPECT().CreateIfNotExists(ctx, &drpc.CreateRequest{Entity: &drpc.Entity{SchemaID: &drpc.SchemaID{}, Fields: outFields}})

	// create the YaRPCClient and give it the mocked RPC interface
	// see https://en.wiktionary.org/wiki/SUT for the reason this is called sut
	sut := Client{Client: mockedClient, SchemaReferenceMap: getTestSchemaReferenceMap()}

	// and run the test
	sut.CreateIfNotExists(ctx, dosa.SchemaReference("test"), inFields)

	// make sure we actually called CreateIfNotExists on the interface
	ctrl.Finish()
}
