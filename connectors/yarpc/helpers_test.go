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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	dosarpc "github.com/uber/dosa-idl/.gen/dosa"
)

var (
	tableName      = "testentity"
	uuidKeyField   = "uuidkeyfield"
	uuidField      = "uuidfield"
	stringKeyField = "stringkeyfield"
	stringField    = "stringfield"
	int32Field     = "int32field"
	int64KeyField  = "int64keyfield"
	int64Field     = "int64field"
	doubleField    = "doublefield"
	blobField      = "blobfield"
	timestampField = "timestampfield"
	boolField      = "boolfield"
)

func TestRawValueFromInterfaceBadType(t *testing.T) {
	assert.Panics(t, func() {
		RawValueFromInterface(func() {})
	})
}

func TestRawValueAsInterfaceBadType(t *testing.T) {
	assert.Panics(t, func() {
		RawValueAsInterface(dosarpc.RawValue{}, dosa.Invalid)
	})
}

func TestRPCTypeFromClientType(t *testing.T) {
	assert.Panics(t, func() {
		RPCTypeFromClientType(dosa.Invalid)
	})
}

func TestRawValueFromInterfaceNilBlob(t *testing.T) {
	var blob []byte
	raw, _ := RawValueFromInterface(blob)
	_, err := raw.ToWire()
	assert.NoError(t, err)
}

func TestRawValueConversionError(t *testing.T) {
	data := []struct {
		input  interface{}
		errmsg string
	}{
		{dosa.UUID(""), "short"}, // empty string
		{dosa.UUID("1"), "short"},
		{dosa.UUID("this is not a uuid, uuids shouldnt contain something like a t in them"), "invalid byte"},
	}

	for _, test := range data {
		_, err := RawValueFromInterface(test.input)
		assert.Error(t, err, "test %+v", test)
		assert.Contains(t, err.Error(), test.errmsg, "test %+v", test)
	}

	// happy path
	v, err := RawValueFromInterface(dosa.UUID("80bccd66-9517-4f54-9dec-0ddb87d0dc2a"))
	assert.NoError(t, err)
	assert.NotNil(t, v)
}

// TODO: add additional happy path unit tests here. The helpers currently get
// good coverage from the connectors though.

var testEntityDefinition = &dosa.EntityDefinition{
	Name: tableName,
	Key: &dosa.PrimaryKey{
		PartitionKeys: []string{uuidKeyField},
		ClusteringKeys: []*dosa.ClusteringKey{
			{
				Name:       stringKeyField,
				Descending: false,
			},
			{
				Name:       int64KeyField,
				Descending: true,
			},
		},
	},
	Indexes: map[string]*dosa.IndexDefinition{
		"index1": {
			Key: &dosa.PrimaryKey{
				PartitionKeys: []string{uuidField},
				ClusteringKeys: []*dosa.ClusteringKey{
					{
						Name:       stringKeyField,
						Descending: false,
					},
				},
			},
		},
		"index2": {
			Key: &dosa.PrimaryKey{
				PartitionKeys: []string{int64Field, uuidKeyField},
				ClusteringKeys: []*dosa.ClusteringKey{
					{
						Name:       stringKeyField,
						Descending: false,
					},
				},
			},
		},
	},
	Columns: []*dosa.ColumnDefinition{
		{
			Name: uuidKeyField,
			Type: dosa.TUUID,
		},
		{
			Name: stringKeyField,
			Type: dosa.String,
		},
		{
			Name: int64KeyField,
			Type: dosa.Int64,
		},

		{
			Name: int32Field,
			Type: dosa.Int32,
		},
		{
			Name: doubleField,
			Type: dosa.Double,
		},
		{
			Name: blobField,
			Type: dosa.Blob,
		},
		{
			Name: timestampField,
			Type: dosa.Timestamp,
		},
		{
			Name: boolField,
			Type: dosa.Bool,
		},
		{
			Name: uuidField,
			Type: dosa.TUUID,
		},
		{
			Name: stringField,
			Type: dosa.String,
		},
		{
			Name: int64Field,
			Type: dosa.Int64,
		},
	},
}

func TestEntityDefinitionConvert(t *testing.T) {
	rpcEd := EntityDefinitionToThrift(testEntityDefinition)
	ed := FromThriftToEntityDefinition(rpcEd)
	assert.Equal(t, testEntityDefinition.Key, ed.Key)
	assert.Equal(t, testEntityDefinition.Name, ed.Name)
	assert.Equal(t, testEntityDefinition.Indexes, ed.Indexes)
	edCols := make(map[string]*dosa.ColumnDefinition)
	for _, c := range ed.Columns {
		edCols[c.Name] = c
	}

	testCols := make(map[string]*dosa.ColumnDefinition)
	for _, c := range testEntityDefinition.Columns {
		testCols[c.Name] = c
	}
	assert.Equal(t, edCols, testCols)
}

func TestEncodeOperator(t *testing.T) {
	data := []struct {
		dop   dosa.Operator
		rpcop dosarpc.Operator
	}{
		{dop: dosa.Eq, rpcop: dosarpc.OperatorEq},
		{dop: dosa.Gt, rpcop: dosarpc.OperatorGt},
		{dop: dosa.Lt, rpcop: dosarpc.OperatorLt},
		{dop: dosa.LtOrEq, rpcop: dosarpc.OperatorLtOrEq},
		{dop: dosa.GtOrEq, rpcop: dosarpc.OperatorGtOrEq},
	}

	for _, test := range data {
		assert.Equal(t, test.rpcop, *encodeOperator(test.dop))
	}
}
