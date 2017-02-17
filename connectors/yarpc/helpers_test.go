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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/yarpc"
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
		yarpc.RawValueFromInterface(func() {})
	})
}

func TestRawValueAsInterfaceBadType(t *testing.T) {
	assert.Panics(t, func() {
		yarpc.RawValueAsInterface(dosarpc.RawValue{}, dosa.Invalid)
	})
}

func TestRPCTypeFromClientType(t *testing.T) {
	assert.Panics(t, func() {
		yarpc.RPCTypeFromClientType(dosa.Invalid)
	})
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
	rpcEd := yarpc.EntityDefinitionToThrift(testEntityDefinition)
	ed := yarpc.FromThriftToEntityDefinition(rpcEd)
	assert.Equal(t, testEntityDefinition.Key, ed.Key)
	assert.Equal(t, testEntityDefinition.Name, ed.Name)
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
