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

package cassandra_test

import (
	"context"

	"testing"

	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
	. "github.com/uber-go/dosa/connectors/cassandra"
)

const (
	keyspace       = "datastore_test"
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

var testStore *Connector

func initTestStore(t *testing.T) {
	ts := GetTestConnector(t)
	testStore = ts.(*Connector)
}

func initTestSchema(ks string, entityInfo *dosa.EntityInfo) error {
	ctx := context.Background()
	err := testStore.CreateScope(ctx, ks)
	if err != nil {
		return errors.Wrapf(err, "Could not create keyspace %q", ks)
	}
	_, err = testStore.UpsertSchema(
		ctx, entityInfo.Ref.Scope, entityInfo.Ref.NamePrefix, []*dosa.EntityDefinition{entityInfo.Def},
	)
	return err
}

func removeTestSchema(ks string) {
	ctx := context.Background()
	testStore.DropScope(ctx, ks)
}

var testEntityInfo = newTestEntityInfo(keyspace)

func newTestEntityInfo(sp string) *dosa.EntityInfo {
	return &dosa.EntityInfo{
		Ref: &dosa.SchemaRef{
			Scope:      sp,
			NamePrefix: "test.datastore",
			EntityName: "testentity",
		},
		Def: &dosa.EntityDefinition{
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
					Name: int32Field,
					Type: dosa.Int32,
				},
				{
					Name: int64KeyField,
					Type: dosa.Int64,
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
		},
	}
}
