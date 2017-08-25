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

package reqcache_test

import (
	"context"
	"testing"

	"github.com/uber-go/dosa/connectors/reqcache"
	"github.com/stretchr/testify/assert"
	"github.com/golang/mock/gomock"

	"github.com/uber-go/dosa/connectors/memory"
	"github.com/uber-go/dosa/mocks"
	"github.com/uber-go/dosa"
)

var testSchemaRef = dosa.SchemaRef{
	Scope:      "scope1",
	NamePrefix: "namePrefix",
	EntityName: "eName",
	Version:    12345,
}


var clusteredEi = &dosa.EntityInfo{
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
			ClusteringKeys: []*dosa.ClusteringKey{
				{Name: "c1", Descending: false},
				{Name: "c7", Descending: true},
			},
		},
		Name: "t2",
		Indexes: map[string]*dosa.IndexDefinition{
			"i2": {Key: &dosa.PrimaryKey{PartitionKeys: []string{"c1"}}}},
	},
}


func TestContext(t *testing.T) {
	ctx := context.Background()
	ctx = reqcache.CacheableContext(ctx)
	assert.IsType(t, &memory.Connector{}, ctx.Value(reqcache.ReqCacheKey))
}

const testScope = "testScope"
const testPrefix = "testPrefix"

// each API is tested without setting up a CacheableContext; this should basically do nothing and
// pass each call to the downstream (mocked) connector
func TestPassthrough(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockedConnector := mocks.NewMockConnector(ctrl)
	sut := reqcache.NewConnector(mockedConnector)
	ctx := context.Background()

	// CheckSchema
	mockedConnector.EXPECT().CheckSchema(ctx, testScope, testPrefix, nil).Return(int32(1), nil)
	version, err := sut.CheckSchema(context.Background(), testScope, testPrefix, nil)
	assert.Nil(t, err)
	assert.Equal(t, int32(1), version)

	// CheckSchemaStatus
	mockedConnector.EXPECT().CheckSchemaStatus(ctx, testScope, testPrefix, int32(1)).Return(&dosa.SchemaStatus{Version: 1, Status: "test"}, nil)
	ss, err := sut.CheckSchemaStatus(ctx, testScope, testPrefix, int32(1))
	assert.Nil(t, err)
	assert.Equal(t, "test", ss.Status)

	// Read
	mockedConnector.EXPECT().Read(ctx, clusteredEi, map[string]dosa.FieldValue{}, dosa.All()).Return(map[string]dosa.FieldValue{}, nil)
	vals, err := sut.Read(ctx, clusteredEi, map[string]dosa.FieldValue{}, dosa.All())
	assert.Nil(t, err)
	assert.Empty(t, vals)

	// Remove
	mockedConnector.EXPECT().Remove(ctx, clusteredEi, map[string]dosa.FieldValue{}).Return(nil)
	assert.NoError(t, sut.Remove(ctx, clusteredEi, map[string]dosa.FieldValue{}))

	// RemoveRange
	mockedConnector.EXPECT().RemoveRange(ctx, clusteredEi, map[string][]*dosa.Condition{}).Return(nil)
	assert.NoError(t, sut.RemoveRange(ctx, clusteredEi, map[string][]*dosa.Condition{}))

	// Scan
	mockedConnector.EXPECT().Scan(ctx, clusteredEi, nil, "", 1).Return(nil, "", nil)
	data, token, err := sut.Scan(ctx, clusteredEi, nil, "", 1)
	assert.NoError(t, err)
	assert.Empty(t, data)
	assert.Empty(t, token)

	// Shutdown
	mockedConnector.EXPECT().Shutdown().Return(nil)
	assert.NoError(t, sut.Shutdown())

	// CreateIfNotExists
	mockedConnector.EXPECT().CreateIfNotExists(ctx, clusteredEi, nil).Return(nil)
	assert.NoError(t, sut.CreateIfNotExists(ctx, clusteredEi, nil))

	//TODO:
	//sut.CreateIfNotExists
	//sut.CreateScope
	//sut.DropScope

	//sut.ScopeExists
	//sut.Shutdown
	//sut.TruncateScope
	//sut.Upsert
	//sut.UpsertSchema
}

var key1 = map[string]dosa.FieldValue{"f1":"k1", "c1": int64(1), "c7": dosa.NewUUID()}


func TestConnector_Read(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockedConnector := mocks.NewMockConnector(ctrl)
	sut := reqcache.NewConnector(mockedConnector)
	ctx := reqcache.CacheableContext(context.Background())
	data1 := map[string]dosa.FieldValue{"c3": "string"}

	for k, v := range key1 {
		data1[k] = v
	}

	// this Read will populate the cache
	mockedConnector.EXPECT().Read(ctx, clusteredEi, gomock.Any(), dosa.All()).Return(data1, nil)
	data, err := sut.Read(ctx, clusteredEi, key1, dosa.All())
	assert.NoError(t, err)
	assert.Equal(t, data1, data)

	// reread it, this will not result in a call to downstream
	data, err = sut.Read(ctx, clusteredEi, key1, dosa.All())
	assert.NoError(t, err)
	assert.Equal(t, data1, data)
}

func TestConnector_Range(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockedConnector := mocks.NewMockConnector(ctrl)
	sut := reqcache.NewConnector(mockedConnector)
	ctx := reqcache.CacheableContext(context.Background())
	data1 := map[string]dosa.FieldValue{"c3": "string"}

	for k, v := range key1 {
		data1[k] = v
	}
	// this Range will populate the cache
	mockedConnector.EXPECT().Range(ctx, clusteredEi, gomock.Any(), dosa.All(), "", 10).Return([]map[string]dosa.FieldValue{data1}, "", nil)
	data, token, err := sut.Range(ctx, clusteredEi, map[string][]*dosa.Condition{"f1":[]*dosa.Condition{&dosa.Condition{Op: dosa.Eq, Value: "k1"}}}, dosa.All(), "", 10)
	assert.NoError(t, err)
	assert.Equal(t, data1, data[0])
	assert.Empty(t, token)

	// reread it, this will not result in a call to downstream
	read_data, err := sut.Read(ctx, clusteredEi, key1, dosa.All())
	assert.NoError(t, err)
	assert.Equal(t, data1, read_data)
}

func TestConnector_Upsert(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockedConnector := mocks.NewMockConnector(ctrl)
	sut := reqcache.NewConnector(mockedConnector)
	ctx := reqcache.CacheableContext(context.Background())
	data1 := map[string]dosa.FieldValue{"c3": "string"}

	for k, v := range key1 {
		data1[k] = v
	}
	mockedConnector.EXPECT().Upsert(ctx, clusteredEi, data1).Return(nil)
	assert.NoError(t, sut.Upsert(ctx, clusteredEi, data1))

	// read it, this will not result in a call to downstream
	read_data, err := sut.Read(ctx, clusteredEi, key1, dosa.All())
	assert.NoError(t, err)
	assert.Equal(t, data1, read_data)
}

// test unimplemented functions
func TestPanics(t *testing.T) {
	sut := reqcache.NewConnector(nil)

	assert.Panics(t, func() {
		_, _ = sut.MultiRead(context.TODO(), clusteredEi, nil, dosa.All())
	})

	assert.Panics(t, func() {
		_, _ = sut.MultiUpsert(context.TODO(), clusteredEi, nil)
	})
	assert.Panics(t, func() {
		_, _ = sut.MultiRemove(context.TODO(), clusteredEi, nil)
	})
	assert.Panics(t, func() {
		_, _, _ = sut.Search(context.TODO(), clusteredEi, dosa.FieldNameValuePair{}, dosa.All(), "", 0)
	})
}

func TestConnector_Remove(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockedConnector := mocks.NewMockConnector(ctrl)
	sut := reqcache.NewConnector(mockedConnector)
	ctx := reqcache.CacheableContext(context.Background())
	data1 := map[string]dosa.FieldValue{"c3": "string"}

	for k, v := range key1 {
		data1[k] = v
	}
	// this Read will populate the cache
	mockedConnector.EXPECT().Read(ctx, clusteredEi, gomock.Any(), dosa.All()).Return(data1, nil).Times(2)
	data, err := sut.Read(ctx, clusteredEi, key1, dosa.All())
	assert.NoError(t, err)
	assert.Equal(t, data1, data)

	// reread it, this will not result in a call to downstream
	data, err = sut.Read(ctx, clusteredEi, key1, dosa.All())
	assert.NoError(t, err)
	assert.Equal(t, data1, data)

	// now delete it
	mockedConnector.EXPECT().Remove(ctx, clusteredEi, key1).Return(nil)
	assert.NoError(t, sut.Remove(ctx, clusteredEi, key1))

	// this read will repopulate the cache, consuming one of the Read calls
	data, err = sut.Read(ctx, clusteredEi, key1, dosa.All())
	assert.NoError(t, err)
	assert.Equal(t, data1, data)

	// reread it, this will not result in a call to downstream
	data, err = sut.Read(ctx, clusteredEi, key1, dosa.All())
	assert.NoError(t, err)
	assert.Equal(t, data1, data)
}