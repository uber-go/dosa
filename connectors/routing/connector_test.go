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

package routing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"sort"

	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/devnull"
	"github.com/uber-go/dosa/connectors/memory"
	"github.com/uber-go/dosa/connectors/random"
	"reflect"
)

const idcount = 10

var (
	cfg = Config{
		Routers: Routers{
			buildRouter("production", "map", "memory"),
			buildRouter("production", "default", "random"),
			buildRouter("eats", "eats-store", "memory"),
			buildRouter("eats", "default", "random"),
			buildRouter("eats", "bazaar.*", "memory"),
			buildRouter("eats", "*", "devnull"),
			buildRouter("development", "map", "memory"),
			buildRouter("development", "default", "random"),
			buildRouter("default", "default", "memory"),
		},
	}
	testInfo = &dosa.EntityInfo{
		Ref: &dosa.SchemaRef{
			Scope:      "production",
			NamePrefix: "map",
			EntityName: "testEntityName",
		},
		Def: &dosa.EntityDefinition{
			Columns: []*dosa.ColumnDefinition{
				{Name: "p1", Type: dosa.String},
				{Name: "c1", Type: dosa.Int64},
				{Name: "c2", Type: dosa.Double},
				{Name: "c3", Type: dosa.String},
				{Name: "c4", Type: dosa.Blob},
				{Name: "c5", Type: dosa.Bool},
				{Name: "c6", Type: dosa.Int32},
				{Name: "c7", Type: dosa.TUUID},
			},
			Key: &dosa.PrimaryKey{
				PartitionKeys: []string{"p1"},
			},
			Name: "t1",
			Indexes: map[string]*dosa.IndexDefinition{
				"i1": {Key: &dosa.PrimaryKey{PartitionKeys: []string{"c1"}}}},
		},
	}
	testInfoRandom = &dosa.EntityInfo{
		Ref: &dosa.SchemaRef{
			Scope:      "production",
			NamePrefix: "default",
			EntityName: "testEntityName",
		},
		Def: &dosa.EntityDefinition{
			Columns: []*dosa.ColumnDefinition{
				{Name: "p1", Type: dosa.String},
				{Name: "c1", Type: dosa.Int64},
				{Name: "c2", Type: dosa.Double},
				{Name: "c3", Type: dosa.String},
				{Name: "c4", Type: dosa.Blob},
				{Name: "c5", Type: dosa.Bool},
				{Name: "c6", Type: dosa.Int32},
				{Name: "c7", Type: dosa.TUUID},
			},
			Key: &dosa.PrimaryKey{
				PartitionKeys: []string{"p1"},
			},
			Name: "t1",
			Indexes: map[string]*dosa.IndexDefinition{
				"i1": {Key: &dosa.PrimaryKey{PartitionKeys: []string{"c1"}}}},
		},
	}
	clusteredEi = &dosa.EntityInfo{
		Ref: &dosa.SchemaRef{
			Scope:      "eats",
			NamePrefix: "bazaar.v1",
			EntityName: "testEntityName",
		},
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
	testNoMatchInfo = &dosa.EntityInfo{
		Ref: &dosa.SchemaRef{
			Scope:      "weirdScope",
			NamePrefix: "testPrefix",
			EntityName: "testEntityName",
		},
		Def: &dosa.EntityDefinition{
			Columns: []*dosa.ColumnDefinition{
				{Name: "p1", Type: dosa.String},
				{Name: "c1", Type: dosa.Int64},
				{Name: "c2", Type: dosa.Double},
				{Name: "c3", Type: dosa.String},
				{Name: "c4", Type: dosa.Blob},
				{Name: "c5", Type: dosa.Bool},
				{Name: "c6", Type: dosa.Int32},
				{Name: "c7", Type: dosa.TUUID},
			},
			Key: &dosa.PrimaryKey{
				PartitionKeys: []string{"p1"},
			},
			Name: "t1",
			Indexes: map[string]*dosa.IndexDefinition{
				"i1": {Key: &dosa.PrimaryKey{PartitionKeys: []string{"c1"}}}},
		},
	}
	testPairs = dosa.FieldNameValuePair{}
	ctx       = context.Background()
)

func getConnectorMap() map[string]dosa.Connector {
	return map[string]dosa.Connector{
		"memory":  memory.NewConnector(),
		"devnull": devnull.NewConnector(),
		"random":  random.NewConnector(),
	}
}

func TestGetConnector(t *testing.T) {
	connectorMap := getConnectorMap()
	// no plugin
	// glob match
	rc := NewRoutingConnector(cfg, connectorMap, nil)
	ei := &dosa.EntityInfo{
		Ref: &dosa.SchemaRef{Scope: "eats", NamePrefix: "bazaar.v1"},
	}
	conn, err := rc.getConnector(ei.Ref.Scope, ei.Ref.NamePrefix, "Read")
	assert.Nil(t, err)
	assert.NotNil(t, conn)

	// exact match
	ei.Ref.NamePrefix = "eats-store"
	conn, err = rc.getConnector(ei.Ref.Scope, ei.Ref.NamePrefix, "Read")
	assert.Nil(t, err)
	assert.NotNil(t, conn)

	// match "*"
	conn, err = rc.getConnector(ei.Ref.Scope, "*", "Read")
	assert.Nil(t, err)
	assert.NotNil(t, conn)

	// match default
	ei = &dosa.EntityInfo{
		Ref: &dosa.SchemaRef{Scope: "notexist", NamePrefix: "bazaar.v1"},
	}
	conn, err = rc.getConnector(ei.Ref.Scope, ei.Ref.NamePrefix, "Read")
	assert.Nil(t, err)
	assert.NotNil(t, conn)
	assert.Equal(t, reflect.TypeOf(conn), reflect.TypeOf(memory.NewConnector()))

	// with plugin
	rc.PluginFunc = func(scope, namePrefix, opName string) (string, string, error) {
		return "eats", "eats-store", nil
	}

	conn, err = rc.getConnector(ei.Ref.Scope, ei.Ref.NamePrefix, "Read")
	assert.NoError(t, err)
	assert.NotNil(t, conn)

	// plugin returns error
	rc.PluginFunc = func(scope, namePrefix, opName string) (string, string, error) {
		return "", "", errors.New("")
	}
	conn, err = rc.getConnector(ei.Ref.Scope, ei.Ref.NamePrefix, "Read")
	assert.Contains(t, err.Error(), "failed to execute getConnector due to Plugin function error")
	assert.Nil(t, conn)
}

func TestRoutingConnector_CreateIfNotExists(t *testing.T) {
	connectorMap := getConnectorMap()
	rc := NewRoutingConnector(cfg, connectorMap, nil)

	assert.NoError(t, rc.CreateIfNotExists(ctx, testInfo, map[string]dosa.FieldValue{
		"p1": dosa.FieldValue("data")}))

	err := rc.CreateIfNotExists(ctx, testInfo, map[string]dosa.FieldValue{
		"p1": dosa.FieldValue("data")})
	assert.True(t, dosa.ErrorIsAlreadyExists(err))
}

func TestRoutingConnector_CreateIfNotExistsDefaultScope(t *testing.T) {
	connectorMap := getConnectorMap()
	rc := NewRoutingConnector(cfg, connectorMap, nil)

	// not exist scope, use default
	err := rc.CreateIfNotExists(ctx, testNoMatchInfo, map[string]dosa.FieldValue{
		"p1": dosa.FieldValue("data")})
	assert.NoError(t, err)

	plugin := func(scope, namePrefix, opName string) (string, string, error) {
		return "", "", errors.New("dummy errors")
	}
	rc.PluginFunc = plugin
	// not exist scope, use default
	err = rc.CreateIfNotExists(ctx, testNoMatchInfo, map[string]dosa.FieldValue{
		"p1": dosa.FieldValue("data")})
	assert.Contains(t, err.Error(), "dummy errors")
}

func TestRoutingConnector_CreateIfNotExists2(t *testing.T) {
	connectorMap := getConnectorMap()
	rc := NewRoutingConnector(cfg, connectorMap, nil)

	testUUIDs := make([]dosa.UUID, 10)
	for x := 0; x < 10; x++ {
		testUUIDs[x] = dosa.NewUUID()
	}

	// first, insert 10 random UUID values into same partition key
	for x := 0; x < 10; x++ {
		err := rc.CreateIfNotExists(ctx, clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue("data"),
			"c1": dosa.FieldValue(int64(1)),
			"c7": dosa.FieldValue(testUUIDs[x])})
		assert.NoError(t, err)
	}
	// attempt to insert them all again
	for x := 0; x < 10; x++ {
		err := rc.CreateIfNotExists(ctx, clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue("data"),
			"c1": dosa.FieldValue(int64(1)),
			"c7": dosa.FieldValue(testUUIDs[x])})
		assert.Error(t, err, string(testUUIDs[x]))
		assert.True(t, dosa.ErrorIsAlreadyExists(err))
	}
	// now, insert them again, but this time with a different secondary key
	for x := 0; x < 10; x++ {
		err := rc.CreateIfNotExists(ctx, clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue("data"),
			"c1": dosa.FieldValue(int64(2)),
			"c7": dosa.FieldValue(testUUIDs[x])})
		assert.NoError(t, err)
	}
	// and with a different primary key
	for x := 0; x < 10; x++ {
		err := rc.CreateIfNotExists(ctx, clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue("different"),
			"c1": dosa.FieldValue(int64(1)),
			"c7": dosa.FieldValue(testUUIDs[x])})
		assert.NoError(t, err)
	}
	data, token, err := rc.Range(ctx, clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
	}, dosa.All(), "", 200)
	assert.NoError(t, err)
	assert.Empty(t, token)
	assert.Len(t, data, 20)
}

func TestRoutingConnector_Read(t *testing.T) {
	connectorMap := getConnectorMap()
	rc := NewRoutingConnector(cfg, connectorMap, nil)

	// read with no data
	val, err := rc.Read(ctx, testInfo, map[string]dosa.FieldValue{
		"p1": dosa.FieldValue("data"),
	}, []string{"c1"})
	assert.Nil(t, val)
	assert.True(t, dosa.ErrorIsNotFound(err))

	// read with no key field
	val, err = rc.Read(ctx, testInfo, map[string]dosa.FieldValue{}, dosa.All())
	assert.Nil(t, val)
	assert.Contains(t, err.Error(), `partition key "p1"`)

	// normal read
	rc.CreateIfNotExists(ctx, testInfo, map[string]dosa.FieldValue{
		"p1": dosa.FieldValue("data"),
		"c1": dosa.FieldValue(int64(1)),
		"c2": dosa.FieldValue(float64(2)),
	})

	val, err = rc.Read(ctx, testInfo, map[string]dosa.FieldValue{
		"p1": dosa.FieldValue("data"),
	}, []string{"c1"})
	assert.NoError(t, err)
	assert.Equal(t, int64(1), val["c1"])

	// read all fields
	val, err = rc.Read(ctx, testInfo, map[string]dosa.FieldValue{
		"p1": dosa.FieldValue("data"),
	}, dosa.All())
	assert.NoError(t, err)
	assert.Equal(t, int64(1), val["c1"])
	assert.Equal(t, float64(2), val["c2"])
	assert.Equal(t, "data", val["p1"])

	// read a key that isn't there
	val, err = rc.Read(ctx, testInfo, map[string]dosa.FieldValue{
		"p1": dosa.FieldValue("not there"),
	}, dosa.All())
	assert.True(t, dosa.ErrorIsNotFound(err))

	// now delete the one that is
	err = rc.Remove(ctx, testInfo, map[string]dosa.FieldValue{
		"p1": dosa.FieldValue("data")})
	assert.NoError(t, err)

	// read the deleted key
	val, err = rc.Read(ctx, testInfo, map[string]dosa.FieldValue{
		"p1": dosa.FieldValue("data")}, dosa.All())
	assert.True(t, dosa.ErrorIsNotFound(err))

	// insert into clustered entity
	id := dosa.NewUUID()

	err = rc.CreateIfNotExists(ctx, clusteredEi, map[string]dosa.FieldValue{
		"f1": dosa.FieldValue("key"),
		"c1": dosa.FieldValue(int64(1)),
		"c2": dosa.FieldValue(float64(1.2)),
		"c7": dosa.FieldValue(id)})
	assert.NoError(t, err)

	// read that row
	val, err = rc.Read(ctx, clusteredEi, map[string]dosa.FieldValue{
		"f1": dosa.FieldValue("key"),
		"c1": dosa.FieldValue(int64(1)),
		"c7": dosa.FieldValue(id)}, dosa.All())
	assert.NoError(t, err)
	assert.Equal(t, dosa.FieldValue(float64(1.2)), val["c2"])

	// and fail a read on a clustered key
	_, err = rc.Read(ctx, clusteredEi, map[string]dosa.FieldValue{
		"f1": dosa.FieldValue("key"),
		"c1": dosa.FieldValue(int64(2)),
		"c7": dosa.FieldValue(id)}, dosa.All())
	assert.True(t, dosa.ErrorIsNotFound(err))

	plugin := func(scope, namePrefix, opName string) (string, string, error) {
		return "", "", errors.New("dummy errors")
	}
	rc.PluginFunc = plugin
	// not exist scope, use default
	val, err = rc.Read(ctx, testInfo, map[string]dosa.FieldValue{
		"p1": dosa.FieldValue("data"),
	}, []string{"c1"})
	assert.Contains(t, err.Error(), "dummy errors")
}

func TestMultiRead(t *testing.T) {
	connectorMap := getConnectorMap()
	rc := NewRoutingConnector(cfg, connectorMap, nil)

	// normal multi-read
	rc.CreateIfNotExists(ctx, testInfoRandom, map[string]dosa.FieldValue{
		"p1": dosa.FieldValue("data"),
		"c1": dosa.FieldValue(int64(1)),
		"c2": dosa.FieldValue(float64(2)),
	})

	testMultiValues := []map[string]dosa.FieldValue{
		{
			"p1": dosa.FieldValue("data"),
		},
		{
			"c1": dosa.FieldValue(int64(1)),
		},
	}
	_, err := rc.MultiRead(ctx, testInfoRandom, testMultiValues, dosa.All())
	assert.NoError(t, err)

	plugin := func(scope, namePrefix, opName string) (string, string, error) {
		return "", "", errors.New("dummy errors")
	}
	rc.PluginFunc = plugin
	// not exist scope, use default
	_, err = rc.MultiRead(ctx, testInfoRandom, testMultiValues, dosa.All())
	assert.Contains(t, err.Error(), "dummy errors")
}

func TestRoutingConnector_Upsert(t *testing.T) {
	connectorMap := getConnectorMap()
	rc := NewRoutingConnector(cfg, connectorMap, nil)

	// no key value specified
	err := rc.Upsert(ctx, testInfo, map[string]dosa.FieldValue{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `partition key "p1"`)

	testUUIDs := make([]dosa.UUID, 10)
	for x := 0; x < 10; x++ {
		testUUIDs[x] = dosa.NewUUID()
	}

	// first, insert 10 random UUID values into same partition key
	for x := 0; x < 10; x++ {
		err = rc.Upsert(ctx, clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue("data"),
			"c1": dosa.FieldValue(int64(1)),
			"c7": dosa.FieldValue(testUUIDs[x])})
		assert.NoError(t, err)
	}

	// attempt to insert them all again
	for x := 0; x < 10; x++ {
		err := rc.Upsert(ctx, clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue("data"),
			"c1": dosa.FieldValue(int64(1)),
			"c6": dosa.FieldValue(int32(x)),
			"c7": dosa.FieldValue(testUUIDs[x])})
		assert.NoError(t, err)
	}

	// now, insert them again, but this time with a different secondary key
	for x := 0; x < 10; x++ {
		err := rc.Upsert(ctx, clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue("data"),
			"c1": dosa.FieldValue(int64(2)),
			"c7": dosa.FieldValue(testUUIDs[x])})
		assert.NoError(t, err)
	}

	// and with a different primary key
	for x := 0; x < 10; x++ {
		err := rc.Upsert(ctx, clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue("different"),
			"c1": dosa.FieldValue(int64(1)),
			"c7": dosa.FieldValue(testUUIDs[x])})
		assert.NoError(t, err)
	}
	data, token, err := rc.Range(ctx, clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
	}, dosa.All(), "", 200)
	assert.NoError(t, err)
	assert.Empty(t, token)
	assert.Len(t, data, 20)
	assert.NotNil(t, data[0]["c6"])

	plugin := func(scope, namePrefix, opName string) (string, string, error) {
		return "", "", errors.New("dummy errors")
	}
	rc.PluginFunc = plugin
	// not exist scope, use default
	err = rc.Upsert(ctx, testInfo, map[string]dosa.FieldValue{})
	assert.Contains(t, err.Error(), "dummy errors")
}

func TestRoutingConnector_MultiUpsert(t *testing.T) {
	connectorMap := getConnectorMap()
	rc := NewRoutingConnector(cfg, connectorMap, nil)

	// normal multi-upsert
	testMultiValues := []map[string]dosa.FieldValue{
		{
			"p1": dosa.FieldValue("data"),
		},
		{
			"c1": dosa.FieldValue(int64(1)),
		},
	}

	_, err := rc.MultiUpsert(ctx, testInfoRandom, testMultiValues)
	assert.NoError(t, err)

	plugin := func(scope, namePrefix, opName string) (string, string, error) {
		return "", "", errors.New("dummy errors")
	}
	rc.PluginFunc = plugin
	// not exist scope, use default
	_, err = rc.MultiUpsert(ctx, testInfoRandom, testMultiValues)
	assert.Contains(t, err.Error(), "dummy errors")
}

func TestRoutingConnector_Remove(t *testing.T) {
	connectorMap := getConnectorMap()
	rc := NewRoutingConnector(cfg, connectorMap, nil)

	// remove with no data
	err := rc.Remove(ctx, testInfo, map[string]dosa.FieldValue{
		"p1": dosa.FieldValue("data")})
	assert.NoError(t, err)

	// create a single row
	err = rc.CreateIfNotExists(ctx, testInfo, map[string]dosa.FieldValue{
		"p1": dosa.FieldValue("data"),
		"c1": dosa.FieldValue(int64(1)),
		"c2": dosa.FieldValue(float64(2)),
	})
	assert.NoError(t, err)

	// remove something not there
	err = rc.Remove(ctx, testInfo, map[string]dosa.FieldValue{
		"p1": dosa.FieldValue("nothere")})
	assert.NoError(t, err)

	// insert into clustered entity
	id := dosa.NewUUID()
	err = rc.CreateIfNotExists(ctx, clusteredEi, map[string]dosa.FieldValue{
		"f1": dosa.FieldValue("key"),
		"c1": dosa.FieldValue(int64(1)),
		"c2": dosa.FieldValue(float64(1.2)),
		"c7": dosa.FieldValue(id)})
	assert.NoError(t, err)

	// remove something not there, but matches partition
	err = rc.Remove(ctx, clusteredEi, map[string]dosa.FieldValue{
		"f1": dosa.FieldValue("key"),
		"c1": dosa.FieldValue(int64(1)),
		"c7": dosa.FieldValue(dosa.NewUUID())})
	assert.NoError(t, err)

	// and remove the partitioned value
	err = rc.Remove(ctx, clusteredEi, map[string]dosa.FieldValue{
		"f1": dosa.FieldValue("key"),
		"c1": dosa.FieldValue(int64(1)),
		"c7": dosa.FieldValue(id)})
	assert.NoError(t, err)

	// remove it again, now that there's nothing at all there (corner case)
	err = rc.Remove(ctx, clusteredEi, map[string]dosa.FieldValue{
		"f1": dosa.FieldValue("key"),
		"c1": dosa.FieldValue(int64(1)),
		"c7": dosa.FieldValue(id)})
	assert.NoError(t, err)

	plugin := func(scope, namePrefix, opName string) (string, string, error) {
		return "", "", errors.New("dummy errors")
	}
	rc.PluginFunc = plugin
	// not exist scope, use default
	err = rc.Remove(ctx, testInfo, map[string]dosa.FieldValue{
		"p1": dosa.FieldValue("nothere")})
	assert.Contains(t, err.Error(), "dummy errors")
}

func TestRoutingConnector_RemoveRange(t *testing.T) {
	connectorMap := getConnectorMap()
	rc := NewRoutingConnector(cfg, connectorMap, nil)

	// test removing a range with no data in the range
	err := rc.RemoveRange(ctx, clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
	})
	assert.NoError(t, err)

	// insert some data all into the data partition, spread out among the c1 clustering key
	for x := 0; x < idcount; x++ {
		err := rc.CreateIfNotExists(ctx, clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue("data"),
			"c1": dosa.FieldValue(int64(x)),
			"c7": dosa.FieldValue(dosa.NewUUID())})
		assert.NoError(t, err)
	}

	// remove with missing primary key values
	err = rc.RemoveRange(ctx, clusteredEi, map[string][]*dosa.Condition{
		"c1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
		"c7": {{Op: dosa.Gt, Value: dosa.FieldValue(int64(4))}},
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "f1")
	assert.Contains(t, err.Error(), "missing")

	// delete all values greater than those with 4 for c1
	err = rc.RemoveRange(ctx, clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
		"c1": {{Op: dosa.Gt, Value: dosa.FieldValue(int64(4))}},
	})
	assert.NoError(t, err)

	// ensure all the rows with c1 value less than or equal to 4 still exist
	data, _, err := rc.Range(ctx, clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
		"c1": {{Op: dosa.LtOrEq, Value: dosa.FieldValue(int64(4))}},
	}, dosa.All(), "", 200)
	assert.NoError(t, err)
	assert.Len(t, data, idcount/2)
	for i, x := range data {
		assert.Equal(t, x["c1"], int64(i))
	}

	// ensure all the with a c1 value greater than 4 are deleted.
	data, _, err = rc.Range(ctx, clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
		"c1": {{Op: dosa.Gt, Value: dosa.FieldValue(int64(4))}},
	}, dosa.All(), "", 200)
	assert.NoError(t, err)
	assert.Empty(t, data)

	// remove everything but the highest value
	err = rc.RemoveRange(ctx, clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
		"c1": {{Op: dosa.Gt, Value: dosa.FieldValue(int64(0))}},
	})
	assert.NoError(t, err)

	// there should only be one value left now
	data, _, err = rc.Range(ctx, clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
	}, dosa.All(), "", 200)
	assert.NoError(t, err)
	assert.Len(t, data, 1)

	// test completely deleting all the rows in a partition.
	err = rc.RemoveRange(ctx, clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
	})
	assert.NoError(t, err)
	data, _, err = rc.Range(ctx, clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
	}, dosa.All(), "", 200)
	assert.NoError(t, err)
	assert.Empty(t, data)

	plugin := func(scope, namePrefix, opName string) (string, string, error) {
		return "", "", errors.New("dummy errors")
	}
	rc.PluginFunc = plugin
	// not exist scope, use default
	err = rc.RemoveRange(ctx, clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
		"c1": {{Op: dosa.Gt, Value: dosa.FieldValue(int64(0))}},
	})
	assert.Contains(t, err.Error(), "dummy errors")
}

func TestRoutingConnector_MultiRemove(t *testing.T) {
	connectorMap := getConnectorMap()
	rc := NewRoutingConnector(cfg, connectorMap, nil)

	// normal multi-upsert
	testMultiValues := []map[string]dosa.FieldValue{
		{
			"p1": dosa.FieldValue("data"),
		},
		{
			"c1": dosa.FieldValue(int64(1)),
		},
	}

	errs, err := rc.MultiRemove(ctx, testInfoRandom, testMultiValues)
	assert.NoError(t, err)
	assert.NotNil(t, errs)

	plugin := func(scope, namePrefix, opName string) (string, string, error) {
		return "", "", errors.New("dummy errors")
	}
	rc.PluginFunc = plugin
	// not exist scope, use default
	errs, err = rc.MultiRemove(ctx, testInfoRandom, testMultiValues)
	assert.Contains(t, err.Error(), "dummy errors")
}

type ByUUID []dosa.UUID

func (u ByUUID) Len() int           { return len(u) }
func (u ByUUID) Swap(i, j int)      { u[i], u[j] = u[j], u[i] }
func (u ByUUID) Less(i, j int) bool { return string(u[i]) > string(u[j]) }

func TestRoutingConnector_Range(t *testing.T) {
	connectorMap := getConnectorMap()
	rc := NewRoutingConnector(cfg, connectorMap, nil)

	// no data at all (corner case)
	data, token, err := rc.Range(ctx, clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
	}, dosa.All(), "", 200)
	assert.NoError(t, err)
	assert.Empty(t, token)
	assert.Empty(t, data)

	// insert some data into data/1/uuid with a random set of uuids
	// we insert them in a random order
	testUUIDs := make([]dosa.UUID, idcount)
	for x := 0; x < idcount; x++ {
		testUUIDs[x] = dosa.NewUUID()
	}
	for x := 0; x < idcount; x++ {
		err := rc.CreateIfNotExists(ctx, clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue("data"),
			"c1": dosa.FieldValue(int64(1)),
			"c7": dosa.FieldValue(testUUIDs[x])})
		assert.NoError(t, err)
	}

	// search using a different partition key
	data, token, err = rc.Range(ctx, clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("wrongdata")}},
	}, dosa.All(), "", 200)
	assert.NoError(t, err)
	assert.Empty(t, data)

	sort.Sort(ByUUID(testUUIDs))
	// search using the right partition key, and check that the data was insertion-sorted
	// correctly
	data, _, _ = rc.Range(ctx, clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
		"c1": {{Op: dosa.Eq, Value: dosa.FieldValue(int64(1))}},
	}, dosa.All(), "", 200)
	for idx, row := range data {
		assert.Equal(t, testUUIDs[idx], row["c7"])
	}

	// find the midpoint and look for all values greater than that
	data, token, err = rc.Range(ctx, clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
		"c1": {{Op: dosa.Eq, Value: dosa.FieldValue(int64(1))}},
		"c7": {{Op: dosa.Gt, Value: dosa.FieldValue(testUUIDs[idcount/2-1])}},
	}, dosa.All(), "", 200)
	assert.NoError(t, err)
	assert.Len(t, data, idcount/2-1)

	// there's one more for greater than or equal
	data, token, err = rc.Range(ctx, clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
		"c1": {{Op: dosa.Eq, Value: dosa.FieldValue(int64(1))}},
		"c7": {{Op: dosa.GtOrEq, Value: dosa.FieldValue(testUUIDs[idcount/2-1])}},
	}, dosa.All(), "", 200)
	assert.NoError(t, err)
	assert.Len(t, data, idcount/2)

	// find the midpoint and look for all values less than that
	data, token, err = rc.Range(ctx, clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
		"c1": {{Op: dosa.Eq, Value: dosa.FieldValue(int64(1))}},
		"c7": {{Op: dosa.Lt, Value: dosa.FieldValue(testUUIDs[idcount/2])}},
	}, dosa.All(), "", 200)
	assert.NoError(t, err)
	assert.Len(t, data, idcount/2-1)

	// and same for less than or equal
	data, token, err = rc.Range(ctx, clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
		"c1": {{Op: dosa.Eq, Value: dosa.FieldValue(int64(1))}},
		"c7": {{Op: dosa.LtOrEq, Value: dosa.FieldValue(testUUIDs[idcount/2])}},
	}, dosa.All(), "", 200)
	assert.NoError(t, err)
	assert.Len(t, data, idcount/2)

	// look off the end of the left side, so greater than maximum (edge case)
	// (uuids are ordered descending so this is non-intuitively backwards)
	data, token, err = rc.Range(ctx, clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
		"c1": {{Op: dosa.Eq, Value: dosa.FieldValue(int64(1))}},
		"c7": {{Op: dosa.Gt, Value: dosa.FieldValue(testUUIDs[0])}},
	}, dosa.All(), "", 200)
	assert.NoError(t, err)
	assert.Empty(t, data)

	// look off the end of the left side, so greater than maximum
	data, _, err = rc.Range(ctx, clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
		"c1": {{Op: dosa.Eq, Value: dosa.FieldValue(int64(1))}},
		"c7": {{Op: dosa.Lt, Value: dosa.FieldValue(testUUIDs[idcount-1])}},
	}, dosa.All(), "", 200)
	assert.NoError(t, err)
	assert.Empty(t, data)

	// Test Ranging on an Index

	// Get "1" partition
	data, token, err = rc.Range(ctx, clusteredEi, map[string][]*dosa.Condition{
		"c1": {{Op: dosa.Eq, Value: dosa.FieldValue(int64(1))}},
	}, dosa.All(), "", 200)
	assert.NoError(t, err)
	assert.Len(t, data, 10)
	assert.Empty(t, token)

	// Get the "2" partition, should be empty
	data, token, err = rc.Range(ctx, clusteredEi, map[string][]*dosa.Condition{
		"c1": {{Op: dosa.Eq, Value: dosa.FieldValue(int64(2))}},
	}, dosa.All(), "", 200)
	assert.NoError(t, err)
	assert.Empty(t, data)
	assert.Empty(t, token)

	plugin := func(scope, namePrefix, opName string) (string, string, error) {
		return "", "", errors.New("dummy errors")
	}
	rc.PluginFunc = plugin
	// not exist scope, use default
	data, token, err = rc.Range(ctx, clusteredEi, map[string][]*dosa.Condition{
		"c1": {{Op: dosa.Eq, Value: dosa.FieldValue(int64(1))}},
	}, dosa.All(), "", 200)
	assert.Contains(t, err.Error(), "dummy errors")
}

func TestRoutingConnector_Search(t *testing.T) {
	connectorMap := getConnectorMap()
	rc := NewRoutingConnector(cfg, connectorMap, nil)

	vals, _, err := rc.Search(ctx, testInfoRandom, testPairs, []string{"c5"}, "", 32)
	assert.NotNil(t, vals)
	assert.NoError(t, err)

	plugin := func(scope, namePrefix, opName string) (string, string, error) {
		return "", "", errors.New("dummy errors")
	}
	rc.PluginFunc = plugin
	// not exist scope, use default
	vals, _, err = rc.Search(ctx, testInfoRandom, testPairs, []string{"c5"}, "", 32)
	assert.Contains(t, err.Error(), "dummy errors")
}

func TestRoutingConnector_Scan(t *testing.T) {
	connectorMap := getConnectorMap()
	rc := NewRoutingConnector(cfg, connectorMap, nil)

	testUUIDs := make([]dosa.UUID, idcount)
	for x := 0; x < idcount; x++ {
		testUUIDs[x] = dosa.NewUUID()
	}
	// scan with nothing there yet
	_, token, err := rc.Scan(ctx, clusteredEi, dosa.All(), "", 100)
	assert.NoError(t, err)
	assert.Empty(t, token)

	// first, insert some random UUID values into two partition keys
	for x := 0; x < idcount; x++ {
		err := rc.Upsert(ctx, clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue("data" + string(x%2)),
			"c1": dosa.FieldValue(int64(1)),
			"c7": dosa.FieldValue(testUUIDs[x])})
		assert.NoError(t, err)
	}

	data, token, err := rc.Scan(ctx, clusteredEi, dosa.All(), "", 100)
	assert.NoError(t, err)
	assert.Len(t, data, idcount)
	assert.Empty(t, token)

	// there's an odd edge case when you delete everything, so do that, then call scan
	for x := 0; x < idcount; x++ {
		err := rc.Remove(ctx, clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue("data" + string(x%2)),
			"c1": dosa.FieldValue(int64(1)),
			"c7": dosa.FieldValue(testUUIDs[x])})
		assert.NoError(t, err)
	}
	data, token, err = rc.Scan(ctx, clusteredEi, dosa.All(), "", 100)
	assert.NoError(t, err)
	assert.Empty(t, data)
	assert.Empty(t, token)

	plugin := func(scope, namePrefix, opName string) (string, string, error) {
		return "", "", errors.New("dummy errors")
	}
	rc.PluginFunc = plugin
	// not exist scope, use default
	data, token, err = rc.Scan(ctx, clusteredEi, dosa.All(), "", 100)
	assert.Contains(t, err.Error(), "dummy errors")
}

func TestRoutingConnector_Shutdown(t *testing.T) {
	connectorMap := getConnectorMap()
	rc := NewRoutingConnector(cfg, connectorMap, nil)

	err := rc.Shutdown()
	assert.NoError(t, err)
}

func TestRoutingConnector_TimeUUIDs(t *testing.T) {
	connectorMap := getConnectorMap()
	rc := NewRoutingConnector(cfg, connectorMap, nil)

	// insert a bunch of values with V1 timestamps as clustering keys
	for x := 0; x < idcount; x++ {
		err := rc.Upsert(ctx, clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue("data"),
			"c1": dosa.FieldValue(int64(1)),
			"c6": dosa.FieldValue(int32(x)),
			"c7": dosa.FieldValue(dosa.UUID(uuid.NewV1().String()))})
		assert.NoError(t, err)
	}

	// read them back, they should be in reverse order
	data, _, _ := rc.Range(ctx, clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
		"c1": {{Op: dosa.Eq, Value: dosa.FieldValue(int64(1))}},
	}, dosa.All(), "", 200)

	// check that the order is backwards
	for idx, row := range data {
		assert.Equal(t, int32(idcount-idx-1), row["c6"])
	}

	// now mix in a few V4 UUIDs
	for x := 0; x < idcount; x++ {
		err := rc.Upsert(ctx, clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue("data"),
			"c1": dosa.FieldValue(int64(1)),
			"c6": dosa.FieldValue(int32(idcount + x)),
			"c7": dosa.FieldValue(dosa.NewUUID())})
		assert.NoError(t, err)
	}

	// the V4's should all be first, since V4 UUIDs sort > V1 UUIDs
	data, _, _ = rc.Range(ctx, clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
		"c1": {{Op: dosa.Eq, Value: dosa.FieldValue(int64(1))}},
	}, dosa.All(), "", 200)
	for _, row := range data[0:idcount] {
		assert.True(t, row["c6"].(int32) >= idcount, row["c6"])
	}
}

func TestRoutingConnector_ScanWithToken(t *testing.T) {
	connectorMap := getConnectorMap()
	rc := NewRoutingConnector(cfg, connectorMap, nil)

	createTestData(t, rc, func(id int) string {
		return "data" + string(id%3)
	}, idcount)
	var token string
	var err error
	var data []map[string]dosa.FieldValue
	for x := 0; x < idcount; x++ {
		data, token, err = rc.Scan(ctx, clusteredEi, dosa.All(), token, 1)
		assert.NoError(t, err)
		assert.Len(t, data, 1)
		if x < idcount-1 {
			assert.NotEmpty(t, token)
		} else {
			assert.Empty(t, token)
		}
	}
	// now walk through again, but delete the item returned
	// (note: we don't have to reset token, because it should be empty now)
	for x := 0; x < idcount; x++ {
		data, token, err = rc.Scan(ctx, clusteredEi, dosa.All(), token, 1)
		assert.NoError(t, err)
		assert.Len(t, data, 1)
		if x < idcount-1 {
			assert.NotEmpty(t, token)
		} else {
			assert.Empty(t, token)
		}
		err = rc.Remove(ctx, clusteredEi, data[0])
		assert.NoError(t, err)
	}
}

func TestRoutingConnector_ScanWithTokenFromWrongTable(t *testing.T) {
	connectorMap := getConnectorMap()
	rc := NewRoutingConnector(cfg, connectorMap, nil)

	createTestData(t, rc, func(id int) string {
		return "data" + string(id%3)
	}, idcount)
	err := rc.Upsert(ctx, testInfo, map[string]dosa.FieldValue{
		"p1": dosa.FieldValue("test"),
	})
	assert.NoError(t, err)

	// get a token from one table
	_, token, err := rc.Scan(ctx, clusteredEi, dosa.All(), "", 1)
	assert.NoError(t, err)
	assert.NotEmpty(t, token)

	// now use it for another scan (oops)
	_, _, err = rc.Scan(ctx, testInfo, dosa.All(), token, 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Invalid token")
	assert.Contains(t, err.Error(), "Missing value")
}

func TestRoutingConnector_ScanWithTokenNoClustering(t *testing.T) {
	connectorMap := getConnectorMap()
	rc := NewRoutingConnector(cfg, connectorMap, nil)

	for x := 0; x < idcount; x++ {
		rc.Upsert(ctx, testInfo, map[string]dosa.FieldValue{
			"p1": dosa.FieldValue("data" + string(x)),
		})
	}
	var token string
	var err error
	var data []map[string]dosa.FieldValue
	for x := 0; x < idcount; x++ {
		data, token, err = rc.Scan(ctx, testInfo, dosa.All(), token, 1)
		assert.NoError(t, err)
		assert.Len(t, data, 1)
		if x < idcount-1 {
			assert.NotEmpty(t, token)
		} else {
			assert.Empty(t, token)
		}
	}
	// and again, this time deleting as we go
	for x := 0; x < idcount; x++ {
		data, token, err = rc.Scan(ctx, testInfo, dosa.All(), token, 1)
		assert.NoError(t, err)
		assert.Len(t, data, 1)
		if x < idcount-1 {
			assert.NotEmpty(t, token)
		} else {
			assert.Empty(t, token)
		}
		err = rc.Remove(ctx, testInfo, data[0])
		assert.NoError(t, err)
	}
}

func TestRangePager(t *testing.T) {
	connectorMap := getConnectorMap()
	rc := NewRoutingConnector(cfg, connectorMap, nil)

	// create test data in one partition "data"
	createTestData(t, rc, func(_ int) string { return "data" }, idcount)
	var token string
	var err error
	var data []map[string]dosa.FieldValue
	for x := 0; x < idcount; x++ {
		data, token, err = rc.Range(ctx, clusteredEi, map[string][]*dosa.Condition{
			"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
			"c1": {{Op: dosa.Eq, Value: dosa.FieldValue(int64(1))}},
		}, dosa.All(), token, 1)
		assert.NoError(t, err)
		assert.Len(t, data, 1)
		if x < idcount-1 {
			assert.NotEmpty(t, token)
		} else {
			assert.Empty(t, token)
		}
	}
	// now walk through again, but delete the item returned
	// (note: we don't have to reset token, because it should be empty now)
	for x := 0; x < idcount; x++ {
		data, token, err = rc.Range(ctx, clusteredEi, map[string][]*dosa.Condition{
			"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
			"c1": {{Op: dosa.Eq, Value: dosa.FieldValue(int64(1))}},
		}, dosa.All(), token, 1)
		assert.NoError(t, err)
		assert.Len(t, data, 1)
		if x < idcount-1 {
			assert.NotEmpty(t, token)
		} else {
			assert.Empty(t, token)
		}
		err = rc.Remove(ctx, clusteredEi, data[0])
		assert.NoError(t, err)
	}
}

func TestInvalidToken(t *testing.T) {
	connectorMap := getConnectorMap()
	rc := NewRoutingConnector(cfg, connectorMap, nil)

	// we don't use the token if there's no data that matches, so lets
	// create one row
	createTestData(t, rc, func(id int) string {
		return "data"
	}, 1)

	token := "this is not a token and not a hot dog"
	t.Run("testInvalidTokenRange", func(t *testing.T) {
		_, _, err := rc.Range(ctx, clusteredEi, map[string][]*dosa.Condition{
			"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
			"c1": {{Op: dosa.Eq, Value: dosa.FieldValue(int64(1))}},
		}, dosa.All(), token, 1)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Invalid token")
	})
	t.Run("testInvalidTokenScan", func(t *testing.T) {
		_, _, err := rc.Scan(ctx, clusteredEi, dosa.All(), token, 1)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Invalid token")
	})
}

func TestRoutingConnector_RangeWithBadCriteria(t *testing.T) {
	connectorMap := getConnectorMap()
	rc := NewRoutingConnector(cfg, connectorMap, nil)

	// we don't look at the criteria unless there is at least one row
	createTestData(t, rc, func(id int) string {
		return "data"
	}, 1)

	_, _, err := rc.Range(ctx, clusteredEi, map[string][]*dosa.Condition{
		"c2": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
		"c3": {{Op: dosa.Eq, Value: dosa.FieldValue(int64(1))}},
	}, dosa.All(), "", 1)
	assert.Error(t, err)
}

// createTestData populates some test data. The keyGenFunc can either return a constant,
// which gives you a single partition of data, or some function of the current offset, which
// will scatter the data across different partition keys
func createTestData(t *testing.T, rc *RoutingConnector, keyGenFunc func(int) string, idcount int) {
	// insert a bunch of values with V1 timestamps as clustering keys
	for x := 0; x < idcount; x++ {
		err := rc.Upsert(ctx, clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue(keyGenFunc(x)),
			"c1": dosa.FieldValue(int64(1)),
			"c6": dosa.FieldValue(int32(x)),
			"c7": dosa.FieldValue(dosa.UUID(uuid.NewV1().String()))})
		assert.NoError(t, err)
	}
}
