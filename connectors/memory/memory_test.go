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

package memory

import (
	"context"
	"testing"
	"time"

	"sort"

	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
)

var testSchemaRef = dosa.SchemaRef{
	Scope:      "scope1",
	NamePrefix: "namePrefix",
	EntityName: "eName",
	Version:    12345,
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
			{Name: "c7", Type: dosa.TUUID},
		},
		Key: &dosa.PrimaryKey{
			PartitionKeys: []string{"f1"},
		},
		Name: "t1",
	},
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
		Name: "t1",
	},
}

func TestConnector_CreateIfNotExists(t *testing.T) {
	sut := Connector{}

	err := sut.CreateIfNotExists(context.TODO(), testEi, map[string]dosa.FieldValue{
		"f1": dosa.FieldValue("data"),
	})
	assert.NoError(t, err)

	err = sut.CreateIfNotExists(context.TODO(), testEi, map[string]dosa.FieldValue{
		"f1": dosa.FieldValue("data"),
	})

	assert.Error(t, err)
	assert.True(t, dosa.ErrorIsAlreadyExists(err))
}
func TestConnector_Upsert(t *testing.T) {
	sut := Connector{}

	err := sut.Upsert(context.TODO(), testEi, map[string]dosa.FieldValue{
		"f1": dosa.FieldValue("data"),
	})
	assert.NoError(t, err)
	vals, err := sut.Read(context.TODO(), testEi, map[string]dosa.FieldValue{
		"f1": dosa.FieldValue("data")}, []string{"c1"})
	assert.NoError(t, err)
	assert.Nil(t, vals["c1"])

	err = sut.Upsert(context.TODO(), testEi, map[string]dosa.FieldValue{
		"f1": dosa.FieldValue("data"),
		"c1": dosa.FieldValue(int64(1)),
	})
	assert.NoError(t, err)

	vals, err = sut.Read(context.TODO(), testEi, map[string]dosa.FieldValue{
		"f1": dosa.FieldValue("data")}, []string{"c1"})
	assert.NoError(t, err)
	assert.Equal(t, dosa.FieldValue(int64(1)), vals["c1"])
}

func TestConnector_Read(t *testing.T) {
	sut := Connector{}

	// read with no data
	vals, err := sut.Read(context.TODO(), testEi, map[string]dosa.FieldValue{
		"f1": dosa.FieldValue("data")}, []string{"c1"})
	assert.Error(t, err)
	assert.True(t, dosa.ErrorIsNotFound(err))

	err = sut.CreateIfNotExists(context.TODO(), testEi, map[string]dosa.FieldValue{
		"f1": dosa.FieldValue("data"),
		"c1": dosa.FieldValue(int64(1)),
		"c2": dosa.FieldValue(float64(2)),
	})
	assert.NoError(t, err)

	vals, err = sut.Read(context.TODO(), testEi, map[string]dosa.FieldValue{
		"f1": dosa.FieldValue("data")}, []string{"c1"})
	assert.NoError(t, err)
	assert.Equal(t, int64(1), vals["c1"])
	assert.Nil(t, vals["c2"])
	assert.Equal(t, 1, len(vals))

	vals, err = sut.Read(context.TODO(), testEi, map[string]dosa.FieldValue{
		"f1": dosa.FieldValue("data")}, dosa.All())
	assert.NoError(t, err)
	assert.Equal(t, int64(1), vals["c1"])
	assert.Equal(t, float64(2), vals["c2"])
	assert.Equal(t, "data", vals["f1"])

	// read a key that isn't there
	vals, err = sut.Read(context.TODO(), testEi, map[string]dosa.FieldValue{
		"f1": dosa.FieldValue("not there")}, dosa.All())
	assert.Error(t, err)
	assert.True(t, dosa.ErrorIsNotFound(err))

	// now delete the one that is
	err = sut.Remove(context.TODO(), testEi, map[string]dosa.FieldValue{
		"f1": dosa.FieldValue("data")})
	assert.NoError(t, err)

	// read the deleted key
	vals, err = sut.Read(context.TODO(), testEi, map[string]dosa.FieldValue{
		"f1": dosa.FieldValue("data")}, dosa.All())
	assert.Error(t, err)
	assert.True(t, dosa.ErrorIsNotFound(err))

	// insert into clustered entity
	id := dosa.NewUUID()
	err = sut.CreateIfNotExists(context.TODO(), clusteredEi, map[string]dosa.FieldValue{
		"f1": dosa.FieldValue("key"),
		"c1": dosa.FieldValue(int64(1)),
		"c2": dosa.FieldValue(float64(1.2)),
		"c7": dosa.FieldValue(id)})
	assert.NoError(t, err)

	// read that row
	vals, err = sut.Read(context.TODO(), clusteredEi, map[string]dosa.FieldValue{
		"f1": dosa.FieldValue("key"),
		"c1": dosa.FieldValue(int64(1)),
		"c7": dosa.FieldValue(id)}, dosa.All())
	assert.NoError(t, err)
	assert.Equal(t, dosa.FieldValue(float64(1.2)), vals["c2"])

	// and fail a read on a clustered key
	vals, err = sut.Read(context.TODO(), clusteredEi, map[string]dosa.FieldValue{
		"f1": dosa.FieldValue("key"),
		"c1": dosa.FieldValue(int64(2)),
		"c7": dosa.FieldValue(id)}, dosa.All())
	assert.Error(t, err)
	assert.True(t, dosa.ErrorIsNotFound(err))
}

func TestConnector_Remove(t *testing.T) {
	sut := Connector{}

	// remove with no data
	err := sut.Remove(context.TODO(), testEi, map[string]dosa.FieldValue{
		"f1": dosa.FieldValue("data")})
	assert.Error(t, err)
	assert.True(t, dosa.ErrorIsNotFound(err))

	// create a single row
	err = sut.CreateIfNotExists(context.TODO(), testEi, map[string]dosa.FieldValue{
		"f1": dosa.FieldValue("data"),
		"c1": dosa.FieldValue(int64(1)),
		"c2": dosa.FieldValue(float64(2)),
	})
	assert.NoError(t, err)

	// remove something not there
	err = sut.Remove(context.TODO(), testEi, map[string]dosa.FieldValue{
		"f1": dosa.FieldValue("nothere")})
	assert.Error(t, err)
	assert.True(t, dosa.ErrorIsNotFound(err))

	// insert into clustered entity
	id := dosa.NewUUID()
	err = sut.CreateIfNotExists(context.TODO(), clusteredEi, map[string]dosa.FieldValue{
		"f1": dosa.FieldValue("key"),
		"c1": dosa.FieldValue(int64(1)),
		"c2": dosa.FieldValue(float64(1.2)),
		"c7": dosa.FieldValue(id)})
	assert.NoError(t, err)

	// remove something not there, but matches partition
	err = sut.Remove(context.TODO(), clusteredEi, map[string]dosa.FieldValue{
		"f1": dosa.FieldValue("key"),
		"c1": dosa.FieldValue(int64(1)),
		"c7": dosa.FieldValue(dosa.NewUUID())})
	assert.Error(t, err)

	// and remove the partitioned value
	err = sut.Remove(context.TODO(), clusteredEi, map[string]dosa.FieldValue{
		"f1": dosa.FieldValue("key"),
		"c1": dosa.FieldValue(int64(1)),
		"c7": dosa.FieldValue(id)})
	assert.NoError(t, err)

	// remove it again, now that there's nothing at all there (corner case)
	err = sut.Remove(context.TODO(), clusteredEi, map[string]dosa.FieldValue{
		"f1": dosa.FieldValue("key"),
		"c1": dosa.FieldValue(int64(1)),
		"c7": dosa.FieldValue(id)})
	assert.Error(t, err)
}

func TestConnector_Shutdown(t *testing.T) {
	sut := Connector{}

	err := sut.Shutdown()
	assert.NoError(t, err)
	assert.Nil(t, sut.data)
}

// test CreateIfNotExists with partitioning
func TestConnector_CreateIfNotExists2(t *testing.T) {
	sut := Connector{}

	testUUIDs := make([]dosa.UUID, 10)
	for x := 0; x < 10; x++ {
		testUUIDs[x] = dosa.NewUUID()
	}

	// first, insert 10 random UUID values into same partition key
	for x := 0; x < 10; x++ {
		err := sut.CreateIfNotExists(context.TODO(), clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue("data"),
			"c1": dosa.FieldValue(int64(1)),
			"c7": dosa.FieldValue(testUUIDs[x])})
		assert.NoError(t, err)
	}
	// attempt to insert them all again
	for x := 0; x < 10; x++ {
		err := sut.CreateIfNotExists(context.TODO(), clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue("data"),
			"c1": dosa.FieldValue(int64(1)),
			"c7": dosa.FieldValue(testUUIDs[x])})
		assert.Error(t, err)
		assert.True(t, dosa.ErrorIsAlreadyExists(err))
	}
	// now, insert them again, but this time with a different secondary key
	for x := 0; x < 10; x++ {
		err := sut.CreateIfNotExists(context.TODO(), clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue("data"),
			"c1": dosa.FieldValue(int64(2)),
			"c7": dosa.FieldValue(testUUIDs[x])})
		assert.NoError(t, err)
	}
	// and with a different primary key
	for x := 0; x < 10; x++ {
		err := sut.CreateIfNotExists(context.TODO(), clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue("different"),
			"c1": dosa.FieldValue(int64(1)),
			"c7": dosa.FieldValue(testUUIDs[x])})
		assert.NoError(t, err)
	}
	data, token, err := sut.Range(context.TODO(), clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
	}, dosa.All(), "", 200)
	assert.NoError(t, err)
	assert.Empty(t, token)
	assert.Equal(t, 20, len(data))
}

func TestConnector_Upsert2(t *testing.T) {
	sut := Connector{}

	testUUIDs := make([]dosa.UUID, 10)
	for x := 0; x < 10; x++ {
		testUUIDs[x] = dosa.NewUUID()
	}

	// first, insert 10 random UUID values into same partition key
	for x := 0; x < 10; x++ {
		err := sut.Upsert(context.TODO(), clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue("data"),
			"c1": dosa.FieldValue(int64(1)),
			"c7": dosa.FieldValue(testUUIDs[x])})
		assert.NoError(t, err)
	}
	// attempt to insert them all again
	for x := 0; x < 10; x++ {
		err := sut.Upsert(context.TODO(), clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue("data"),
			"c1": dosa.FieldValue(int64(1)),
			"c6": dosa.FieldValue(int32(x)),
			"c7": dosa.FieldValue(testUUIDs[x])})
		assert.NoError(t, err)
	}
	// now, insert them again, but this time with a different secondary key
	for x := 0; x < 10; x++ {
		err := sut.Upsert(context.TODO(), clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue("data"),
			"c1": dosa.FieldValue(int64(2)),
			"c7": dosa.FieldValue(testUUIDs[x])})
		assert.NoError(t, err)
	}
	// and with a different primary key
	for x := 0; x < 10; x++ {
		err := sut.Upsert(context.TODO(), clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue("different"),
			"c1": dosa.FieldValue(int64(1)),
			"c7": dosa.FieldValue(testUUIDs[x])})
		assert.NoError(t, err)
	}
	data, token, err := sut.Range(context.TODO(), clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
	}, dosa.All(), "", 200)
	assert.NoError(t, err)
	assert.Empty(t, token)
	assert.Equal(t, 20, len(data))
	assert.NotNil(t, data[0]["c6"])
}

func TestConnector_Range(t *testing.T) {
	const idcount = 10
	sut := Connector{}

	// no data at all (corner case)
	data, token, err := sut.Range(context.TODO(), clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
	}, dosa.All(), "", 200)
	assert.Error(t, err)
	assert.True(t, dosa.ErrorIsNotFound(err))
	assert.Empty(t, token)
	assert.Empty(t, data)

	// insert some data into data/1/uuid with a random set of uuids
	// we insert them in a random order
	testUUIDs := make([]dosa.UUID, idcount)
	for x := 0; x < idcount; x++ {
		testUUIDs[x] = dosa.NewUUID()
	}
	for x := 0; x < idcount; x++ {
		err := sut.CreateIfNotExists(context.TODO(), clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue("data"),
			"c1": dosa.FieldValue(int64(1)),
			"c7": dosa.FieldValue(testUUIDs[x])})
		assert.NoError(t, err)
	}

	// search using a different partition key
	data, token, err = sut.Range(context.TODO(), clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("wrongdata")}},
	}, dosa.All(), "", 200)
	assert.Error(t, err)
	assert.True(t, dosa.ErrorIsNotFound(err))

	sort.Sort(ByUUID(testUUIDs))
	// search using the right partition key, and check that the data was insertion-sorted
	// correctly
	data, _, _ = sut.Range(context.TODO(), clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
		"c1": {{Op: dosa.Eq, Value: dosa.FieldValue(int64(1))}},
	}, dosa.All(), "", 200)
	for idx, row := range data {
		assert.Equal(t, testUUIDs[idx], row["c7"])
	}

	// find the midpoint and look for all values greater than that
	data, token, err = sut.Range(context.TODO(), clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
		"c1": {{Op: dosa.Eq, Value: dosa.FieldValue(int64(1))}},
		"c7": {{Op: dosa.Gt, Value: dosa.FieldValue(testUUIDs[idcount/2-1])}},
	}, dosa.All(), "", 200)
	assert.NoError(t, err)
	assert.Equal(t, idcount/2-1, len(data))

	// there's one more for greater than or equal
	data, token, err = sut.Range(context.TODO(), clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
		"c1": {{Op: dosa.Eq, Value: dosa.FieldValue(int64(1))}},
		"c7": {{Op: dosa.GtOrEq, Value: dosa.FieldValue(testUUIDs[idcount/2-1])}},
	}, dosa.All(), "", 200)
	assert.NoError(t, err)
	assert.Equal(t, idcount/2, len(data))

	// find the midpoint and look for all values less than that
	data, token, err = sut.Range(context.TODO(), clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
		"c1": {{Op: dosa.Eq, Value: dosa.FieldValue(int64(1))}},
		"c7": {{Op: dosa.Lt, Value: dosa.FieldValue(testUUIDs[idcount/2])}},
	}, dosa.All(), "", 200)
	assert.NoError(t, err)
	assert.Equal(t, idcount/2-1, len(data))

	// and same for less than or equal
	data, token, err = sut.Range(context.TODO(), clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
		"c1": {{Op: dosa.Eq, Value: dosa.FieldValue(int64(1))}},
		"c7": {{Op: dosa.LtOrEq, Value: dosa.FieldValue(testUUIDs[idcount/2])}},
	}, dosa.All(), "", 200)
	assert.NoError(t, err)
	assert.Equal(t, idcount/2, len(data))

	// look off the end of the left side, so greater than maximum (edge case)
	// (uuids are ordered descending so this is non-intuitively backwards)
	data, token, err = sut.Range(context.TODO(), clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
		"c1": {{Op: dosa.Eq, Value: dosa.FieldValue(int64(1))}},
		"c7": {{Op: dosa.Gt, Value: dosa.FieldValue(testUUIDs[0])}},
	}, dosa.All(), "", 200)
	assert.Error(t, err)
	assert.True(t, dosa.ErrorIsNotFound(err))

	// look off the end of the left side, so greater than maximum
	data, token, err = sut.Range(context.TODO(), clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
		"c1": {{Op: dosa.Eq, Value: dosa.FieldValue(int64(1))}},
		"c7": {{Op: dosa.Lt, Value: dosa.FieldValue(testUUIDs[idcount-1])}},
	}, dosa.All(), "", 200)
	assert.Error(t, err)
	assert.True(t, dosa.ErrorIsNotFound(err))
}

func TestConnector_TimeUUIDs(t *testing.T) {
	sut := Connector{}
	const idcount = 10

	// insert a bunch of values with V1 timestamps as clustering keys
	for x := 0; x < idcount; x++ {
		err := sut.Upsert(context.TODO(), clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue("data"),
			"c1": dosa.FieldValue(int64(1)),
			"c6": dosa.FieldValue(int32(x)),
			"c7": dosa.FieldValue(dosa.UUID(uuid.NewV1().String()))})
		assert.NoError(t, err)
	}

	// read them back, they should be in reverse order
	data, _, _ := sut.Range(context.TODO(), clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
		"c1": {{Op: dosa.Eq, Value: dosa.FieldValue(int64(1))}},
	}, dosa.All(), "", 200)

	// check that the order is backwards
	for idx, row := range data {
		assert.Equal(t, int32(idcount-idx-1), row["c6"])
	}

	// now mix in a few V4 UUIDs
	for x := 0; x < idcount; x++ {
		err := sut.Upsert(context.TODO(), clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue("data"),
			"c1": dosa.FieldValue(int64(1)),
			"c6": dosa.FieldValue(int32(idcount + x)),
			"c7": dosa.FieldValue(dosa.NewUUID())})
		assert.NoError(t, err)
	}

	// the V4's should all be first, since V4 UUIDs sort > V1 UUIDs
	data, _, _ = sut.Range(context.TODO(), clusteredEi, map[string][]*dosa.Condition{
		"f1": {{Op: dosa.Eq, Value: dosa.FieldValue("data")}},
		"c1": {{Op: dosa.Eq, Value: dosa.FieldValue(int64(1))}},
	}, dosa.All(), "", 200)
	for _, row := range data[0:idcount] {
		assert.True(t, row["c6"].(int32) >= idcount, row["c6"])
	}

}

type ByUUID []dosa.UUID

func (u ByUUID) Len() int           { return len(u) }
func (u ByUUID) Swap(i, j int)      { u[i], u[j] = u[j], u[i] }
func (u ByUUID) Less(i, j int) bool { return string(u[i]) > string(u[j]) }

func BenchmarkConnector_CreateIfNotExists(b *testing.B) {
	sut := Connector{}
	for x := 0; x < b.N; x++ {
		id := dosa.NewUUID()
		err := sut.CreateIfNotExists(context.TODO(), clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue("key"),
			"c1": dosa.FieldValue(int64(1)),
			"c7": dosa.FieldValue(id)})
		assert.NoError(b, err)
		if x%1000 == 0 {
			sut.data = nil
		}
	}
}

func BenchmarkConnector_Read(b *testing.B) {
	const idcount = 100
	testUUIDs := make([]dosa.UUID, idcount)
	for x := 0; x < idcount; x++ {
		testUUIDs[x] = dosa.NewUUID()
	}
	sut := Connector{}
	for x := 0; x < idcount; x++ {
		err := sut.CreateIfNotExists(context.TODO(), clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue("data"),
			"c1": dosa.FieldValue(int64(1)),
			"c7": dosa.FieldValue(testUUIDs[x])})
		assert.NoError(b, err)
	}

	for x := 0; x < b.N; x++ {
		_, err := sut.Read(context.TODO(), clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue("data"),
			"c1": dosa.FieldValue(int64(1)),
			"c7": dosa.FieldValue(testUUIDs[x%idcount])}, dosa.All())
		assert.NoError(b, err)
	}
}

func TestCompareType(t *testing.T) {
	tuuid := dosa.NewUUID()
	v1uuid := dosa.UUID(uuid.NewV1().String())
	v1newer := dosa.UUID(uuid.NewV1().String())
	tests := []struct {
		t1, t2 dosa.FieldValue
		result int8
	}{
		{dosa.FieldValue(int32(1)), dosa.FieldValue(int32(1)), 0},
		{dosa.FieldValue(int64(1)), dosa.FieldValue(int64(1)), 0},
		{dosa.FieldValue("test"), dosa.FieldValue("test"), 0},
		{dosa.FieldValue(time.Time{}), dosa.FieldValue(time.Time{}), 0},
		{dosa.FieldValue(tuuid), dosa.FieldValue(tuuid), 0},
		{dosa.FieldValue(v1uuid), dosa.FieldValue(v1uuid), 0},
		{dosa.FieldValue(false), dosa.FieldValue(false), 0},
		{dosa.FieldValue([]byte{1}), dosa.FieldValue([]byte{1}), 0},
		{dosa.FieldValue(1.0), dosa.FieldValue(1.0), 0},

		{dosa.FieldValue(int32(1)), dosa.FieldValue(int32(2)), -1},
		{dosa.FieldValue(int64(1)), dosa.FieldValue(int64(2)), -1},
		{dosa.FieldValue("test"), dosa.FieldValue("test2"), -1},
		{dosa.FieldValue(time.Time{}), dosa.FieldValue(time.Time{}.Add(time.Duration(1))), -1},
		{dosa.FieldValue(v1uuid), dosa.FieldValue(tuuid), -1},
		{dosa.FieldValue(v1uuid), dosa.FieldValue(v1newer), -1},
		{dosa.FieldValue(false), dosa.FieldValue(true), -1},
		{dosa.FieldValue([]byte{1}), dosa.FieldValue([]byte{2}), -1},
		{dosa.FieldValue(0.9), dosa.FieldValue(1.0), -1},

		{dosa.FieldValue(int32(2)), dosa.FieldValue(int32(1)), 1},
		{dosa.FieldValue(int64(2)), dosa.FieldValue(int64(1)), 1},
		{dosa.FieldValue("test2"), dosa.FieldValue("test"), 1},
		{dosa.FieldValue(time.Time{}.Add(time.Duration(1))), dosa.FieldValue(time.Time{}), 1},
		{dosa.FieldValue(tuuid), dosa.FieldValue(v1uuid), 1},
		{dosa.FieldValue(v1newer), dosa.FieldValue(v1uuid), 1},
		{dosa.FieldValue(true), dosa.FieldValue(false), 1},
		{dosa.FieldValue([]byte{2}), dosa.FieldValue([]byte{1}), 1},
		{dosa.FieldValue(1.1), dosa.FieldValue(1.0), 1},
	}
	for _, test := range tests {
		assert.Equal(t, test.result, compareType(test.t1, test.t2))
	}

	assert.Panics(t, func() { compareType(t, t) })
}

func TestConnector_Scan(t *testing.T) {
	sut := Connector{}
	testUUIDs := make([]dosa.UUID, 10)
	for x := 0; x < 10; x++ {
		testUUIDs[x] = dosa.NewUUID()
	}
	// scan with nothing there yet
	_, token, err := sut.Scan(context.TODO(), clusteredEi, dosa.All(), "", 100)
	assert.Error(t, err)
	assert.True(t, dosa.ErrorIsNotFound(err))
	assert.Empty(t, token)

	// first, insert 10 random UUID values into two partition keys
	for x := 0; x < 10; x++ {
		err := sut.Upsert(context.TODO(), clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue("data" + string(x%2)),
			"c1": dosa.FieldValue(int64(1)),
			"c7": dosa.FieldValue(testUUIDs[x])})
		assert.NoError(t, err)
	}

	data, token, err := sut.Scan(context.TODO(), clusteredEi, dosa.All(), "", 100)
	assert.NoError(t, err)
	assert.Equal(t, 10, len(data))
	assert.Empty(t, token)

	// there's an odd edge case when you delete everything, so do that, then call scan
	for x := 0; x < 10; x++ {
		err := sut.Remove(context.TODO(), clusteredEi, map[string]dosa.FieldValue{
			"f1": dosa.FieldValue("data" + string(x%2)),
			"c1": dosa.FieldValue(int64(1)),
			"c7": dosa.FieldValue(testUUIDs[x])})
		assert.NoError(t, err)
	}
	data, token, err = sut.Scan(context.TODO(), clusteredEi, dosa.All(), "", 100)
	assert.Error(t, err)
	assert.True(t, dosa.ErrorIsNotFound(err))
	assert.Empty(t, token)
}

func TestConstruction(t *testing.T) {
	c, err := dosa.GetConnector("memory", nil)
	assert.NoError(t, err)
	assert.IsType(t, &Connector{}, c)

	v, err := c.CheckSchema(context.TODO(), "dummy", "dummy", nil)
	assert.Equal(t, int32(1), v)
	assert.NoError(t, err)
}

func TestPanics(t *testing.T) {
	assert.Panics(t, func() {
		passCol(dosa.FieldValue(int64(1)), &dosa.Condition{Op: 0, Value: dosa.FieldValue(int64(1))})
	})
}
