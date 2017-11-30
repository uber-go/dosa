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
	"time"

	"github.com/pkg/errors"
	gouuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
)

var (
	defaultStringKeyValue       = "apple"
	defaultInt64KeyValue  int64 = 100
)

func TestReadNotFound(t *testing.T) {
	sut := GetTestConnector(t)
	id := constructKeys(dosa.UUID(gouuid.NewV4().String()))
	_, err := sut.Read(context.TODO(), testEntityInfo, id, []string{int32Field})
	assert.Error(t, err)
	assert.IsType(t, &dosa.ErrNotFound{}, errors.Cause(err), err.Error())
}

func TestReadTimeout(t *testing.T) {
	sut := GetTestConnector(t)
	id := constructKeys(dosa.UUID(gouuid.NewV4().String()))
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()
	_, err := sut.Read(ctx, testEntityInfo, id, []string{int32Field})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context deadline exceeded")
}

func TestUpsertAndRead(t *testing.T) {
	sut := GetTestConnector(t)
	uuid := dosa.UUID(gouuid.NewV4().String())
	values := constructFullValues(uuid)
	err := sut.Upsert(context.TODO(), testEntityInfo, values)
	assert.NoError(t, err)

	id := constructKeys(uuid)
	allValueFields := []string{int32Field, doubleField, blobField, boolField, timestampField, int64Field, stringField, uuidField}
	readRes, err := sut.Read(context.TODO(), testEntityInfo, id, allValueFields)
	assert.NoError(t, err)
	assert.Equal(t, len(allValueFields), len(readRes))
	for _, field := range allValueFields {
		assert.Equal(t, values[field], readRes[field])
	}

	// partial update first object
	pu := map[string]dosa.FieldValue{
		uuidKeyField:   uuid,
		stringKeyField: defaultStringKeyValue,
		int64KeyField:  defaultInt64KeyValue,
		int32Field:     int32(-100), // update to -100
	}

	// partial create second object
	uuid2 := dosa.UUID(gouuid.NewV4().String())
	pc2 := map[string]dosa.FieldValue{
		uuidKeyField:   uuid2,
		stringKeyField: defaultStringKeyValue,
		int64KeyField:  defaultInt64KeyValue,
		int32Field:     int32(-100),
	}

	err = sut.Upsert(context.TODO(), testEntityInfo, pu)
	assert.NoError(t, err)
	err = sut.Upsert(context.TODO(), testEntityInfo, pc2)
	assert.NoError(t, err)

	// check updated int32 field
	updated, err := sut.Read(context.TODO(), testEntityInfo, id, []string{int32Field})
	assert.NoError(t, err)
	assert.Len(t, updated, 1)
	assert.Equal(t, updated[int32Field], int32(-100))

	// read second object with nil `fieldsToRead`
	id2 := constructKeys(uuid2)
	res, err := sut.Read(context.TODO(), testEntityInfo, id2, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(allValueFields), len(res)) // should read all non-key fields
	expectedPartialValues := map[string]dosa.FieldValue{
		int32Field: int32(-100),
		// the following are all zero values
		int64Field:     int64(0),
		doubleField:    float64(0.0),
		blobField:      []byte{},
		boolField:      false,
		timestampField: time.Time{},
		stringField:    "",
		uuidField:      dosa.UUID("00000000-0000-0000-0000-000000000000"),
	}
	assert.Equal(t, expectedPartialValues, res)
}

func TestCreateIfNotExists(t *testing.T) {
	sut := GetTestConnector(t)
	uuid := dosa.UUID(gouuid.NewV4().String())
	values := constructFullValues(uuid)
	err := sut.CreateIfNotExists(context.TODO(), testEntityInfo, values)
	assert.NoError(t, err)

	id := constructKeys(uuid)
	allValueFields := []string{int32Field, doubleField, blobField, boolField, timestampField, int64Field, stringField, uuidField}
	readRes, err := sut.Read(context.TODO(), testEntityInfo, id, allValueFields)
	assert.NoError(t, err)
	assert.Equal(t, len(allValueFields), len(readRes))
	for _, field := range allValueFields {
		assert.Equal(t, values[field], readRes[field])
	}

	// should fail if already exists
	err = sut.CreateIfNotExists(context.TODO(), testEntityInfo, values)
	assert.Error(t, err)
	assert.IsType(t, &dosa.ErrAlreadyExists{}, err)
}

func TestDelete(t *testing.T) {
	sut := GetTestConnector(t)
	uuid1 := dosa.UUID(gouuid.NewV4().String())
	id1 := constructKeys(uuid1)
	v1 := constructFullValues(uuid1)

	err := sut.Upsert(context.TODO(), testEntityInfo, v1)
	assert.NoError(t, err)

	_, err = sut.Read(context.TODO(), testEntityInfo, id1, []string{int32Field})
	assert.NoError(t, err)

	err = sut.Remove(context.TODO(), testEntityInfo, id1)
	assert.NoError(t, err)

	_, err = sut.Read(context.TODO(), testEntityInfo, id1, []string{int32Field})
	assert.Error(t, err)
	assert.IsType(t, &dosa.ErrNotFound{}, err)

	// no-op
	err = sut.Remove(context.TODO(), testEntityInfo, id1)
	assert.NoError(t, err)
}

func TestRemoveRange(t *testing.T) {
	sut := GetTestConnector(t)
	uuid1 := dosa.UUID(gouuid.NewV4().String())
	v1 := constructFullValues(uuid1)
	v1[int64KeyField] = 1
	v2 := constructFullValues(uuid1)
	v2[int64KeyField] = 2
	v3 := constructFullValues(uuid1)
	v3[int64KeyField] = 3

	// Test RemoveRange with no entities in range
	conds1 := map[string][]*dosa.Condition{
		uuidKeyField:   {&dosa.Condition{Op: dosa.Eq, Value: uuid1}},
		stringKeyField: {&dosa.Condition{Op: dosa.Eq, Value: defaultStringKeyValue}},
	}
	err := sut.RemoveRange(context.TODO(), testEntityInfo, conds1)

	// Upsert all values
	err = sut.Upsert(context.TODO(), testEntityInfo, v1)
	assert.NoError(t, err)

	err = sut.Upsert(context.TODO(), testEntityInfo, v2)
	assert.NoError(t, err)

	err = sut.Upsert(context.TODO(), testEntityInfo, v3)
	assert.NoError(t, err)

	// Remove Values larger than 200
	conds2 := map[string][]*dosa.Condition{
		uuidKeyField:   {&dosa.Condition{Op: dosa.Eq, Value: uuid1}},
		stringKeyField: {&dosa.Condition{Op: dosa.Eq, Value: defaultStringKeyValue}},
		int64KeyField:  {&dosa.Condition{Op: dosa.Gt, Value: 2}},
	}
	err = sut.RemoveRange(context.TODO(), testEntityInfo, conds2)
	assert.NoError(t, err)

	// Ensure 100 and 200 are still in the range
	conds3 := map[string][]*dosa.Condition{
		uuidKeyField:   {&dosa.Condition{Op: dosa.Eq, Value: uuid1}},
		stringKeyField: {&dosa.Condition{Op: dosa.Eq, Value: defaultStringKeyValue}},
	}

	res, _, err := testStore.Range(context.TODO(), testEntityInfo, conds3, dosa.All(), "", 5)
	assert.NoError(t, err)
	assert.Len(t, res, 2)
	assert.Equal(t, int64(2), res[0][int64KeyField])
	assert.Equal(t, int64(1), res[1][int64KeyField])

	// Remove Values less than 200
	conds4 := map[string][]*dosa.Condition{
		uuidKeyField:   {&dosa.Condition{Op: dosa.Eq, Value: uuid1}},
		stringKeyField: {&dosa.Condition{Op: dosa.Eq, Value: defaultStringKeyValue}},
		int64KeyField:  {&dosa.Condition{Op: dosa.Lt, Value: 2}},
	}
	err = sut.RemoveRange(context.TODO(), testEntityInfo, conds4)
	assert.NoError(t, err)

	// Ensure the 200 value is still in the range. It's the only value that should be left.
	conds5 := map[string][]*dosa.Condition{
		uuidKeyField:   {&dosa.Condition{Op: dosa.Eq, Value: uuid1}},
		stringKeyField: {&dosa.Condition{Op: dosa.Eq, Value: defaultStringKeyValue}},
	}

	res, _, err = sut.Range(context.TODO(), testEntityInfo, conds5, dosa.All(), "", 5)
	assert.NoError(t, err)
	assert.Len(t, res, 1)
	assert.Equal(t, int64(2), res[0][int64KeyField])
}

func constructFullValues(uuid dosa.UUID) map[string]dosa.FieldValue {
	return map[string]dosa.FieldValue{
		uuidKeyField:   uuid,
		stringKeyField: defaultStringKeyValue,
		int64KeyField:  defaultInt64KeyValue,
		int32Field:     int32(100),
		int64Field:     int64(100),
		doubleField:    float64(100.0),
		blobField:      []byte("blob"),
		boolField:      true,
		// TODO: store timestamp using int64 or else declare we only support ms resolution
		// Anything smaller than ms is lost.
		timestampField: time.Unix(100, 111000000).UTC(),
		stringField:    "appleV",
		uuidField:      dosa.UUID(gouuid.NewV4().String()),
	}
}

func constructKeys(uuid dosa.UUID) map[string]dosa.FieldValue {
	return map[string]dosa.FieldValue{
		uuidKeyField:   uuid,
		int64KeyField:  defaultInt64KeyValue,
		stringKeyField: defaultStringKeyValue,
	}
}
