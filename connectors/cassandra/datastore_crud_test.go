package cassandra_test

import (
	"context"
	"testing"
	"time"

	"code.uber.internal/infra/dosa-gateway/datastore/common"
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
	id := constructKeys(dosa.UUID(gouuid.NewV4().String()))
	_, err := testStore.Read(context.TODO(), testEntityInfo, id, []string{int32Field})
	assert.Error(t, err)
	assert.IsType(t, common.ErrNotFound{}, errors.Cause(err))
}

func TestUpsertAndRead(t *testing.T) {
	uuid := dosa.UUID(gouuid.NewV4().String())
	values := constructFullValues(uuid)
	err := testStore.Upsert(context.TODO(), testEntityInfo, values)
	assert.NoError(t, err)

	id := constructKeys(uuid)
	allValueFields := []string{int32Field, doubleField, blobField, boolField, timestampField, int64Field, stringField, uuidField}
	readRes, err := testStore.Read(context.TODO(), testEntityInfo, id, allValueFields)
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

	err = testStore.Upsert(context.TODO(), testEntityInfo, pu)
	assert.NoError(t, err)
	err = testStore.Upsert(context.TODO(), testEntityInfo, pc2)
	assert.NoError(t, err)

	// check updated int32 field
	updated, err := testStore.Read(context.TODO(), testEntityInfo, id, []string{int32Field})
	assert.NoError(t, err)
	assert.Len(t, updated, 1)
	t.Log(updated)
	assert.Equal(t, updated[int32Field], int32(-100))

	// read second object with nil `fieldsToRead`
	id2 := constructKeys(uuid2)
	res, err := testStore.Read(context.TODO(), testEntityInfo, id2, nil)
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
	uuid := dosa.UUID(gouuid.NewV4().String())
	values := constructFullValues(uuid)
	err := testStore.CreateIfNotExists(context.TODO(), testEntityInfo, values)
	assert.NoError(t, err)

	id := constructKeys(uuid)
	allValueFields := []string{int32Field, doubleField, blobField, boolField, timestampField, int64Field, stringField, uuidField}
	readRes, err := testStore.Read(context.TODO(), testEntityInfo, id, allValueFields)
	assert.NoError(t, err)
	assert.Equal(t, len(allValueFields), len(readRes))
	for _, field := range allValueFields {
		assert.Equal(t, values[field], readRes[field])
	}

	// should fail if already exists
	err = testStore.CreateIfNotExists(context.TODO(), testEntityInfo, values)
	assert.Error(t, err)
	assert.IsType(t, common.ErrAlreadyExists{}, err)
}

func TestDelete(t *testing.T) {
	uuid1 := dosa.UUID(gouuid.NewV4().String())
	id1 := constructKeys(uuid1)
	v1 := constructFullValues(uuid1)

	err := testStore.Upsert(context.TODO(), testEntityInfo, v1)
	assert.NoError(t, err)

	_, err = testStore.Read(context.TODO(), testEntityInfo, id1, []string{int32Field})
	assert.NoError(t, err)

	err = testStore.Remove(context.TODO(), testEntityInfo, id1)
	assert.NoError(t, err)

	_, err = testStore.Read(context.TODO(), testEntityInfo, id1, []string{int32Field})
	assert.Error(t, err)
	assert.IsType(t, common.ErrNotFound{}, err)

	// no-op
	err = testStore.Remove(context.TODO(), testEntityInfo, id1)
	assert.NoError(t, err)
}

func TestRemoveRange(t *testing.T) {
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
	err := testStore.RemoveRange(context.TODO(), testEntityInfo, conds1)

	// Upsert all values
	err = testStore.Upsert(context.TODO(), testEntityInfo, v1)
	assert.NoError(t, err)

	err = testStore.Upsert(context.TODO(), testEntityInfo, v2)
	assert.NoError(t, err)

	err = testStore.Upsert(context.TODO(), testEntityInfo, v3)
	assert.NoError(t, err)

	// Remove Values larger than 200
	conds2 := map[string][]*dosa.Condition{
		uuidKeyField:   {&dosa.Condition{Op: dosa.Eq, Value: uuid1}},
		stringKeyField: {&dosa.Condition{Op: dosa.Eq, Value: defaultStringKeyValue}},
		int64KeyField:  {&dosa.Condition{Op: dosa.Gt, Value: 2}},
	}
	err = testStore.RemoveRange(context.TODO(), testEntityInfo, conds2)
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
	err = testStore.RemoveRange(context.TODO(), testEntityInfo, conds4)
	assert.NoError(t, err)

	// Ensure the 200 value is still in the range. It's the only value that should be left.
	conds5 := map[string][]*dosa.Condition{
		uuidKeyField:   {&dosa.Condition{Op: dosa.Eq, Value: uuid1}},
		stringKeyField: {&dosa.Condition{Op: dosa.Eq, Value: defaultStringKeyValue}},
	}

	res, _, err = testStore.Range(context.TODO(), testEntityInfo, conds5, dosa.All(), "", 5)
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
