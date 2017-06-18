package cassandra_test

import (
	"context"
	"testing"

	"code.uber.internal/infra/dosa-gateway/datastore/common"
	gouuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
)

const pageSize = 5

var (
	strKeys = []string{"aa", "bb", "cc"}
)

func TestRangeQuery(t *testing.T) {
	partitionKey := dosa.UUID(gouuid.NewV4().String())
	populateEntityRange(t, partitionKey)

	var (
		res   []map[string]dosa.FieldValue
		token string
		err   error
	)
	// range query with paging for entire partition
	for i := range strKeys {
		res, token, err = testStore.Range(context.TODO(), testEntityInfo, map[string][]*dosa.Condition{
			uuidKeyField: {{Op: dosa.Eq, Value: partitionKey}},
		}, []string{uuidKeyField, stringKeyField, int64KeyField, int32Field}, token, pageSize)
		assert.NoError(t, err)
		assert.Len(t, res, pageSize)
		for j, row := range res {
			assert.Equal(t, partitionKey, row[uuidKeyField])
			// res ordered by strKey ASC
			assert.Equal(t, strKeys[i], row[stringKeyField])
			// res ordered by int64Key DESC
			assert.Equal(t, int64(pageSize-j-1), row[int64KeyField])
			assert.Contains(t, row, int32Field)
			assert.EqualValues(t, row[int64KeyField], row[int32Field])
		}
	}

	// range query with constraints on first clustering key
	res, token, err = testStore.Range(context.TODO(), testEntityInfo, map[string][]*dosa.Condition{
		uuidKeyField:   {{Op: dosa.Eq, Value: partitionKey}},
		stringKeyField: {{Op: dosa.Gt, Value: strKeys[0]}, {Op: dosa.Lt, Value: strKeys[2]}}, // "aa" < strKey < "cc"
	}, []string{uuidKeyField, stringKeyField, int64KeyField, int32Field}, "", pageSize*2)
	assert.NoError(t, err)
	assert.Len(t, res, pageSize) // limit == pageSize * 2 but we only have pageSize rows in range
	assert.Empty(t, token)
	for i, row := range res {
		assert.Equal(t, partitionKey, row[uuidKeyField])
		assert.Equal(t, strKeys[1], row[stringKeyField])
		// res ordered by int64Key DESC
		assert.Equal(t, int64(pageSize-i-1), row[int64KeyField])
		assert.Contains(t, row, int32Field)
		assert.EqualValues(t, row[int64KeyField], row[int32Field])
	}

	// range query with constraints for all clustering keys
	res, token, err = testStore.Range(context.TODO(), testEntityInfo, map[string][]*dosa.Condition{
		uuidKeyField:   {{Op: dosa.Eq, Value: partitionKey}},
		stringKeyField: {{Op: dosa.Eq, Value: strKeys[0]}},
		int64KeyField:  {{Op: dosa.GtOrEq, Value: int64(1)}, {Op: dosa.LtOrEq, Value: int64(3)}},
	}, []string{uuidKeyField, stringKeyField, int64KeyField, int32Field}, "", pageSize*2)
	assert.NoError(t, err)
	assert.Len(t, res, 3) // limit == pageSize * 2 but we only have 3 rows in range
	assert.Empty(t, token)
	for i, row := range res {
		assert.Equal(t, partitionKey, row[uuidKeyField])
		assert.Equal(t, strKeys[0], row[stringKeyField])
		// res ordered by int64Key DESC
		assert.Equal(t, int64(3-i), row[int64KeyField])
		assert.Contains(t, row, int32Field)
		assert.EqualValues(t, row[int64KeyField], row[int32Field])
	}
}

func TestRangeQueryInvalidToken(t *testing.T) {
	_, _, err := testStore.Range(context.TODO(), testEntityInfo, map[string][]*dosa.Condition{
		uuidKeyField: {{Op: dosa.Eq, Value: dosa.UUID(gouuid.NewV4().String())}},
	}, []string{int32Field}, "西瓜", pageSize)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "bad token")
	assert.True(t, common.IsBadTokenErr(err))
}

func TestRangeQueryFieldsToRead(t *testing.T) {
	partitionKey := dosa.UUID(gouuid.NewV4().String())
	populateEntityRange(t, partitionKey)

	res, token, err := testStore.Range(context.TODO(), testEntityInfo, map[string][]*dosa.Condition{
		uuidKeyField:   {{Op: dosa.Eq, Value: partitionKey}},
		stringKeyField: {{Op: dosa.Eq, Value: strKeys[0]}},
		int64KeyField:  {{Op: dosa.GtOrEq, Value: int64(1)}, {Op: dosa.LtOrEq, Value: int64(3)}},
	}, nil, "", pageSize*2)
	assert.NoError(t, err)
	assert.Empty(t, token)
	assert.Len(t, res, 3)
	for _, row := range res {
		// should include all columns in result
		for _, c := range testEntityInfo.Def.Columns {
			assert.Contains(t, row, c.Name)
		}
	}
}

// as Scan shares doCommonQuery code path with Range, we'll skip invalid token test and fieldsToRead test
func TestScan(t *testing.T) {
	sp := "datastore_scan_test"
	entityInfo := newTestEntityInfo(sp)
	// has to be done in separate keyspace in order to avoid interference from other tests
	err := initTestSchema(sp, entityInfo)
	if !assert.NoError(t, err) {
		assert.FailNow(t, "failed to create scope for scan test", err.Error())
	}

	defer removeTestSchema(sp)

	expectedUUIDSet := make(map[dosa.UUID]struct{})
	expectedIntValueSet := make(map[int]struct{})

	for i := 0; i < 100; i++ {
		id := dosa.UUID(gouuid.NewV4().String())
		expectedUUIDSet[id] = struct{}{}
		expectedIntValueSet[i] = struct{}{}
		err := testStore.Upsert(context.TODO(), entityInfo, map[string]dosa.FieldValue{
			uuidKeyField:   id,
			stringKeyField: "apple",
			int64KeyField:  int64(i),
			int32Field:     int32(i),
		})
		if !assert.NoError(t, err) {
			assert.FailNow(t, "failed to populate entities for scan test", err)
		}
	}

	actualUUIDSet := make(map[dosa.UUID]struct{})
	actualIntValueSet := make(map[int]struct{})
	var (
		res   []map[string]dosa.FieldValue
		token string
	)
	for {
		res, token, err = testStore.Scan(context.TODO(), entityInfo, []string{uuidKeyField, int32Field}, token, 39)
		assert.NoError(t, err)
		for _, row := range res {
			actualUUIDSet[row[uuidKeyField].(dosa.UUID)] = struct{}{}
			actualIntValueSet[int(row[int32Field].(int32))] = struct{}{}
		}
		t.Log("token: ", token)
		if len(token) == 0 {
			break
		}
	}

	assert.Equal(t, expectedUUIDSet, actualUUIDSet)
	assert.Equal(t, expectedIntValueSet, actualIntValueSet)
}

func populateEntityRange(t *testing.T, uuid dosa.UUID) {
	for _, strKey := range strKeys {
		for i := 0; i < pageSize; i++ {
			err := testStore.Upsert(context.TODO(), testEntityInfo, map[string]dosa.FieldValue{
				uuidKeyField:   uuid,
				stringKeyField: strKey,
				int64KeyField:  int64(i),
				int32Field:     int32(i),
			})
			if !assert.NoError(t, err) {
				assert.FailNow(t, "failed to populate entity range", err)
			}
		}
	}
}
