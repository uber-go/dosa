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

	gouuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
)

const pageSize = 5

var (
	strKeys = []string{"aa", "bb", "cc"}
)

func TestRangeQuery(t *testing.T) {
	sut := GetTestConnector(t)
	defer ShutdownTestConnector()
	partitionKey := dosa.UUID(gouuid.NewV4().String())
	populateEntityRange(t, partitionKey)

	var (
		res   []map[string]dosa.FieldValue
		token string
		err   error
	)
	// range query with paging for entire partition
	for i := range strKeys {
		res, token, err = sut.Range(context.TODO(), testEntityInfo, map[string][]*dosa.Condition{
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
	res, token, err = sut.Range(context.TODO(), testEntityInfo, map[string][]*dosa.Condition{
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
	res, token, err = sut.Range(context.TODO(), testEntityInfo, map[string][]*dosa.Condition{
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
	sut := GetTestConnector(t)
	defer ShutdownTestConnector()
	_, _, err := sut.Range(context.TODO(), testEntityInfo, map[string][]*dosa.Condition{
		uuidKeyField: {{Op: dosa.Eq, Value: dosa.UUID(gouuid.NewV4().String())}},
	}, []string{int32Field}, "西瓜", pageSize)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "bad token")
}

func TestRangeQueryFieldsToRead(t *testing.T) {
	sut := GetTestConnector(t)
	defer ShutdownTestConnector()
	partitionKey := dosa.UUID(gouuid.NewV4().String())
	populateEntityRange(t, partitionKey)

	res, token, err := sut.Range(context.TODO(), testEntityInfo, map[string][]*dosa.Condition{
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
	sut := GetTestConnector(t)
	defer ShutdownTestConnector()
	sp := "datastore_scan_test"
	entityInfo := newTestEntityInfo(sp)

	expectedUUIDSet := make(map[dosa.UUID]struct{})
	expectedIntValueSet := make(map[int]struct{})

	// remove any entities from any previous test
	for {
		res, _, err := sut.Scan(context.TODO(), entityInfo, []string{uuidKeyField, stringKeyField, int64KeyField}, "", 200)
		if err != nil || len(res) == 0 {
			break
		}
		for _, e := range res {
			_ = sut.Remove(context.TODO(), entityInfo, e)
		}
	}

	for i := 0; i < 100; i++ {
		id := dosa.UUID(gouuid.NewV4().String())
		expectedUUIDSet[id] = struct{}{}
		expectedIntValueSet[i] = struct{}{}
		err := sut.Upsert(context.TODO(), entityInfo, map[string]dosa.FieldValue{
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
		var err error
		res, token, err = sut.Scan(context.TODO(), entityInfo, []string{uuidKeyField, int32Field}, token, 39)
		assert.NoError(t, err)
		for _, row := range res {
			actualUUIDSet[row[uuidKeyField].(dosa.UUID)] = struct{}{}
			actualIntValueSet[int(row[int32Field].(int32))] = struct{}{}
		}
		if len(token) == 0 {
			break
		}
	}

	assert.Equal(t, expectedUUIDSet, actualUUIDSet)
	assert.Equal(t, expectedIntValueSet, actualIntValueSet)
}

func populateEntityRange(t *testing.T, uuid dosa.UUID) {
	sut := GetTestConnector(t)
	defer ShutdownTestConnector()
	for _, strKey := range strKeys {
		for i := 0; i < pageSize; i++ {
			err := sut.Upsert(context.TODO(), testEntityInfo, map[string]dosa.FieldValue{
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
