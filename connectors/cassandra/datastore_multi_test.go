package cassandra_test

//
//import (
//	"context"
//	"testing"
//
//	"code.uber.internal/infra/dosa-gateway/datastore"
//	gouuid "github.com/satori/go.uuid"
//	"github.com/stretchr/testify/assert"
//	"github.com/uber-go/dosa"
//)
//
//func TestMultiRead(t *testing.T) {
//	keys := []map[string]dosa.FieldValue{}
//
//	taskNum := 50
//	for i := 0; i < taskNum; i++ {
//		id := dosa.UUID(gouuid.NewV4().String())
//		keys = append(keys, constructKeys(id))
//		v := map[string]dosa.FieldValue{
//			uuidKeyField:   id,
//			stringKeyField: defaultStringKeyValue,
//			int64KeyField:  defaultInt64KeyValue,
//			int32Field:     int32(i),
//		}
//		err := testStore.Upsert(context.TODO(), testEntityInfo, v)
//		if !assert.NoError(t, err) {
//			assert.FailNow(t, "failed to set up for multiread test", err.Error())
//		}
//	}
//
//	badKey := constructKeys(dosa.UUID(gouuid.NewV4().String()))
//	keys = append(keys, badKey)
//
//	multiResult, err := testStore.MultiRead(context.TODO(), testEntityInfo, keys, []string{int32Field})
//	assert.NoError(t, err)
//	assert.Len(t, multiResult, len(keys))
//
//	for i := 0; i < taskNum; i++ {
//		res := multiResult[i]
//		if assert.NotNil(t, res) {
//			assert.NoError(t, res.Error)
//			assert.Len(t, res.Values, 1)
//			assert.Equal(t, int32(i), res.Values[int32Field])
//		}
//	}
//
//	// should be not found
//	notFoundRes := multiResult[taskNum]
//	assert.Nil(t, notFoundRes.Values)
//	assert.True(t, datastore.IsNotFoundErr(notFoundRes.Error))
//}
//
//func TestMultiUpsertAndMultiRemove(t *testing.T) {
//	keys := []map[string]dosa.FieldValue{}
//	values := []map[string]dosa.FieldValue{}
//
//	taskNum := 50
//	for i := 0; i < taskNum; i++ {
//		id := dosa.UUID(gouuid.NewV4().String())
//		keys = append(keys, constructKeys(id))
//		v := map[string]dosa.FieldValue{
//			uuidKeyField:   id,
//			stringKeyField: defaultStringKeyValue,
//			int64KeyField:  defaultInt64KeyValue,
//			int32Field:     int32(i),
//		}
//		values = append(values, v)
//	}
//
//	multiErrs, err := testStore.MultiUpsert(context.TODO(), testEntityInfo, values)
//	assert.NoError(t, err)
//	assert.Len(t, multiErrs, len(keys))
//	for _, err := range multiErrs {
//		assert.NoError(t, err)
//	}
//
//	multiResult, err := testStore.MultiRead(context.TODO(), testEntityInfo, keys, []string{int32Field})
//	assert.NoError(t, err)
//	assert.Len(t, multiResult, len(keys))
//
//	for i := 0; i < taskNum; i++ {
//		res := multiResult[i]
//		if assert.NotNil(t, res) {
//			assert.NoError(t, res.Error)
//			assert.Len(t, res.Values, 1)
//			assert.Equal(t, int32(i), res.Values[int32Field])
//		}
//	}
//
//	multiErrs, err = testStore.MultiRemove(context.TODO(), testEntityInfo, keys)
//	assert.NoError(t, err)
//	assert.Len(t, multiErrs, len(keys))
//	for _, err := range multiErrs {
//		assert.NoError(t, err)
//	}
//
//	multiResult, err = testStore.MultiRead(context.TODO(), testEntityInfo, keys, []string{int32Field})
//	assert.NoError(t, err)
//	assert.Len(t, multiResult, len(keys))
//	for _, result := range multiResult {
//		if assert.NotNil(t, result) {
//			assert.True(t, datastore.IsNotFoundErr(result.Error))
//		}
//	}
//}
//
//func TestPanicRover(t *testing.T) {
//	// intentionall pass in bad args to cause panic in spawned goroutings
//	var badContext context.Context
//
//	id := dosa.UUID(gouuid.NewV4().String())
//	k := constructKeys(id)
//	v := map[string]dosa.FieldValue{
//		uuidKeyField:   id,
//		stringKeyField: defaultStringKeyValue,
//		int64KeyField:  defaultInt64KeyValue,
//		int32Field:     int32(100),
//	}
//
//	_, err := testStore.MultiRead(badContext, testEntityInfo, []map[string]dosa.FieldValue{k}, nil)
//	if assert.Error(t, err) {
//		assert.Contains(t, err.Error(), "gorouting paniced")
//	}
//
//	_, err = testStore.MultiUpsert(badContext, testEntityInfo, []map[string]dosa.FieldValue{v})
//	if assert.Error(t, err) {
//		assert.Contains(t, err.Error(), "gorouting paniced")
//	}
//
//	_, err = testStore.MultiRemove(badContext, testEntityInfo, []map[string]dosa.FieldValue{k})
//	if assert.Error(t, err) {
//		assert.Contains(t, err.Error(), "gorouting paniced")
//	}
//}
