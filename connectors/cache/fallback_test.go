package cache

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/redis"
	"github.com/uber-go/dosa/mocks"
	"github.com/uber-go/dosa/testentity"
)

var (
	testRedisConfig = redis.Config{
		ServerSettings: redis.ServerConfig{
			Host: "localhost",
			Port: redis.RedisPort,
		},
		TTL: 5 * time.Second,
	}
	redisC    = redis.NewConnector(testRedisConfig)
	schemaRef = dosa.SchemaRef{Scope: "testing", NamePrefix: "example"}
	testEi    = createTestEi(schemaRef)
	adaptedEi = &dosa.EntityInfo{
		Def: &dosa.EntityDefinition{
			Name: "awesome_test_entity",
			Key: &dosa.PrimaryKey{
				PartitionKeys: []string{key},
			},
			Columns: []*dosa.ColumnDefinition{
				{Name: value, Type: dosa.Blob},
				{Name: key, Type: dosa.Blob},
			},
		},
		Ref: &schemaRef,
	}
)

func createTestEi(sr dosa.SchemaRef) *dosa.EntityInfo {
	table, _ := dosa.TableFromInstance(&testentity.TestEntity{})
	testEi := &dosa.EntityInfo{Ref: &sr, Def: &table.EntityDefinition}
	return testEi
}

// Test origin upsert, also upserts to cache
func TestUpsert(t *testing.T) {
	originCtrl := gomock.NewController(t)
	defer originCtrl.Finish()
	mockOrigin := mocks.NewMockConnector(originCtrl)

	fallbackCtrl := gomock.NewController(t)
	defer fallbackCtrl.Finish()
	mockFallback := mocks.NewMockConnector(fallbackCtrl)

	keys := map[string]dosa.FieldValue{}

	mockOrigin.EXPECT().Upsert(context.TODO(), testEi, keys).Return(nil)
	mockFallback.EXPECT().Upsert(context.TODO(), adaptedEi, gomock.Any()).Return(nil)

	connector := NewConnector(mockOrigin, mockFallback, NewJSONEncoder())
	connector.setSynchronousMode(true)
	err := connector.Upsert(context.TODO(), testEi, keys)
	assert.NoError(t, err)
}

// Test read, origin succeeds, should write response to cache
func TestReadSuccess(t *testing.T) {
	originCtrl := gomock.NewController(t)
	defer originCtrl.Finish()
	mockOrigin := mocks.NewMockConnector(originCtrl)

	fallbackCtrl := gomock.NewController(t)
	defer fallbackCtrl.Finish()
	mockFallback := mocks.NewMockConnector(fallbackCtrl)

	values := map[string]dosa.FieldValue{}
	originResponse := map[string]dosa.FieldValue{"a": "b"}
	mockOrigin.EXPECT().Read(context.TODO(), testEi, values, dosa.All()).Return(originResponse, nil)
	mockFallback.EXPECT().Upsert(context.TODO(), adaptedEi, gomock.Any()).Return(nil)

	connector := NewConnector(mockOrigin, mockFallback, NewJSONEncoder())
	connector.setSynchronousMode(true)
	resp, err := connector.Read(context.TODO(), testEi, values, []string{})
	assert.NoError(t, err)
	assert.EqualValues(t, originResponse, resp)
}

// Test read, origin has error, fallback to cache
func TestReadFail(t *testing.T) {
	originCtrl := gomock.NewController(t)
	defer originCtrl.Finish()
	mockOrigin := mocks.NewMockConnector(originCtrl)

	fallbackCtrl := gomock.NewController(t)
	defer fallbackCtrl.Finish()
	mockFallback := mocks.NewMockConnector(fallbackCtrl)

	readValues := map[string]dosa.FieldValue{}
	fallbackResponse := map[string]dosa.FieldValue{"value": []byte("{\"b\": 7}")}
	originErr := errors.New("origin error")
	mockOrigin.EXPECT().Read(context.TODO(), testEi, readValues, dosa.All()).Return(nil, originErr)
	mockFallback.EXPECT().Read(context.TODO(), adaptedEi, gomock.Any(), dosa.All()).Return(fallbackResponse, nil)

	connector := NewConnector(mockOrigin, mockFallback, NewJSONEncoder())
	connector.setSynchronousMode(true)
	resp, err := connector.Read(context.TODO(), testEi, readValues, []string{})
	assert.NoError(t, err)
	assert.EqualValues(t, map[string]dosa.FieldValue{"b": float64(7)}, resp)
}

// Test read, origin has error, fallback to cache fails. Return original results
func TestReadFallbackFail(t *testing.T) {
	originCtrl := gomock.NewController(t)
	defer originCtrl.Finish()
	mockOrigin := mocks.NewMockConnector(originCtrl)

	fallbackCtrl := gomock.NewController(t)
	defer fallbackCtrl.Finish()
	mockFallback := mocks.NewMockConnector(fallbackCtrl)

	readValues := map[string]dosa.FieldValue{}
	originResponse := map[string]dosa.FieldValue{"a": "b"}
	originErr := errors.New("origin error")
	fallbackErr := errors.New("fallback error")
	mockOrigin.EXPECT().Read(context.TODO(), testEi, readValues, dosa.All()).Return(originResponse, originErr)
	mockFallback.EXPECT().Read(context.TODO(), adaptedEi, gomock.Any(), dosa.All()).Return(nil, fallbackErr)

	connector := NewConnector(mockOrigin, mockFallback, NewJSONEncoder())
	connector.setSynchronousMode(true)
	resp, err := connector.Read(context.TODO(), testEi, readValues, []string{})
	assert.EqualError(t, err, originErr.Error())
	assert.Equal(t, originResponse, resp)
}

// Test range, origin succeeds, should write response to cache
func TestRangeSuccess(t *testing.T) {
	originCtrl := gomock.NewController(t)
	defer originCtrl.Finish()
	mockOrigin := mocks.NewMockConnector(originCtrl)

	fallbackCtrl := gomock.NewController(t)
	defer fallbackCtrl.Finish()
	mockFallback := mocks.NewMockConnector(fallbackCtrl)

	rangeResponse := []map[string]dosa.FieldValue{{"a": "b"}}
	rangeTok := "nextToken"
	mockOrigin.EXPECT().Range(context.TODO(), testEi, map[string][]*dosa.Condition{}, dosa.All(), "token", 2).Return(rangeResponse, rangeTok, nil)
	mockFallback.EXPECT().Upsert(context.TODO(), adaptedEi, gomock.Any()).Return(nil)

	connector := NewConnector(mockOrigin, mockFallback, NewJSONEncoder())
	connector.setSynchronousMode(true)
	resp, tok, err := connector.Range(context.TODO(), testEi, map[string][]*dosa.Condition{}, []string{}, "token", 2)
	assert.NoError(t, err)
	assert.EqualValues(t, rangeResponse, resp)
	assert.EqualValues(t, rangeTok, tok)
}

// Test range, origin has error, fallback to cache succeeds.
func TestRangeError(t *testing.T) {
	originCtrl := gomock.NewController(t)
	defer originCtrl.Finish()
	mockOrigin := mocks.NewMockConnector(originCtrl)

	fallbackCtrl := gomock.NewController(t)
	defer fallbackCtrl.Finish()
	mockFallback := mocks.NewMockConnector(fallbackCtrl)

	fallbackResponse := map[string]dosa.FieldValue{"value": []byte("{\"rows\": [{\"b\": 7}], \"tokenNext\": \"nextToken\"}")}
	mockOrigin.EXPECT().Range(context.TODO(), testEi, map[string][]*dosa.Condition{}, dosa.All(), "token", 2).Return(nil, "", assert.AnError)
	mockFallback.EXPECT().Read(context.TODO(), adaptedEi, gomock.Any(), dosa.All()).Return(fallbackResponse, nil)

	connector := NewConnector(mockOrigin, mockFallback, NewJSONEncoder())
	connector.setSynchronousMode(true)
	resp, tok, err := connector.Range(context.TODO(), testEi, map[string][]*dosa.Condition{}, []string{}, "token", 2)
	assert.NoError(t, err)
	assert.EqualValues(t, []map[string]dosa.FieldValue{{"b": float64(7)}}, resp)
	assert.EqualValues(t, "nextToken", tok)
}

// Test range, origin has error, fallback to cache fails. Return original results
func TestRangeFallbackError(t *testing.T) {
	originCtrl := gomock.NewController(t)
	defer originCtrl.Finish()
	mockOrigin := mocks.NewMockConnector(originCtrl)

	fallbackCtrl := gomock.NewController(t)
	defer fallbackCtrl.Finish()
	mockFallback := mocks.NewMockConnector(fallbackCtrl)

	rangeResponse := []map[string]dosa.FieldValue{{"a": "b"}}
	rangeTok := "nextToken"
	rangeErr := errors.New("origin error")
	mockOrigin.EXPECT().Range(context.TODO(), testEi, map[string][]*dosa.Condition{}, dosa.All(), "token", 2).Return(rangeResponse, rangeTok, rangeErr)
	mockFallback.EXPECT().Read(context.TODO(), adaptedEi, gomock.Any(), dosa.All()).Return(nil, assert.AnError)

	connector := NewConnector(mockOrigin, mockFallback, NewJSONEncoder())
	connector.setSynchronousMode(true)
	resp, tok, err := connector.Range(context.TODO(), testEi, map[string][]*dosa.Condition{}, []string{}, "token", 2)
	assert.EqualError(t, err, rangeErr.Error())
	assert.EqualValues(t, rangeResponse, resp)
	assert.EqualValues(t, rangeTok, tok)
}

// Test scan has same behavior as range

// Test remove from origin also removes from cache. Does not matter if origin has an error or not
func TestRemove(t *testing.T) {
	originCtrl := gomock.NewController(t)
	defer originCtrl.Finish()
	mockOrigin := mocks.NewMockConnector(originCtrl)

	fallbackCtrl := gomock.NewController(t)
	defer fallbackCtrl.Finish()
	mockFallback := mocks.NewMockConnector(fallbackCtrl)

	keys := map[string]dosa.FieldValue{}

	mockOrigin.EXPECT().Remove(context.TODO(), testEi, keys).Return(nil)
	mockFallback.EXPECT().Remove(context.TODO(), adaptedEi, gomock.Any()).Return(nil)

	connector := NewConnector(mockOrigin, mockFallback, NewJSONEncoder())
	connector.setSynchronousMode(true)
	err := connector.Remove(context.TODO(), testEi, keys)
	assert.NoError(t, err)
}

// Test a connector method reverts to using origin if it's not defined in connector
func TestCreateIfNotExists(t *testing.T) {
	originCtrl := gomock.NewController(t)
	defer originCtrl.Finish()
	mockOrigin := mocks.NewMockConnector(originCtrl)

	values := map[string]dosa.FieldValue{}
	mockOrigin.EXPECT().CreateIfNotExists(context.TODO(), testEi, values).Return(nil)

	connector := NewConnector(mockOrigin, nil, NewJSONEncoder())
	connector.setSynchronousMode(true)
	err := connector.CreateIfNotExists(context.TODO(), testEi, values)
	assert.NoError(t, err)
}

// Test read write against redis
// First upsert successfully to origin and redis
// On read, origin errors. Should return result from redis
func TestUpsertRead(t *testing.T) {
	if !redis.IsRunning() {
		t.Skip("Redis is not running")
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockDownstreamConnector := mocks.NewMockConnector(ctrl)

	values := map[string]dosa.FieldValue{
		"UUIDKey":  "d1449c93-25b8-4032-920b-60471d91acc9",
		"StrKey":   "test key string",
		"Int64Key": 2932,
		"StrV":     "test value string",
		"BoolV":    false,
		"BlobV":    []byte("test value byte array"),
	}
	// Origin upsert succeeds
	mockDownstreamConnector.EXPECT().Upsert(context.TODO(), testEi, values).Return(nil)
	// origin read fails
	mockDownstreamConnector.EXPECT().Read(context.TODO(), testEi, values, dosa.All()).Return(nil, assert.AnError)

	connector := NewConnector(mockDownstreamConnector, redisC, NewGobEncoder())
	connector.setSynchronousMode(true)

	err := connector.Upsert(context.TODO(), testEi, values)
	assert.NoError(t, err)

	resp, err := connector.Read(context.TODO(), testEi, values, []string{})
	assert.NoError(t, err)
	assert.NotEmpty(t, resp)
	assert.EqualValues(t, values, resp)
}
