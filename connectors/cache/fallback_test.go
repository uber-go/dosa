package cache

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/memory"
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
		TTL: 15 * time.Second,
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
	cacheableEntities = []dosa.DomainObject{
		&testentity.TestEntity{},
	}
)

type BadEncoder struct{}

func (b *BadEncoder) Encode(interface{}) ([]byte, error) {
	return nil, errors.New("Encoding failed")
}

func (b *BadEncoder) Decode([]byte, interface{}) error {
	return errors.New("Decoding failed")
}

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

	values := map[string]dosa.FieldValue{
		"an_uuid_key": "d1449c93-25b8-4032-920b-60471d91acc9",
		"strkey":      "test key string",
		"StrV":        "test value string",
		"BoolV":       false,
	}
	transformedValues := map[string]dosa.FieldValue{
		key:   []byte(`[{"an_uuid_key":"d1449c93-25b8-4032-920b-60471d91acc9"},{"strkey":"test key string"}]`),
		value: []byte(`{"BoolV":false,"StrV":"test value string","an_uuid_key":"d1449c93-25b8-4032-920b-60471d91acc9","strkey":"test key string"}`),
	}
	mockOrigin.EXPECT().Upsert(context.TODO(), testEi, values).Return(nil)
	mockFallback.EXPECT().Upsert(context.TODO(), adaptedEi, transformedValues).Return(nil)

	connector := NewConnector(mockOrigin, mockFallback, NewJSONEncoder(), cacheableEntities...)
	connector.setSynchronousMode(true)
	err := connector.Upsert(context.TODO(), testEi, values)
	assert.NoError(t, err)
}

func TestAsyncUpsert(t *testing.T) {
	values := map[string]dosa.FieldValue{
		"an_uuid_key": "d1449c93-25b8-4032-920b-60471d91acc9",
		"strkey":      "test key string",
		"StrV":        "test value string",
		"BoolV":       false,
	}
	connector := NewConnector(memory.NewConnector(), memory.NewConnector(), NewJSONEncoder(), cacheableEntities...)
	err := connector.Upsert(context.TODO(), testEi, values)
	assert.NoError(t, err)
}

// Encoding error when creating cache key means we should not upsert into fallback
func TestUpsertEncodeError(t *testing.T) {
	originCtrl := gomock.NewController(t)
	defer originCtrl.Finish()
	mockOrigin := mocks.NewMockConnector(originCtrl)

	fallbackCtrl := gomock.NewController(t)
	defer fallbackCtrl.Finish()
	mockFallback := mocks.NewMockConnector(fallbackCtrl)

	values := map[string]dosa.FieldValue{
		"an_uuid_key": "d1449c93-25b8-4032-920b-60471d91acc9",
		"strkey":      "test key string",
		"StrV":        "test value string",
		"BoolV":       false,
	}
	mockOrigin.EXPECT().Upsert(context.TODO(), testEi, values).Return(nil)

	connector := NewConnector(mockOrigin, mockFallback, &BadEncoder{}, cacheableEntities...)
	connector.setSynchronousMode(true)
	err := connector.Upsert(context.TODO(), testEi, values)
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
	transformedResponse := map[string]dosa.FieldValue{
		key:   []byte("[]"),
		value: []byte(`{"a":"b"}`),
	}
	mockOrigin.EXPECT().Read(context.TODO(), testEi, values, dosa.All()).Return(originResponse, nil)
	mockFallback.EXPECT().Upsert(context.TODO(), adaptedEi, transformedResponse).Return(nil)

	connector := NewConnector(mockOrigin, mockFallback, NewJSONEncoder(), cacheableEntities...)
	connector.setSynchronousMode(true)
	resp, err := connector.Read(context.TODO(), testEi, values, []string{})
	assert.NoError(t, err)
	assert.EqualValues(t, originResponse, resp)
}

// fallback should never be called when this is an uncached entity
func TestReadUncachedEntity(t *testing.T) {
	originCtrl := gomock.NewController(t)
	defer originCtrl.Finish()
	mockOrigin := mocks.NewMockConnector(originCtrl)

	fallbackCtrl := gomock.NewController(t)
	defer fallbackCtrl.Finish()
	mockFallback := mocks.NewMockConnector(fallbackCtrl)

	values := map[string]dosa.FieldValue{}
	originResponse := map[string]dosa.FieldValue{"a": "b"}
	mockOrigin.EXPECT().Read(context.TODO(), testEi, values, dosa.All()).Return(originResponse, nil)

	connector := NewConnector(mockOrigin, mockFallback, NewJSONEncoder(), nil)
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

	fallbackResponse := map[string]dosa.FieldValue{"value": []byte("{\"b\": 7}")}
	originErr := errors.New("origin error")
	readValues := map[string]dosa.FieldValue{}
	transformedReadValues := map[string]dosa.FieldValue{
		key: []byte("[]"),
	}

	mockOrigin.EXPECT().Read(context.TODO(), testEi, readValues, dosa.All()).Return(nil, originErr)
	mockFallback.EXPECT().Read(context.TODO(), adaptedEi, transformedReadValues, dosa.All()).Return(fallbackResponse, nil)

	connector := NewConnector(mockOrigin, mockFallback, NewJSONEncoder(), cacheableEntities...)
	connector.setSynchronousMode(true)
	resp, err := connector.Read(context.TODO(), testEi, readValues, []string{})
	assert.NoError(t, err)
	assert.EqualValues(t, map[string]dosa.FieldValue{"b": float64(7)}, resp)
}

// Encoding error means do not upsert into fallback
func TestReadEncodeError(t *testing.T) {
	originCtrl := gomock.NewController(t)
	defer originCtrl.Finish()
	mockOrigin := mocks.NewMockConnector(originCtrl)

	fallbackCtrl := gomock.NewController(t)
	defer fallbackCtrl.Finish()
	mockFallback := mocks.NewMockConnector(fallbackCtrl)

	values := map[string]dosa.FieldValue{}
	originResponse := map[string]dosa.FieldValue{"a": "b"}
	mockOrigin.EXPECT().Read(context.TODO(), testEi, values, dosa.All()).Return(originResponse, nil)

	connector := NewConnector(mockOrigin, mockFallback, &BadEncoder{}, cacheableEntities...)
	connector.setSynchronousMode(true)
	resp, err := connector.Read(context.TODO(), testEi, values, []string{})
	assert.NoError(t, err)
	assert.EqualValues(t, originResponse, resp)
}

func TestReadDecodeError(t *testing.T) {
	originCtrl := gomock.NewController(t)
	defer originCtrl.Finish()
	mockOrigin := mocks.NewMockConnector(originCtrl)

	fallbackCtrl := gomock.NewController(t)
	defer fallbackCtrl.Finish()
	mockFallback := mocks.NewMockConnector(fallbackCtrl)

	fallbackResponse := map[string]dosa.FieldValue{"value": []byte("{\"b\": 7}")}
	originResponse := map[string]dosa.FieldValue{"a": "b"}
	originErr := errors.New("origin error")
	readValues := map[string]dosa.FieldValue{}
	transformedReadValues := map[string]dosa.FieldValue{
		key: []byte{},
	}

	mockOrigin.EXPECT().Read(context.TODO(), testEi, readValues, dosa.All()).Return(originResponse, originErr)
	mockFallback.EXPECT().Read(context.TODO(), adaptedEi, transformedReadValues, dosa.All()).Return(fallbackResponse, nil)

	connector := NewConnector(mockOrigin, mockFallback, &BadEncoder{}, cacheableEntities...)
	resp, err := connector.Read(context.TODO(), testEi, readValues, []string{})
	assert.Equal(t, originErr, err)
	assert.Equal(t, originResponse, resp)
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
	transformedReadValues := map[string]dosa.FieldValue{
		key: []byte("[]"),
	}
	originResponse := map[string]dosa.FieldValue{"a": "b"}
	originErr := errors.New("origin error")
	fallbackErr := errors.New("fallback error")
	mockOrigin.EXPECT().Read(context.TODO(), testEi, readValues, dosa.All()).Return(originResponse, originErr)
	mockFallback.EXPECT().Read(context.TODO(), adaptedEi, transformedReadValues, dosa.All()).Return(nil, fallbackErr)

	connector := NewConnector(mockOrigin, mockFallback, NewJSONEncoder(), cacheableEntities...)
	connector.setSynchronousMode(true)
	resp, err := connector.Read(context.TODO(), testEi, readValues, []string{})
	assert.EqualError(t, err, originErr.Error())
	assert.Equal(t, originResponse, resp)
}

func TestReadFallbackBadValue(t *testing.T) {
	originCtrl := gomock.NewController(t)
	defer originCtrl.Finish()
	mockOrigin := mocks.NewMockConnector(originCtrl)

	fallbackCtrl := gomock.NewController(t)
	defer fallbackCtrl.Finish()
	mockFallback := mocks.NewMockConnector(fallbackCtrl)

	readValues := map[string]dosa.FieldValue{}
	transformedReadValues := map[string]dosa.FieldValue{
		key: []byte("[]"),
	}
	originResponse := map[string]dosa.FieldValue{"a": "b"}
	originErr := errors.New("origin error")
	mockOrigin.EXPECT().Read(context.TODO(), testEi, readValues, dosa.All()).Return(originResponse, originErr)
	// fallback returns a response with no value field
	mockFallback.EXPECT().Read(context.TODO(), adaptedEi, transformedReadValues, dosa.All()).Return(nil, nil)

	connector := NewConnector(mockOrigin, mockFallback, NewJSONEncoder(), cacheableEntities...)
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
	transformedResponse := map[string]dosa.FieldValue{
		key:   []byte(`{"Conditions":{"column":[{"Op":5,"Value":"columnVal"}]},"Token":"token","Limit":2}`),
		value: []byte(`{"Rows":[{"a":"b"}],"TokenNext":"nextToken"}`),
	}
	conditions := map[string][]*dosa.Condition{"column": {{Op: dosa.GtOrEq, Value: "columnVal"}}}
	mockOrigin.EXPECT().Range(context.TODO(), testEi, conditions, dosa.All(), "token", 2).Return(rangeResponse, rangeTok, nil)
	mockFallback.EXPECT().Upsert(context.TODO(), adaptedEi, transformedResponse).Return(nil)

	connector := NewConnector(mockOrigin, mockFallback, NewJSONEncoder(), cacheableEntities...)
	connector.setSynchronousMode(true)
	resp, tok, err := connector.Range(context.TODO(), testEi, conditions, []string{}, "token", 2)
	assert.NoError(t, err)
	assert.EqualValues(t, rangeResponse, resp)
	assert.EqualValues(t, rangeTok, tok)
}

func TestRangeUncachedEntity(t *testing.T) {
	originCtrl := gomock.NewController(t)
	defer originCtrl.Finish()
	mockOrigin := mocks.NewMockConnector(originCtrl)

	fallbackCtrl := gomock.NewController(t)
	defer fallbackCtrl.Finish()
	mockFallback := mocks.NewMockConnector(fallbackCtrl)

	rangeResponse := []map[string]dosa.FieldValue{{"a": "b"}}
	rangeTok := "nextToken"
	conditions := map[string][]*dosa.Condition{"column": {{Op: dosa.GtOrEq, Value: "columnVal"}}}
	mockOrigin.EXPECT().Range(context.TODO(), testEi, conditions, dosa.All(), "token", 2).Return(rangeResponse, rangeTok, nil)

	// caching no entities, should return result from origin, fallback never called
	connector := NewConnector(mockOrigin, mockFallback, NewJSONEncoder(), nil)
	resp, tok, err := connector.Range(context.TODO(), testEi, conditions, []string{}, "token", 2)
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

	conditions := map[string][]*dosa.Condition{"column": {{Op: dosa.GtOrEq, Value: "columnVal"}}}
	transformedKey := map[string]dosa.FieldValue{
		key: []byte(`{"Conditions":{"column":[{"Op":5,"Value":"columnVal"}]},"Token":"token","Limit":2}`),
	}
	fallbackResponse := map[string]dosa.FieldValue{"value": []byte("{\"rows\": [{\"b\": 7}], \"tokenNext\": \"nextToken\"}")}
	mockOrigin.EXPECT().Range(context.TODO(), testEi, conditions, dosa.All(), "token", 2).Return(nil, "", assert.AnError)
	mockFallback.EXPECT().Read(context.TODO(), adaptedEi, transformedKey, dosa.All()).Return(fallbackResponse, nil)

	connector := NewConnector(mockOrigin, mockFallback, NewJSONEncoder(), cacheableEntities...)
	connector.setSynchronousMode(true)
	resp, tok, err := connector.Range(context.TODO(), testEi, conditions, []string{}, "token", 2)
	assert.NoError(t, err)
	assert.EqualValues(t, []map[string]dosa.FieldValue{{"b": float64(7)}}, resp)
	assert.EqualValues(t, "nextToken", tok)
}

func TestRangeEncodeError(t *testing.T) {
	originCtrl := gomock.NewController(t)
	defer originCtrl.Finish()
	mockOrigin := mocks.NewMockConnector(originCtrl)

	fallbackCtrl := gomock.NewController(t)
	defer fallbackCtrl.Finish()
	mockFallback := mocks.NewMockConnector(fallbackCtrl)

	rangeResponse := []map[string]dosa.FieldValue{{"a": "b"}}
	rangeTok := "nextToken"
	conditions := map[string][]*dosa.Condition{"column": {{Op: dosa.GtOrEq, Value: "columnVal"}}}
	mockOrigin.EXPECT().Range(context.TODO(), testEi, conditions, dosa.All(), "token", 2).Return(rangeResponse, rangeTok, nil)

	connector := NewConnector(mockOrigin, mockFallback, &BadEncoder{}, cacheableEntities...)
	connector.setSynchronousMode(true)
	resp, tok, err := connector.Range(context.TODO(), testEi, conditions, []string{}, "token", 2)
	assert.NoError(t, err)
	assert.EqualValues(t, rangeResponse, resp)
	assert.EqualValues(t, rangeTok, tok)
}

// Bad decoding of fallback response should return the original response
func TestRangeDecodeError(t *testing.T) {
	originCtrl := gomock.NewController(t)
	defer originCtrl.Finish()
	mockOrigin := mocks.NewMockConnector(originCtrl)

	fallbackCtrl := gomock.NewController(t)
	defer fallbackCtrl.Finish()
	mockFallback := mocks.NewMockConnector(fallbackCtrl)

	conditions := map[string][]*dosa.Condition{"column": {{Op: dosa.GtOrEq, Value: "columnVal"}}}
	transformedKey := map[string]dosa.FieldValue{
		key: []byte(nil),
	}
	rangeResponse := []map[string]dosa.FieldValue{{"a": "b"}}
	rangeTok := "nextToken"
	rangeErr := errors.New("origin error")

	fallbackResponse := map[string]dosa.FieldValue{"value": []byte("{\"rows\": [{\"b\": 7}], \"tokenNext\": \"nextToken\"}")}
	mockOrigin.EXPECT().Range(context.TODO(), testEi, conditions, dosa.All(), "token", 2).Return(rangeResponse, rangeTok, rangeErr)
	mockFallback.EXPECT().Read(context.TODO(), adaptedEi, transformedKey, dosa.All()).Return(fallbackResponse, nil)

	connector := NewConnector(mockOrigin, mockFallback, &BadEncoder{}, cacheableEntities...)
	resp, tok, err := connector.Range(context.TODO(), testEi, conditions, []string{}, "token", 2)
	assert.Equal(t, rangeErr, err)
	assert.Equal(t, rangeResponse, resp)
	assert.Equal(t, rangeTok, tok)
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
	transformedKeys := map[string]dosa.FieldValue{
		key: []byte(`{"Token":"token","Limit":2}`),
	}
	mockOrigin.EXPECT().Range(context.TODO(), testEi, nil, dosa.All(), "token", 2).Return(rangeResponse, rangeTok, rangeErr)
	mockFallback.EXPECT().Read(context.TODO(), adaptedEi, transformedKeys, dosa.All()).Return(nil, assert.AnError)

	connector := NewConnector(mockOrigin, mockFallback, NewJSONEncoder(), cacheableEntities...)
	connector.setSynchronousMode(true)
	resp, tok, err := connector.Range(context.TODO(), testEi, nil, []string{}, "token", 2)
	assert.EqualError(t, err, rangeErr.Error())
	assert.EqualValues(t, rangeResponse, resp)
	assert.EqualValues(t, rangeTok, tok)
}

// Test scan, origin succeeds, should upsert to fallback
func TestScanSuccess(t *testing.T) {
	originCtrl := gomock.NewController(t)
	defer originCtrl.Finish()
	mockOrigin := mocks.NewMockConnector(originCtrl)

	fallbackCtrl := gomock.NewController(t)
	defer fallbackCtrl.Finish()
	mockFallback := mocks.NewMockConnector(fallbackCtrl)

	rangeResponse := []map[string]dosa.FieldValue{{"a": "b"}}
	rangeTok := "nextToken"
	transformedResponse := map[string]dosa.FieldValue{
		key:   []byte(`{"Token":"token","Limit":2}`),
		value: []byte(`{"Rows":[{"a":"b"}],"TokenNext":"nextToken"}`),
	}
	mockOrigin.EXPECT().Range(context.TODO(), testEi, nil, dosa.All(), "token", 2).Return(rangeResponse, rangeTok, nil)
	mockFallback.EXPECT().Upsert(context.TODO(), adaptedEi, transformedResponse).Return(nil)

	connector := NewConnector(mockOrigin, mockFallback, NewJSONEncoder(), cacheableEntities...)
	connector.setSynchronousMode(true)
	resp, tok, err := connector.Scan(context.TODO(), testEi, []string{}, "token", 2)
	assert.NoError(t, err)
	assert.EqualValues(t, rangeResponse, resp)
	assert.EqualValues(t, rangeTok, tok)
}

// Test scan, origin has error, fallback to cache succeeds.
func TestScanError(t *testing.T) {
	originCtrl := gomock.NewController(t)
	defer originCtrl.Finish()
	mockOrigin := mocks.NewMockConnector(originCtrl)

	fallbackCtrl := gomock.NewController(t)
	defer fallbackCtrl.Finish()
	mockFallback := mocks.NewMockConnector(fallbackCtrl)

	transformedKey := map[string]dosa.FieldValue{
		key: []byte(`{"Token":"token","Limit":2}`),
	}
	fallbackResponse := map[string]dosa.FieldValue{"value": []byte("{\"rows\": [{\"b\": 7}], \"tokenNext\": \"nextToken\"}")}
	mockOrigin.EXPECT().Range(context.TODO(), testEi, nil, dosa.All(), "token", 2).Return(nil, "", assert.AnError)
	mockFallback.EXPECT().Read(context.TODO(), adaptedEi, transformedKey, dosa.All()).Return(fallbackResponse, nil)

	connector := NewConnector(mockOrigin, mockFallback, NewJSONEncoder(), cacheableEntities...)
	connector.setSynchronousMode(true)
	resp, tok, err := connector.Scan(context.TODO(), testEi, []string{}, "token", 2)
	assert.NoError(t, err)
	assert.EqualValues(t, []map[string]dosa.FieldValue{{"b": float64(7)}}, resp)
	assert.EqualValues(t, "nextToken", tok)
}

// Test scan, origin has error, fallback to cache fails. Return original results
func TestScanFallbackError(t *testing.T) {
	originCtrl := gomock.NewController(t)
	defer originCtrl.Finish()
	mockOrigin := mocks.NewMockConnector(originCtrl)

	fallbackCtrl := gomock.NewController(t)
	defer fallbackCtrl.Finish()
	mockFallback := mocks.NewMockConnector(fallbackCtrl)

	rangeResponse := []map[string]dosa.FieldValue{{"a": "b"}}
	rangeTok := "nextToken"
	rangeErr := errors.New("origin error")
	transformedKeys := map[string]dosa.FieldValue{
		key: []byte(`{"Token":"token","Limit":2}`),
	}
	mockOrigin.EXPECT().Range(context.TODO(), testEi, nil, dosa.All(), "token", 2).Return(rangeResponse, rangeTok, rangeErr)
	mockFallback.EXPECT().Read(context.TODO(), adaptedEi, transformedKeys, dosa.All()).Return(nil, assert.AnError)

	connector := NewConnector(mockOrigin, mockFallback, NewJSONEncoder(), cacheableEntities...)
	connector.setSynchronousMode(true)
	resp, tok, err := connector.Scan(context.TODO(), testEi, []string{}, "token", 2)
	assert.EqualError(t, err, rangeErr.Error())
	assert.EqualValues(t, rangeResponse, resp)
	assert.EqualValues(t, rangeTok, tok)
}

// Test remove from origin also removes from cache. Does not matter if origin has an error or not
func TestRemove(t *testing.T) {
	originCtrl := gomock.NewController(t)
	defer originCtrl.Finish()
	mockOrigin := mocks.NewMockConnector(originCtrl)

	fallbackCtrl := gomock.NewController(t)
	defer fallbackCtrl.Finish()
	mockFallback := mocks.NewMockConnector(fallbackCtrl)

	keys := map[string]dosa.FieldValue{}
	transformedKeys := map[string]dosa.FieldValue{key: []byte("[]")}
	mockOrigin.EXPECT().Remove(context.TODO(), testEi, keys).Return(nil)
	mockFallback.EXPECT().Remove(context.TODO(), adaptedEi, transformedKeys).Return(nil)

	connector := NewConnector(mockOrigin, mockFallback, NewJSONEncoder(), cacheableEntities...)
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

	connector := NewConnector(mockOrigin, nil, NewJSONEncoder(), cacheableEntities...)
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
		"an_uuid_key": "d1449c93-25b8-4032-920b-60471d91acc9",
		"strkey":      "test key string",
		"int64key":    2932,
		"strv":        "test value string",
		"boolv":       false,
		"blobv":       []byte("test value byte array"),
	}
	// Origin upsert succeeds
	mockDownstreamConnector.EXPECT().Upsert(context.TODO(), testEi, values).Return(nil)
	// origin read fails
	mockDownstreamConnector.EXPECT().Read(context.TODO(), testEi, values, dosa.All()).Return(nil, assert.AnError)

	connector := NewConnector(mockDownstreamConnector, redisC, NewGobEncoder(), cacheableEntities...)
	connector.setSynchronousMode(true)

	err := connector.Upsert(context.TODO(), testEi, values)
	assert.NoError(t, err)

	resp, err := connector.Read(context.TODO(), testEi, values, []string{})
	assert.NoError(t, err)
	assert.NotEmpty(t, resp)
	assert.EqualValues(t, values, resp)
}

func TestCreateCacheKey(t *testing.T) {
	values := map[string]dosa.FieldValue{
		"an_uuid_key": "d1449c93-25b8-4032-920b-60471d91acc9",
		"strv":        "test value string",
		"boolv":       false,
		"int64key":    2932,
		"blobv":       []byte("test value byte array"),
		"strkey":      "test key string",
	}
	key := createCacheKey(testEi, values, NewJSONEncoder())
	assert.Equal(t, []byte(`[{"an_uuid_key":"d1449c93-25b8-4032-920b-60471d91acc9"},{"int64key":2932},{"strkey":"test key string"}]`), key)
}

func TestCacheableEntities(t *testing.T) {
	// Test cacheable entities, ignore entities in the list that are invalid
	set := createCachedEntitiesSet([]dosa.DomainObject{&testentity.TestEntity{}, &dosa.Entity{}})
	assert.Len(t, set, 1)
}

func TestSettingCachedEntities(t *testing.T) {
	e1 := struct {
		dosa.Entity `dosa:"name=e1, primaryKey=(Hello)"`
		Hello       string
	}{}
	e2 := struct {
		dosa.Entity `dosa:"name=e2, primaryKey=(World)"`
		World       string
	}{}
	connector := NewConnector(memory.NewConnector(), memory.NewConnector(), NewJSONEncoder(), &e1, &e2)
	assert.Len(t, connector.cacheableEntities, 2)
	assert.Contains(t, connector.cacheableEntities, "e1")
	assert.Contains(t, connector.cacheableEntities, "e2")
	connector.SetCachedEntities(nil)
	assert.Empty(t, connector.cacheableEntities)
}
