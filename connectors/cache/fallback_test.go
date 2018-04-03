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
	"github.com/uber-go/dosa/encoding"
	"github.com/uber-go/dosa/mocks"
	"github.com/uber-go/dosa/testentity"
)

var (
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
	encodedValue = []byte("Test encoding")
	i            = int32(7)
	s            = "test decode"
	b            = true
	decodedValue = map[string]dosa.FieldValue{
		"int32v": i,
		"strv":   s,
		"boolvp": b,
	}
	decodedValueAsPointers = map[string]dosa.FieldValue{
		"int32v": &i,
		"strv":   &s,
		"boolvp": &b,
	}
)

func createTestEi(sr dosa.SchemaRef) *dosa.EntityInfo {
	table, _ := dosa.TableFromInstance(&testentity.TestEntity{})
	testEi := &dosa.EntityInfo{Ref: &sr, Def: &table.EntityDefinition}
	return testEi
}

type testCase struct {
	encoder          encoding.Encoder
	cachedEntities   []dosa.DomainObject
	originRead       *expectArgs
	originUpsert     *expectArgs
	originRange      *rangeArgs
	fallbackRead     *expectArgs
	fallbackUpsert   *expectArgs
	expectedResp     map[string]dosa.FieldValue   // For read and upsert
	expectedManyResp []map[string]dosa.FieldValue // For range
	expectedTok      string
	expectedErr      error
	description      string
}

type expectArgs struct {
	values map[string]dosa.FieldValue
	resp   map[string]dosa.FieldValue
	err    error
}

type rangeArgs struct {
	columnConditions map[string][]*dosa.Condition
	token            string
	limit            int
	resp             []map[string]dosa.FieldValue
	nextToken        string
	err              error
}

type staticEncoder struct {
	encodeErr error
	decodeErr error
}

func (e staticEncoder) Encode(int interface{}) (b []byte, err error) {
	return encodedValue, e.encodeErr
}

func (e staticEncoder) Decode(byt []byte, int interface{}) error {
	if m, ok := int.(*map[string]dosa.FieldValue); ok {
		*m = decodedValue
		return e.decodeErr
	}
	if r, ok := int.(*rangeResults); ok {
		(*r).Rows = []map[string]dosa.FieldValue{decodedValue}
		(*r).TokenNext = "hardcodedDecodedToken"
		return e.decodeErr
	}
	return e.decodeErr
}

// Test dosa upsert and the various behaviors of the fallback
func TestUpsertCases(t *testing.T) {
	runTestCase := func(tc testCase) {
		t.Run(tc.description, func(t *testing.T) {
			originCtrl := gomock.NewController(t)
			defer originCtrl.Finish()
			mockOrigin := mocks.NewMockConnector(originCtrl)

			fallbackCtrl := gomock.NewController(t)
			defer fallbackCtrl.Finish()
			mockFallback := mocks.NewMockConnector(fallbackCtrl)

			mockOrigin.EXPECT().Upsert(context.TODO(), testEi, tc.originUpsert.values).Return(tc.originUpsert.err)
			if tc.fallbackUpsert != nil {
				mockFallback.EXPECT().Remove(gomock.Not(context.TODO()), adaptedEi, tc.fallbackUpsert.values).Return(nil).AnyTimes()
			}

			connector := newConnector(mockOrigin, mockFallback, nil, tc.encoder, cacheableEntities...)
			connector.setSynchronousMode(true)
			err := connector.Upsert(context.TODO(), testEi, tc.originUpsert.values)
			assert.Equal(t, tc.expectedErr, err, tc.description)
		})
	}

	var testBool bool

	testCases := []testCase{
		{
			description: "Successful origin upsert invalidates the key in fallback",
			originUpsert: &expectArgs{
				values: map[string]dosa.FieldValue{
					"an_uuid_key": "d1449c93-25b8-4032-920b-60471d91acc9",
					"strkey":      "test key string",
					"StrV":        "test value string",
					"BoolVP":      &testBool,
				}},
			fallbackUpsert: &expectArgs{
				values: map[string]dosa.FieldValue{
					"key": encodedValue,
				},
			},
			encoder: staticEncoder{},
		},
		{
			description: "Encoding error while creating cache key means we use empty key when calling fallback",
			originUpsert: &expectArgs{
				values: map[string]dosa.FieldValue{
					"an_uuid_key": "d1449c93-25b8-4032-920b-60471d91acc9",
					"strkey":      "test key string",
					"BoolV":       false,
				}},
			fallbackUpsert: &expectArgs{
				values: map[string]dosa.FieldValue{
					"key": []byte{},
				},
			},
			encoder: staticEncoder{encodeErr: assert.AnError},
		},
		{
			description: "Unsuccessful origin upsert still invalidates fallback",
			originUpsert: &expectArgs{
				values: map[string]dosa.FieldValue{
					"an_uuid_key": "d1449c93-25b8-4032-920b-60471d91acc9",
					"strkey":      "test key string",
					"StrV":        "test value string",
					"BoolVP":      &testBool,
				},
				err: assert.AnError,
			},
			fallbackUpsert: &expectArgs{
				values: map[string]dosa.FieldValue{
					"key": encodedValue,
				},
			},
			encoder:     staticEncoder{},
			expectedErr: assert.AnError,
		},
	}
	for _, t := range testCases {
		runTestCase(t)
	}
}

// Run the upsert to fallback in the goroutine. Should not affect the main path.
func TestAsyncUpsert(t *testing.T) {
	values := map[string]dosa.FieldValue{
		"an_uuid_key": "d1449c93-25b8-4032-920b-60471d91acc9",
		"strkey":      "test key string",
		"StrV":        "test value string",
		"BoolV":       false,
	}
	connector := NewConnector(memory.NewConnector(), memory.NewConnector(), nil, cacheableEntities...)
	err := connector.Upsert(context.TODO(), testEi, values)
	assert.NoError(t, err)
}

// Test dosa read and the various behaviors of the fallback
func TestReadCases(t *testing.T) {
	runTestCase := func(tc testCase) {
		t.Run(tc.description, func(t *testing.T) {
			originCtrl := gomock.NewController(t)
			defer originCtrl.Finish()
			mockOrigin := mocks.NewMockConnector(originCtrl)

			fallbackCtrl := gomock.NewController(t)
			defer fallbackCtrl.Finish()
			mockFallback := mocks.NewMockConnector(fallbackCtrl)

			mockOrigin.EXPECT().Read(context.TODO(), testEi, tc.originRead.values, dosa.All()).Return(tc.originRead.resp, tc.originRead.err)
			if tc.fallbackRead != nil {
				mockFallback.EXPECT().Read(context.TODO(), adaptedEi, tc.fallbackRead.values, dosa.All()).Return(tc.fallbackRead.resp, tc.fallbackRead.err)
			}
			if tc.fallbackUpsert != nil {
				mockFallback.EXPECT().Upsert(gomock.Not(context.TODO()), adaptedEi, tc.fallbackUpsert.values).Return(tc.fallbackUpsert.err)
			}

			connector := newConnector(mockOrigin, mockFallback, nil, tc.encoder, tc.cachedEntities...)
			connector.setSynchronousMode(true)
			resp, err := connector.Read(context.TODO(), testEi, tc.originRead.values, []string{})
			assert.Equal(t, tc.expectedErr, err, tc.description)
			assert.Equal(t, tc.expectedResp, resp, tc.description)
		})
	}

	testCases := []testCase{
		createReadSuccessTestCase(),
		createReadUncachedEntityTestCase(),
		createReadFailTestCase(),
		createReadNotFoundTestCase(),
		createFallbackWriteKeyEncodeErrorTestCase(),
		createFallbackReadEncodeErrorTestCase(),
		createReadDecodeErrorTestCase(),
		createReadFallbackFailTestCase(),
		createReadFallbackBadValueTestCase(),
	}
	for _, tc := range testCases {
		runTestCase(tc)
	}
}

func createReadSuccessTestCase() testCase {
	return testCase{
		description:    "Test read from origin succeeds, should write response to fallback that includes primary keys",
		cachedEntities: cacheableEntities,
		originRead: &expectArgs{
			values: map[string]dosa.FieldValue{"strkey": "primaryValue"},
			resp:   map[string]dosa.FieldValue{"a": "b"},
		},
		fallbackUpsert: &expectArgs{
			values: map[string]dosa.FieldValue{
				key:   encodedValue,
				value: encodedValue,
			},
		},
		expectedResp: map[string]dosa.FieldValue{"a": "b", "strkey": "primaryValue"},
		expectedErr:  nil,
		encoder:      staticEncoder{},
	}
}

func createReadUncachedEntityTestCase() testCase {
	originResponse := map[string]dosa.FieldValue{"a": "b"}

	return testCase{
		cachedEntities: nil,
		originRead: &expectArgs{
			resp: originResponse,
		},
		expectedResp: originResponse,
		expectedErr:  nil,
		description:  "Fallback should never be called when we set the list of cached entities to empty",
	}
}

func createReadFailTestCase() testCase {
	return testCase{
		description:    "Test when read origin has error, we return from the fallback",
		cachedEntities: cacheableEntities,
		originRead: &expectArgs{
			values: map[string]dosa.FieldValue{"int64key": 20},
			err:    assert.AnError,
		},
		fallbackRead: &expectArgs{
			values: map[string]dosa.FieldValue{key: encodedValue},
			resp:   map[string]dosa.FieldValue{"value": []byte("some response")},
		},
		expectedResp: decodedValueAsPointers,
		expectedErr:  nil,
		encoder:      staticEncoder{},
	}
}

func createReadNotFoundTestCase() testCase {
	originResponse := map[string]dosa.FieldValue{"a": "b"}
	originErr := &dosa.ErrNotFound{}
	return testCase{
		description:    "Test that when read origin has err not found, do not read from fallback, return origin response and error",
		cachedEntities: cacheableEntities,
		originRead: &expectArgs{
			err:  originErr,
			resp: originResponse,
		},
		expectedResp: originResponse,
		expectedErr:  originErr,
	}
}

func createFallbackWriteKeyEncodeErrorTestCase() testCase {
	originResponse := map[string]dosa.FieldValue{"a": "b"}

	return testCase{
		description:    "If an encoding error occurs while creating encoded key for the fallback, do not upsert into fallback",
		cachedEntities: cacheableEntities,
		originRead: &expectArgs{
			values: map[string]dosa.FieldValue{"strkey": "primaryValue"},
			resp:   originResponse,
			err:    nil,
		},
		expectedResp: originResponse,
		expectedErr:  nil,
		encoder:      staticEncoder{encodeErr: assert.AnError},
	}
}

func createFallbackReadEncodeErrorTestCase() testCase {
	return testCase{
		description:    "If an encoding error occurs while creating encoded values for the fallback, do not read from fallback",
		cachedEntities: cacheableEntities,
		originRead: &expectArgs{
			values: map[string]dosa.FieldValue{"strkey": "primaryValue"},
			resp:   nil,
			err:    assert.AnError,
		},
		expectedResp: nil,
		expectedErr:  assert.AnError,
		encoder:      staticEncoder{encodeErr: assert.AnError},
	}
}

func createReadDecodeErrorTestCase() testCase {
	originResponse := map[string]dosa.FieldValue{"a": "b"}
	originErr := errors.New("origin error")

	return testCase{
		description:    "If an error occurs while trying to decode the response from the fallback, return the original response",
		cachedEntities: cacheableEntities,
		originRead: &expectArgs{
			values: map[string]dosa.FieldValue{"strkey": "primaryValue"},
			resp:   originResponse,
			err:    originErr,
		},
		fallbackRead: &expectArgs{
			values: map[string]dosa.FieldValue{key: encodedValue},
			resp:   map[string]dosa.FieldValue{"value": []byte("some response")},
		},
		expectedResp: originResponse,
		expectedErr:  originErr,
		encoder:      staticEncoder{decodeErr: assert.AnError},
	}
}

func createReadFallbackFailTestCase() testCase {
	originResponse := map[string]dosa.FieldValue{"a": "b"}
	originErr := errors.New("origin error")

	return testCase{
		cachedEntities: cacheableEntities,
		originRead: &expectArgs{
			resp: originResponse,
			err:  originErr,
		},
		fallbackRead: &expectArgs{
			values: map[string]dosa.FieldValue{key: encodedValue},
			err:    errors.New("fallback error"),
		},
		expectedResp: originResponse,
		expectedErr:  originErr,
		encoder:      staticEncoder{},
		description:  "Test read origin has error and fallback also fails. Should return the origin error",
	}
}

func createReadFallbackBadValueTestCase() testCase {
	originResponse := map[string]dosa.FieldValue{"a": "b"}
	originErr := errors.New("origin error")

	return testCase{
		description:    "When fallback response is empty/corrupted, return the response from origin",
		encoder:        staticEncoder{},
		cachedEntities: cacheableEntities,
		originRead: &expectArgs{
			resp: originResponse,
			err:  originErr,
		},
		fallbackRead: &expectArgs{
			values: map[string]dosa.FieldValue{key: encodedValue},
			// fallback returns a response with no value field
			resp: nil,
		},
		expectedResp: originResponse,
		expectedErr:  originErr,
	}

}

// Test logging stats when using fallback path
func TestFallbackStats(t *testing.T) {
	originCtrl := gomock.NewController(t)
	defer originCtrl.Finish()
	mockOrigin := mocks.NewMockConnector(originCtrl)

	fallbackCtrl := gomock.NewController(t)
	defer fallbackCtrl.Finish()
	mockFallback := mocks.NewMockConnector(fallbackCtrl)

	statsCtrl := gomock.NewController(t)
	defer statsCtrl.Finish()
	mockStats := mocks.NewMockScope(statsCtrl)

	counterCtrl := gomock.NewController(t)
	defer counterCtrl.Finish()
	mockCounter := mocks.NewMockCounter(counterCtrl)

	type testCase struct {
		counter      string
		fallbackResp map[string]dosa.FieldValue
		fallbackErr  error
	}
	connector := NewConnector(mockOrigin, mockFallback, mockStats, cacheableEntities...)

	testCases := []testCase{
		{
			counter:     "failure",
			fallbackErr: assert.AnError,
		},
		{
			counter:      "success",
			fallbackResp: map[string]dosa.FieldValue{"value": []byte("{\"b\": 7}")},
		},
	}
	for _, t := range testCases {
		mockOrigin.EXPECT().Read(context.TODO(), testEi, nil, dosa.All()).Return(nil, assert.AnError)
		mockFallback.EXPECT().Read(context.TODO(), adaptedEi, gomock.Any(), dosa.All()).Return(t.fallbackResp, t.fallbackErr)
		mockStats.EXPECT().SubScope("fallback").Return(mockStats)
		mockStats.EXPECT().Tagged(map[string]string{"method": "READ", "entityName": "awesome_test_entity"}).Return(mockStats)
		mockStats.EXPECT().Counter(t.counter).Return(mockCounter)
		mockCounter.EXPECT().Inc(int64(1))

		connector.Read(context.TODO(), testEi, nil, []string{})
	}
}

// Test dosa range and the various behavior of the fallback path
func TestRangeCases(t *testing.T) {
	runTestCase := func(tc testCase) {
		t.Run(tc.description, func(t *testing.T) {
			originCtrl := gomock.NewController(t)
			defer originCtrl.Finish()
			mockOrigin := mocks.NewMockConnector(originCtrl)

			fallbackCtrl := gomock.NewController(t)
			defer fallbackCtrl.Finish()
			mockFallback := mocks.NewMockConnector(fallbackCtrl)

			mockOrigin.EXPECT().Range(context.TODO(), testEi, tc.originRange.columnConditions, dosa.All(), tc.originRange.token, tc.originRange.limit).
				Return(tc.originRange.resp, tc.originRange.nextToken, tc.originRange.err)

			if tc.fallbackRead != nil {
				mockFallback.EXPECT().Read(context.TODO(), adaptedEi, tc.fallbackRead.values, dosa.All()).Return(tc.fallbackRead.resp, tc.fallbackRead.err)
			}
			if tc.fallbackUpsert != nil {
				mockFallback.EXPECT().Upsert(gomock.Not(context.TODO()), adaptedEi, tc.fallbackUpsert.values).Return(tc.fallbackUpsert.err)
			}

			connector := newConnector(mockOrigin, mockFallback, nil, tc.encoder, tc.cachedEntities...)
			connector.setSynchronousMode(true)

			resp, tok, err := connector.Range(context.TODO(), testEi, tc.originRange.columnConditions, []string{}, tc.originRange.token, tc.originRange.limit)
			assert.Equal(t, tc.expectedErr, err, tc.description)
			assert.EqualValues(t, tc.expectedManyResp, resp, tc.description)
			assert.Equal(t, tc.expectedTok, tok, tc.description)
		})
	}

	testCases := []testCase{
		createRangeSuccessTestCase(),
		createRangeUncachedEntityTestCase(),
		createRangeFailTestCase(),
		createRangeNotFoundTestCase(),
		createRangeEncodeErrorTestCase(),
		createRangeDecodeErrorTestCase(),
		createRangeFallbackFailTestCase(),
		createRangeFallbackBadValueTestCase(),
	}
	for _, tc := range testCases {
		runTestCase(tc)
	}
}

func createRangeSuccessTestCase() testCase {
	rangeResponse := []map[string]dosa.FieldValue{{"a": "b"}}
	rangeTok := "nextToken"
	return testCase{
		description:    "Test range from origin succeeds, should write response to cache",
		cachedEntities: cacheableEntities,
		originRange: &rangeArgs{
			columnConditions: map[string][]*dosa.Condition{"column": {{Op: dosa.GtOrEq, Value: "columnVal"}}},
			token:            "token",
			limit:            2,
			resp:             rangeResponse,
			nextToken:        rangeTok,
		},
		fallbackUpsert: &expectArgs{
			values: map[string]dosa.FieldValue{
				key:   encodedValue,
				value: encodedValue,
			},
			err: nil,
		},
		expectedErr:      nil,
		expectedManyResp: rangeResponse,
		expectedTok:      rangeTok,
		encoder:          staticEncoder{},
	}
}

func createRangeUncachedEntityTestCase() testCase {
	rangeResponse := []map[string]dosa.FieldValue{{"a": "b"}}
	rangeTok := "nextToken"
	conditions := map[string][]*dosa.Condition{"column": {{Op: dosa.GtOrEq, Value: "columnVal"}}}

	return testCase{
		description:    "When caching no entities, should return result from origin, and fallback is never called",
		cachedEntities: nil,
		originRange: &rangeArgs{
			columnConditions: conditions,
			token:            "token",
			limit:            2,
			resp:             rangeResponse,
			nextToken:        rangeTok,
		},
		expectedErr:      nil,
		expectedManyResp: rangeResponse,
		expectedTok:      rangeTok,
	}
}

func createRangeFailTestCase() testCase {
	conditions := map[string][]*dosa.Condition{"column": {{Op: dosa.GtOrEq, Value: "columnVal"}}}

	return testCase{
		description:    "Test range from origin has error and fallback succeeds",
		cachedEntities: cacheableEntities,
		originRange: &rangeArgs{
			columnConditions: conditions,
			token:            "token",
			limit:            2,
			err:              assert.AnError,
		},
		fallbackRead: &expectArgs{
			values: map[string]dosa.FieldValue{key: encodedValue},
			resp:   map[string]dosa.FieldValue{"value": []byte("b")},
		},
		expectedErr:      nil,
		expectedManyResp: []map[string]dosa.FieldValue{decodedValueAsPointers},
		expectedTok:      "hardcodedDecodedToken",
		encoder:          staticEncoder{},
	}
}

func createRangeNotFoundTestCase() testCase {
	conditions := map[string][]*dosa.Condition{"column": {{Op: dosa.GtOrEq, Value: "columnVal"}}}
	rangeResponse := []map[string]dosa.FieldValue{{"a": "b"}}
	rangeTok := "nextToken"
	rangeErr := &dosa.ErrNotFound{}

	return testCase{
		description:    "Test that when origin has err not found, do not read from fallback, return origin response and error",
		cachedEntities: cacheableEntities,
		originRange: &rangeArgs{
			columnConditions: conditions,
			token:            "token",
			limit:            2,
			resp:             rangeResponse,
			nextToken:        rangeTok,
			err:              rangeErr,
		},
		expectedErr:      rangeErr,
		expectedManyResp: rangeResponse,
		expectedTok:      rangeTok,
		encoder:          staticEncoder{},
	}
}

func createRangeEncodeErrorTestCase() testCase {
	rangeResponse := []map[string]dosa.FieldValue{{"a": "b"}}
	rangeTok := "nextToken"
	conditions := map[string][]*dosa.Condition{"column": {{Op: dosa.GtOrEq, Value: "columnVal"}}}

	return testCase{
		description:    "If an encoding error occurs while creating encoded values for the fallback, do not upsert into fallback",
		cachedEntities: cacheableEntities,
		originRange: &rangeArgs{
			columnConditions: conditions,
			token:            "token",
			limit:            2,
			resp:             rangeResponse,
			nextToken:        rangeTok,
		},
		expectedErr:      nil,
		expectedManyResp: rangeResponse,
		expectedTok:      rangeTok,
		encoder:          staticEncoder{encodeErr: assert.AnError},
	}
}

func createRangeDecodeErrorTestCase() testCase {
	conditions := map[string][]*dosa.Condition{"column": {{Op: dosa.GtOrEq, Value: errors.New("some unknown type")}}}
	rangeResponse := []map[string]dosa.FieldValue{{"a": "b"}}
	rangeTok := "nextToken"
	rangeErr := errors.New("origin error")
	fallbackResponse := map[string]dosa.FieldValue{"value": []byte("bad cache value")}

	return testCase{
		description:    "Bad decoding of fallback response should result in returning the original response",
		cachedEntities: cacheableEntities,
		originRange: &rangeArgs{
			columnConditions: conditions,
			token:            "token",
			limit:            2,
			resp:             rangeResponse,
			nextToken:        rangeTok,
			err:              rangeErr,
		},
		fallbackRead: &expectArgs{
			values: map[string]dosa.FieldValue{key: encodedValue},
			resp:   fallbackResponse,
		},
		expectedErr:      rangeErr,
		expectedManyResp: rangeResponse,
		expectedTok:      rangeTok,
		encoder:          staticEncoder{decodeErr: assert.AnError},
	}
}

func createRangeFallbackFailTestCase() testCase {
	rangeResponse := []map[string]dosa.FieldValue{{"a": "b"}}
	rangeTok := "nextToken"
	rangeErr := errors.New("origin error")
	return testCase{
		description:    "Test range from origin has error and fallback to cache fails. Should return origin results",
		cachedEntities: cacheableEntities,
		originRange: &rangeArgs{
			token:     "token",
			limit:     2,
			resp:      rangeResponse,
			nextToken: rangeTok,
			err:       rangeErr,
		},
		fallbackRead: &expectArgs{
			values: map[string]dosa.FieldValue{key: encodedValue},
			err:    assert.AnError,
		},
		expectedErr:      rangeErr,
		expectedManyResp: rangeResponse,
		expectedTok:      rangeTok,
		encoder:          staticEncoder{},
	}
}

func createRangeFallbackBadValueTestCase() testCase {
	rangeResponse := []map[string]dosa.FieldValue{{"a": "b"}}
	rangeTok := "nextToken"
	rangeErr := errors.New("origin error")
	return testCase{
		description:    "Bad value from fallback should return origin results",
		cachedEntities: cacheableEntities,
		originRange: &rangeArgs{
			token:     "token",
			limit:     2,
			resp:      rangeResponse,
			nextToken: rangeTok,
			err:       rangeErr,
		},
		fallbackRead: &expectArgs{
			values: map[string]dosa.FieldValue{key: encodedValue},
			resp:   nil,
		},
		expectedErr:      rangeErr,
		expectedManyResp: rangeResponse,
		expectedTok:      rangeTok,
		encoder:          staticEncoder{},
	}
}

// Test scan calls Range
func TestScan(t *testing.T) {
	originCtrl := gomock.NewController(t)
	defer originCtrl.Finish()
	mockOrigin := mocks.NewMockConnector(originCtrl)

	rangeResponse := []map[string]dosa.FieldValue{{"a": "b"}}
	rangeTok := "nextToken"
	mockOrigin.EXPECT().Range(context.TODO(), testEi, nil, dosa.All(), "token", 2).Return(rangeResponse, rangeTok, nil)

	connector := NewConnector(mockOrigin, memory.NewConnector(), nil)
	resp, tok, err := connector.Scan(context.TODO(), testEi, []string{}, "token", 2)
	assert.NoError(t, err)
	assert.EqualValues(t, rangeResponse, resp)
	assert.EqualValues(t, rangeTok, tok)
}

// Test remove from origin also removes from fallback. Does not matter if origin has an error or not
func TestRemove(t *testing.T) {
	originCtrl := gomock.NewController(t)
	defer originCtrl.Finish()
	mockOrigin := mocks.NewMockConnector(originCtrl)

	fallbackCtrl := gomock.NewController(t)
	defer fallbackCtrl.Finish()
	mockFallback := mocks.NewMockConnector(fallbackCtrl)

	keys := map[string]dosa.FieldValue{}
	mockOrigin.EXPECT().Remove(context.TODO(), testEi, keys).Return(assert.AnError)
	mockFallback.EXPECT().Remove(gomock.Not(context.TODO()), adaptedEi, gomock.Any()).Return(nil)

	connector := NewConnector(mockOrigin, mockFallback, nil, cacheableEntities...)
	connector.setSynchronousMode(true)
	err := connector.Remove(context.TODO(), testEi, keys)
	assert.Error(t, err)
}

// Test that if a Connector interface method is not defined in fallback.Connector, revert to
// using the origin's implementation of the method
func TestCreateIfNotExists(t *testing.T) {
	originCtrl := gomock.NewController(t)
	defer originCtrl.Finish()
	mockOrigin := mocks.NewMockConnector(originCtrl)

	values := map[string]dosa.FieldValue{}
	mockOrigin.EXPECT().CreateIfNotExists(context.TODO(), testEi, values).Return(nil)

	connector := NewConnector(mockOrigin, nil, nil, cacheableEntities...)
	connector.setSynchronousMode(true)
	err := connector.CreateIfNotExists(context.TODO(), testEi, values)
	assert.NoError(t, err)
}

// Test read and write against actual redis fallback.
// First read successfully to origin, which should populate the entry into redis cache
// Then force origin to fail, and verify that it returns the value from redis
func TestFallbackEndToEnd(t *testing.T) {
	if !redis.IsRunning() {
		t.Skip("Redis is not running")
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockDownstreamConnector := mocks.NewMockConnector(ctrl)

	testRedisConfig := redis.Config{
		ServerSettings: redis.ServerConfig{
			Host: "localhost",
			Port: redis.RedisPort,
		},
		TTL: 15 * time.Second,
	}
	redisC := redis.NewConnector(testRedisConfig, nil)

	testUUID := dosa.UUID("d1449c93-25b8-4032-920b-60471d91acc9")
	testStr := "test string"
	testStr2 := "another test string"
	testInt64 := int64(29385235)
	testInt32 := int32(232)
	testFloat64 := float64(999.88)
	var testbool bool
	testTime := time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC)
	testBytes := []byte("test value byte array")

	values := map[string]dosa.FieldValue{
		// primary key
		"an_uuid_key": testUUID,
		"strkey":      testStr,
		"int64key":    testInt64,

		// values
		"uuidv":          testUUID,
		"strv":           testStr2,
		"an_int64_value": testInt64,
		"int32v":         testInt32,
		"doublev":        testFloat64,
		"boolv":          testbool,
		"blobv":          testBytes,
		"tsv":            testTime,

		// pointers
		"uuidvp":   &testUUID,
		"strvp":    &testStr,
		"int64vp":  &testInt64,
		"int32vp":  &testInt32,
		"doublevp": &testFloat64,
		"boolvp":   &testbool,
		"tsvp":     &testTime,
	}

	// Fake origin read succeeding, which will upsert the result into redis
	mockDownstreamConnector.EXPECT().Read(context.TODO(), testEi, values, dosa.All()).Return(values, nil)
	// Fake origin read failing, and then read from redis
	mockDownstreamConnector.EXPECT().Read(context.TODO(), testEi, values, dosa.All()).Return(nil, assert.AnError)

	connector := NewConnector(mockDownstreamConnector, redisC, nil, cacheableEntities...)
	connector.setSynchronousMode(true)

	_, err := connector.Read(context.TODO(), testEi, values, nil)
	assert.NoError(t, err)

	resp, err := connector.Read(context.TODO(), testEi, values, nil)
	assert.NoError(t, err)
	assert.NotEmpty(t, resp)
	expectedResult := map[string]dosa.FieldValue{
		// primary key
		"an_uuid_key": &testUUID,
		"strkey":      &testStr,
		"int64key":    &testInt64,

		// values
		"uuidv":          &testUUID,
		"strv":           &testStr2,
		"an_int64_value": &testInt64,
		"int32v":         &testInt32,
		"doublev":        &testFloat64,
		"boolv":          &testbool,
		"blobv":          testBytes,
		"tsv":            &testTime,

		// pointers
		"uuidvp":   &testUUID,
		"strvp":    &testStr,
		"int64vp":  &testInt64,
		"int32vp":  &testInt32,
		"doublevp": &testFloat64,
		"boolvp":   &testbool,
		"tsvp":     &testTime,
	}

	assert.EqualValues(t, expectedResult, resp)
}

// Test the internal method for serializing a cache key
func TestCreateCacheKey(t *testing.T) {
	values := map[string]dosa.FieldValue{
		"an_uuid_key": "d1449c93-25b8-4032-920b-60471d91acc9",
		"strv":        "test value string",
		"boolv":       false,
		"int64key":    2932,
		"blobv":       []byte("test value byte array"),
		"strkey":      "test key string",
	}
	key := createCacheKey(testEi, values)
	assert.Equal(t, key, []map[string]dosa.FieldValue{{"an_uuid_key": "d1449c93-25b8-4032-920b-60471d91acc9"}, {"int64key": 2932}, {"strkey": "test key string"}})
}

// Test that creating cacheable entities set ignores entities in the list that are invalid
func TestCacheableEntities(t *testing.T) {
	set := createCachedEntitiesSet([]dosa.DomainObject{&testentity.TestEntity{}, &dosa.Entity{}})
	assert.Len(t, set, 1)
}

// Test creating cacheable entities set
func TestSettingCachedEntities(t *testing.T) {
	e1 := struct {
		dosa.Entity `dosa:"name=e1, primaryKey=(Hello)"`
		Hello       string
	}{}
	e2 := struct {
		dosa.Entity `dosa:"name=e2, primaryKey=(World)"`
		World       string
	}{}
	connector := NewConnector(memory.NewConnector(), memory.NewConnector(), nil, &e1, &e2)
	assert.Len(t, connector.cacheableEntities, 2)
	assert.Contains(t, connector.cacheableEntities, "e1")
	assert.Contains(t, connector.cacheableEntities, "e2")
	connector.SetCachedEntities(nil)
	assert.Empty(t, connector.cacheableEntities)
}

func TestWriteKeyValueToFallback(t *testing.T) {
	connector := NewConnector(memory.NewConnector(), memory.NewConnector(), nil)
	err := connector.writeKeyValueToFallback(context.TODO(), testEi, "a", nil)
	// Should error on being unable to encode value
	assert.Error(t, err)
}

func TestRawRowAsPointers(t *testing.T) {
	e1 := struct {
		dosa.Entity `dosa:"name=e1, primaryKey=(Hello)"`
		Hello       string
	}{}
	table, _ := dosa.TableFromInstance(&e1)
	ei := &dosa.EntityInfo{Ref: &schemaRef, Def: &table.EntityDefinition}
	// Change the column's type to unknown type
	ei.Def.Columns[0].Type = dosa.Invalid

	world := dosa.FieldValue("world")
	resp := rawRowAsPointers(ei, map[string]dosa.FieldValue{"hello": world})
	assert.Equal(t, map[string]dosa.FieldValue{"hello": &world}, resp)
}
