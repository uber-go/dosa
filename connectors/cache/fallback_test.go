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
)

func createTestEi(sr dosa.SchemaRef) *dosa.EntityInfo {
	table, _ := dosa.TableFromInstance(&testentity.TestEntity{})
	testEi := &dosa.EntityInfo{Ref: &sr, Def: &table.EntityDefinition}
	return testEi
}

type testCase struct {
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

func TestUpsertCases(t *testing.T) {
	runTestCase := func(tc testCase) {
		originCtrl := gomock.NewController(t)
		defer originCtrl.Finish()
		mockOrigin := mocks.NewMockConnector(originCtrl)

		fallbackCtrl := gomock.NewController(t)
		defer fallbackCtrl.Finish()
		mockFallback := mocks.NewMockConnector(fallbackCtrl)

		mockOrigin.EXPECT().Upsert(context.TODO(), testEi, tc.originUpsert.values).Return(nil)
		if tc.fallbackUpsert != nil {
			// use gomock.Any() because gob encoding of maps is non-deterministic so could be a different argument value every time
			mockFallback.EXPECT().Upsert(gomock.Not(context.TODO()), adaptedEi, gomock.Any()).Return(nil).AnyTimes()
		}

		connector := NewConnector(mockOrigin, mockFallback, nil, cacheableEntities...)
		connector.setSynchronousMode(true)
		err := connector.Upsert(context.TODO(), testEi, tc.originUpsert.values)
		assert.NoError(t, err, tc.description)
	}

	var testBool bool

	testCases := []testCase{
		{
			description: "Successful origin upsert also upserts to fallback",
			originUpsert: &expectArgs{
				values: map[string]dosa.FieldValue{
					"an_uuid_key": "d1449c93-25b8-4032-920b-60471d91acc9",
					"strkey":      "test key string",
					"StrV":        "test value string",
					"BoolVP":      &testBool,
				}},
			fallbackUpsert: &expectArgs{
				values: map[string]dosa.FieldValue{
					// gob encoding of [{"an_uuid_key":"d1449c93-25b8-4032-920b-60471d91acc9"},{"strkey":"test key string"}]
					key: []byte{13, 255, 131, 2, 1, 2, 255, 132, 0, 1, 255, 130, 0, 0, 14, 255, 129, 4, 1, 2, 255, 130, 0, 1, 12, 1, 16, 0, 0, 98, 255, 132, 0, 2, 1, 11, 97, 110, 95, 117, 117, 105, 100, 95, 107, 101, 121, 6, 115, 116, 114, 105, 110, 103, 12, 38, 0, 36, 100, 49, 52, 52, 57, 99, 57, 51, 45, 50, 53, 98, 56, 45, 52, 48, 51, 50, 45, 57, 50, 48, 98, 45, 54, 48, 52, 55, 49, 100, 57, 49, 97, 99, 99, 57, 1, 6, 115, 116, 114, 107, 101, 121, 6, 115, 116, 114, 105, 110, 103, 12, 17, 0, 15, 116, 101, 115, 116, 32, 107, 101, 121, 32, 115, 116, 114, 105, 110, 103},
					// gob encoding of {"BoolV":false,"StrV":"test value string","an_uuid_key":"d1449c93-25b8-4032-920b-60471d91acc9","strkey":"test key string"}
					value: []byte{14, 255, 129, 4, 1, 2, 255, 130, 0, 1, 12, 1, 16, 0, 0, 255, 145, 255, 130, 0, 4, 11, 97, 110, 95, 117, 117, 105, 100, 95, 107, 101, 121, 6, 115, 116, 114, 105, 110, 103, 12, 38, 0, 36, 100, 49, 52, 52, 57, 99, 57, 51, 45, 50, 53, 98, 56, 45, 52, 48, 51, 50, 45, 57, 50, 48, 98, 45, 54, 48, 52, 55, 49, 100, 57, 49, 97, 99, 99, 57, 6, 115, 116, 114, 107, 101, 121, 6, 115, 116, 114, 105, 110, 103, 12, 17, 0, 15, 116, 101, 115, 116, 32, 107, 101, 121, 32, 115, 116, 114, 105, 110, 103, 4, 83, 116, 114, 86, 6, 115, 116, 114, 105, 110, 103, 12, 19, 0, 17, 116, 101, 115, 116, 32, 118, 97, 108, 117, 101, 32, 115, 116, 114, 105, 110, 103, 6, 66, 111, 111, 108, 86, 80, 4, 98, 111, 111, 108, 2, 2, 0, 0},
				},
			},
		},
		{
			description: "Encoding error while creating cache key means no upsert to fallback",
			originUpsert: &expectArgs{
				values: map[string]dosa.FieldValue{
					"an_uuid_key": "d1449c93-25b8-4032-920b-60471d91acc9",
					"strkey":      "test key string",
					"StrV":        errors.New("some unknown value type"),
					"BoolV":       false,
				}},
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
				mockFallback.EXPECT().Upsert(gomock.Not(context.TODO()), adaptedEi, gomock.Any()).Return(tc.fallbackUpsert.err)
			}

			connector := NewConnector(mockOrigin, mockFallback, nil, tc.cachedEntities...)
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
		createReadEncodeErrorTestCase(),
		createReadDecodeErrorTestCase(),
		createReadFallbackFailTestCase(),
		createReadFallbackBadValueTestCase(),
	}
	for _, tc := range testCases {
		runTestCase(tc)
	}
}

func createReadSuccessTestCase() testCase {
	originResponse := map[string]dosa.FieldValue{"a": "b"}
	// gob encoding of {"a":"b","primaryKey":"primaryValue"}
	// note that gob encoding of maps is non-deterministic so the above map could be different bytes
	gobEncodedResp := []byte{14, 255, 129, 4, 1, 2, 255, 130, 0, 1, 12, 1, 16, 0, 0, 52, 255, 130, 0, 2, 1, 97, 6, 115, 116, 114, 105, 110, 103, 12, 3, 0, 1, 98, 10, 112, 114, 105, 109, 97, 114, 121, 75, 101, 121, 6, 115, 116, 114, 105, 110, 103, 12, 14, 0, 12, 112, 114, 105, 109, 97, 114, 121, 86, 97, 108, 117, 101}

	return testCase{
		cachedEntities: cacheableEntities,
		originRead: &expectArgs{
			values: map[string]dosa.FieldValue{"primaryKey": "primaryValue"},
			resp:   originResponse,
		},
		fallbackUpsert: &expectArgs{
			values: map[string]dosa.FieldValue{
				key:   []byte{},
				value: gobEncodedResp,
			},
		},
		expectedResp: originResponse,
		expectedErr:  nil,
		description:  "Test read from origin succeeds, should write response to fallback that includes primary keys",
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
	resp := int32(7)
	// {"int32v": 7} in gob form
	gobEncoded := []byte{14, 255, 129, 4, 1, 2, 255, 130, 0, 1, 12, 1, 16, 0, 0, 21, 255, 130, 0, 1, 6, 105, 110, 116, 51, 50, 118, 5, 105, 110, 116, 51, 50, 4, 2, 0, 14}
	return testCase{
		cachedEntities: cacheableEntities,
		originRead: &expectArgs{
			err: assert.AnError,
		},
		fallbackRead: &expectArgs{
			values: map[string]dosa.FieldValue{key: []byte{}},
			resp:   map[string]dosa.FieldValue{"value": gobEncoded},
		},
		expectedResp: map[string]dosa.FieldValue{"int32v": &resp},
		expectedErr:  nil,
		description:  "Test that when read origin has error, we return from the fallback",
	}
}

func createReadNotFoundTestCase() testCase {
	originResponse := map[string]dosa.FieldValue{"a": "b"}
	originErr := &dosa.ErrNotFound{}
	return testCase{
		cachedEntities: cacheableEntities,
		originRead: &expectArgs{
			err:  originErr,
			resp: originResponse,
		},
		expectedResp: originResponse,
		expectedErr:  originErr,
		description:  "Test that when read origin has err not found, do not read from fallback, return origin response and error",
	}
}

func createReadEncodeErrorTestCase() testCase {
	originResponse := map[string]dosa.FieldValue{"a": "b"}

	return testCase{
		cachedEntities: cacheableEntities,
		originRead: &expectArgs{
			values: map[string]dosa.FieldValue{"strkey": errors.New("Bad key value")},
			resp:   originResponse,
		},
		expectedResp: originResponse,
		expectedErr:  nil,
		description:  "If an encoding error occurs while creating encoded values for the fallback, do not upsert into fallback",
	}
}

func createReadDecodeErrorTestCase() testCase {
	originResponse := map[string]dosa.FieldValue{"a": "b"}
	originErr := errors.New("origin error")

	return testCase{
		cachedEntities: cacheableEntities,
		originRead: &expectArgs{
			resp: originResponse,
			err:  originErr,
		},
		fallbackRead: &expectArgs{
			values: map[string]dosa.FieldValue{key: []byte{}},
			resp:   map[string]dosa.FieldValue{"value": []byte("not a gob encoded byte string")},
		},
		expectedResp: originResponse,
		expectedErr:  originErr,
		description:  "If an error occurs while trying to decode the response from the fallback, return the original response",
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
			values: map[string]dosa.FieldValue{key: []byte{}},
			err:    errors.New("fallback error"),
		},
		expectedResp: originResponse,
		expectedErr:  originErr,
		description:  "Test read origin has error and fallback also fails. Should return the origin error",
	}
}

func createReadFallbackBadValueTestCase() testCase {
	originResponse := map[string]dosa.FieldValue{"a": "b"}
	originErr := errors.New("origin error")

	return testCase{
		cachedEntities: cacheableEntities,
		originRead: &expectArgs{
			resp: originResponse,
			err:  originErr,
		},
		fallbackRead: &expectArgs{
			values: map[string]dosa.FieldValue{key: []byte{}},
			// fallback returns a response with no value field
			resp: nil,
		},
		expectedResp: originResponse,
		expectedErr:  originErr,
		description:  "When fallback response is empty/corrupted, return the response from origin",
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
				mockFallback.EXPECT().Read(context.TODO(), adaptedEi, gomock.Any(), dosa.All()).Return(tc.fallbackRead.resp, tc.fallbackRead.err)
			}
			if tc.fallbackUpsert != nil {
				mockFallback.EXPECT().Upsert(gomock.Not(context.TODO()), adaptedEi, gomock.Any()).Return(tc.fallbackUpsert.err)
			}

			connector := NewConnector(mockOrigin, mockFallback, nil, tc.cachedEntities...)
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
				key:   []byte(`{"Conditions":[{"Name":"column","Condition":{"Op":5,"Value":"columnVal"}}],"Token":"token","Limit":2}`),
				value: []byte(`{"Rows":[{"a":"b"}],"TokenNext":"nextToken"}`),
			},
			err: nil,
		},
		expectedErr:      nil,
		expectedManyResp: rangeResponse,
		expectedTok:      rangeTok,
		description:      "Test range from origin succeeds, should write response to cache",
	}
}

func createRangeUncachedEntityTestCase() testCase {
	rangeResponse := []map[string]dosa.FieldValue{{"a": "b"}}
	rangeTok := "nextToken"
	conditions := map[string][]*dosa.Condition{"column": {{Op: dosa.GtOrEq, Value: "columnVal"}}}

	return testCase{
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
		description:      "When caching no entities, should return result from origin, and fallback is never called",
	}
}

func createRangeFailTestCase() testCase {
	conditions := map[string][]*dosa.Condition{"column": {{Op: dosa.GtOrEq, Value: "columnVal"}}}
	// gob encoded version of response: "{\"rows\": [{\"int32v\": 7}], \"tokenNext\": \"nextToken\"}")}
	fallbackResponse := map[string]dosa.FieldValue{"value": []byte{50, 255, 131, 3, 1, 1, 12, 114, 97, 110, 103, 101, 82, 101, 115, 117, 108, 116, 115, 1, 255, 132, 0, 1, 2, 1, 4, 82, 111, 119, 115, 1, 255, 134, 0, 1, 9, 84, 111, 107, 101, 110, 78, 101, 120, 116, 1, 12, 0, 0, 0, 40, 255, 133, 2, 1, 1, 25, 91, 93, 109, 97, 112, 91, 115, 116, 114, 105, 110, 103, 93, 105, 110, 116, 101, 114, 102, 97, 99, 101, 32, 123, 125, 1, 255, 134, 0, 1, 255, 130, 0, 0, 14, 255, 129, 4, 1, 2, 255, 130, 0, 1, 12, 1, 16, 0, 0, 34, 255, 132, 1, 1, 1, 6, 105, 110, 116, 51, 50, 118, 5, 105, 110, 116, 51, 50, 4, 2, 0, 14, 1, 9, 110, 101, 120, 116, 84, 111, 107, 101, 110, 0}}
	// gob encoded version of key: {"Conditions":[{"Name":"column","Condition":{"Op":5,"Value":"columnVal"}}],"Token":"token","Limit":2}`
	gobKey := []byte{60, 255, 129, 3, 1, 1, 10, 114, 97, 110, 103, 101, 81, 117, 101, 114, 121, 1, 255, 130, 0, 1, 3, 1, 10, 67, 111, 110, 100, 105, 116, 105, 111, 110, 115, 1, 255, 136, 0, 1, 5, 84, 111, 107, 101, 110, 1, 12, 0, 1, 5, 76, 105, 109, 105, 116, 1, 4, 0, 0, 0, 38, 255, 135, 2, 1, 1, 23, 91, 93, 42, 100, 111, 115, 97, 46, 67, 111, 108, 117, 109, 110, 67, 111, 110, 100, 105, 116, 105, 111, 110, 1, 255, 136, 0, 1, 255, 132, 0, 0, 36, 255, 131, 3, 1, 2, 255, 132, 0, 1, 2, 1, 4, 78, 97, 109, 101, 1, 12, 0, 1, 9, 67, 111, 110, 100, 105, 116, 105, 111, 110, 1, 255, 134, 0, 0, 0, 40, 255, 133, 3, 1, 1, 9, 67, 111, 110, 100, 105, 116, 105, 111, 110, 1, 255, 134, 0, 1, 2, 1, 2, 79, 112, 1, 4, 0, 1, 5, 86, 97, 108, 117, 101, 1, 16, 0, 0, 0, 48, 255, 130, 1, 1, 1, 6, 99, 111, 108, 117, 109, 110, 1, 1, 10, 1, 6, 115, 116, 114, 105, 110, 103, 12, 11, 0, 9, 99, 111, 108, 117, 109, 110, 86, 97, 108, 0, 0, 1, 5, 116, 111, 107, 101, 110, 1, 4, 0}
	fieldVal := int32(7)

	return testCase{
		cachedEntities: cacheableEntities,
		originRange: &rangeArgs{
			columnConditions: conditions,
			token:            "token",
			limit:            2,
			err:              assert.AnError,
		},
		fallbackRead: &expectArgs{
			values: map[string]dosa.FieldValue{
				key: gobKey,
			},
			resp: fallbackResponse,
		},
		expectedErr:      nil,
		expectedManyResp: []map[string]dosa.FieldValue{{"int32v": &fieldVal}},
		expectedTok:      "nextToken",
		description:      "Test range from origin has error and fallback succeeds",
	}
}

func createRangeNotFoundTestCase() testCase {
	conditions := map[string][]*dosa.Condition{"column": {{Op: dosa.GtOrEq, Value: "columnVal"}}}
	rangeResponse := []map[string]dosa.FieldValue{{"a": "b"}}
	rangeTok := "nextToken"
	rangeErr := &dosa.ErrNotFound{}

	return testCase{
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
		description:      "Test that when origin has err not found, do not read from fallback, return origin response and error",
	}
}

func createRangeEncodeErrorTestCase() testCase {
	rangeResponse := []map[string]dosa.FieldValue{{"a": errors.New("Unknown type")}}
	rangeTok := "nextToken"
	conditions := map[string][]*dosa.Condition{"column": {{Op: dosa.GtOrEq, Value: "columnVal"}}}

	return testCase{
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
		description:      "If an encoding error occurs while creating encoded values for the fallback, do not upsert into fallback",
	}
}

func createRangeDecodeErrorTestCase() testCase {
	conditions := map[string][]*dosa.Condition{"column": {{Op: dosa.GtOrEq, Value: errors.New("some unknown type")}}}
	rangeResponse := []map[string]dosa.FieldValue{{"a": "b"}}
	rangeTok := "nextToken"
	rangeErr := errors.New("origin error")
	fallbackResponse := map[string]dosa.FieldValue{"value": []byte("bad cache value")}

	return testCase{
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
			values: map[string]dosa.FieldValue{key: []byte{}},
			resp:   fallbackResponse,
		},
		expectedErr:      rangeErr,
		expectedManyResp: rangeResponse,
		expectedTok:      rangeTok,
		description:      "Bad decoding of fallback response should result in returning the original response",
	}
}

func createRangeFallbackFailTestCase() testCase {
	rangeResponse := []map[string]dosa.FieldValue{{"a": "b"}}
	rangeTok := "nextToken"
	rangeErr := errors.New("origin error")
	// gob encoded key {"Token":"token","Limit":2}
	gobKey := []byte{60, 255, 129, 3, 1, 1, 10, 114, 97, 110, 103, 101, 81, 117, 101, 114, 121, 1, 255, 130, 0, 1, 3, 1, 10, 67, 111, 110, 100, 105, 116, 105, 111, 110, 115, 1, 255, 136, 0, 1, 5, 84, 111, 107, 101, 110, 1, 12, 0, 1, 5, 76, 105, 109, 105, 116, 1, 4, 0, 0, 0, 38, 255, 135, 2, 1, 1, 23, 91, 93, 42, 100, 111, 115, 97, 46, 67, 111, 108, 117, 109, 110, 67, 111, 110, 100, 105, 116, 105, 111, 110, 1, 255, 136, 0, 1, 255, 132, 0, 0, 36, 255, 131, 3, 1, 2, 255, 132, 0, 1, 2, 1, 4, 78, 97, 109, 101, 1, 12, 0, 1, 9, 67, 111, 110, 100, 105, 116, 105, 111, 110, 1, 255, 134, 0, 0, 0, 40, 255, 133, 3, 1, 1, 9, 67, 111, 110, 100, 105, 116, 105, 111, 110, 1, 255, 134, 0, 1, 2, 1, 2, 79, 112, 1, 4, 0, 1, 5, 86, 97, 108, 117, 101, 1, 16, 0, 0, 0, 12, 255, 130, 2, 5, 116, 111, 107, 101, 110, 1, 4, 0}

	return testCase{
		cachedEntities: cacheableEntities,
		originRange: &rangeArgs{
			token:     "token",
			limit:     2,
			resp:      rangeResponse,
			nextToken: rangeTok,
			err:       rangeErr,
		},
		fallbackRead: &expectArgs{
			values: map[string]dosa.FieldValue{
				key: gobKey,
			},
			err: assert.AnError,
		},
		expectedErr:      rangeErr,
		expectedManyResp: rangeResponse,
		expectedTok:      rangeTok,
		description:      "Test range from origin has error and fallback to cache fails. Should return origin results",
	}
}

func createRangeFallbackBadValueTestCase() testCase {
	rangeResponse := []map[string]dosa.FieldValue{{"a": "b"}}
	rangeTok := "nextToken"
	rangeErr := errors.New("origin error")
	// gob encoded key {"Token":"token","Limit":2}
	gobKey := []byte{60, 255, 129, 3, 1, 1, 10, 114, 97, 110, 103, 101, 81, 117, 101, 114, 121, 1, 255, 130, 0, 1, 3, 1, 10, 67, 111, 110, 100, 105, 116, 105, 111, 110, 115, 1, 255, 136, 0, 1, 5, 84, 111, 107, 101, 110, 1, 12, 0, 1, 5, 76, 105, 109, 105, 116, 1, 4, 0, 0, 0, 38, 255, 135, 2, 1, 1, 23, 91, 93, 42, 100, 111, 115, 97, 46, 67, 111, 108, 117, 109, 110, 67, 111, 110, 100, 105, 116, 105, 111, 110, 1, 255, 136, 0, 1, 255, 132, 0, 0, 36, 255, 131, 3, 1, 2, 255, 132, 0, 1, 2, 1, 4, 78, 97, 109, 101, 1, 12, 0, 1, 9, 67, 111, 110, 100, 105, 116, 105, 111, 110, 1, 255, 134, 0, 0, 0, 40, 255, 133, 3, 1, 1, 9, 67, 111, 110, 100, 105, 116, 105, 111, 110, 1, 255, 134, 0, 1, 2, 1, 2, 79, 112, 1, 4, 0, 1, 5, 86, 97, 108, 117, 101, 1, 16, 0, 0, 0, 12, 255, 130, 2, 5, 116, 111, 107, 101, 110, 1, 4, 0}

	return testCase{
		cachedEntities: cacheableEntities,
		originRange: &rangeArgs{
			token:     "token",
			limit:     2,
			resp:      rangeResponse,
			nextToken: rangeTok,
			err:       rangeErr,
		},
		fallbackRead: &expectArgs{
			values: map[string]dosa.FieldValue{
				key: gobKey,
			},
			resp: nil,
		},
		expectedErr:      rangeErr,
		expectedManyResp: rangeResponse,
		expectedTok:      rangeTok,
		description:      "Bad value from fallback should return origin results",
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
	transformedKeys := map[string]dosa.FieldValue{key: []byte{}}
	mockOrigin.EXPECT().Remove(context.TODO(), testEi, keys).Return(nil)
	mockFallback.EXPECT().Remove(gomock.Not(context.TODO()), adaptedEi, transformedKeys).Return(nil)

	connector := NewConnector(mockOrigin, mockFallback, nil, cacheableEntities...)
	connector.setSynchronousMode(true)
	err := connector.Remove(context.TODO(), testEi, keys)
	assert.NoError(t, err)
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

// Test read write against actual redis fallback.
// First upsert successfully to origin and redis.
// On origin read errors, should return result from redis
func TestUpsertRead(t *testing.T) {
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

	// Fake origin upserting succeeding and actually upsert into redis
	mockDownstreamConnector.EXPECT().Upsert(context.TODO(), testEi, values).Return(nil)
	// Fake origin read failing and read from redis
	mockDownstreamConnector.EXPECT().Read(context.TODO(), testEi, values, dosa.All()).Return(nil, assert.AnError)

	connector := NewConnector(mockDownstreamConnector, redisC, nil, cacheableEntities...)
	connector.setSynchronousMode(true)

	err := connector.Upsert(context.TODO(), testEi, values)
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
	key := createCacheKey(testEi, values, encoding.NewJSONEncoder())
	assert.Equal(t, []byte(`[{"an_uuid_key":"d1449c93-25b8-4032-920b-60471d91acc9"},{"int64key":2932},{"strkey":"test key string"}]`), key)
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
