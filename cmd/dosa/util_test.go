// Copyright (c) 2018 Uber Technologies, Inc.
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

package main

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
)

func TestParseQuery(t *testing.T) {
	cases := []struct {
		inExps     []string
		expQueries []*queryObj
		expErr     bool
	}{
		{
			[]string{"StrKey:eq:foo"},
			[]*queryObj{
				{
					fieldName: "StrKey",
					op:        "eq",
					valueStr:  "foo",
				},
			},
			false,
		}, {
			[]string{"StrKey:eq"},
			nil,
			true,
		}, {
			[]string{"StrKey:eq:foo", "StrKey:eq"},
			nil,
			true,
		},
	}

	for _, c := range cases {
		outQueries, err := parseQuery(c.inExps)
		assert.True(t, reflect.DeepEqual(c.expQueries, outQueries))
		assert.Equal(t, c.expErr, err != nil)
	}
}

func TestSetQueryFieldValues(t *testing.T) {
	table, _ := dosa.FindEntityByName("../../testentity", "TestEntity")
	re := dosa.NewRegisteredEntity(scope, namePrefix, table)

	// success case
	query := newQueryObj("StrKey", "eq", "foo")
	queries, err := setQueryFieldValues([]*queryObj{query}, re)
	assert.NotNil(t, queries)
	assert.NoError(t, err)
	assert.Len(t, queries, 1)
	assert.Equal(t, dosa.FieldValue("foo"), queries[0].value)

	// bad case
	query = newQueryObj("BadKey", "eq", "foo")
	queries, err = setQueryFieldValues([]*queryObj{query}, re)
	assert.Nil(t, queries)
	assert.Contains(t, err.Error(), "is not a valid field for")
}

func TestGetfields(t *testing.T) {
	fields := []map[string]dosa.FieldValue{
		{"id": dosa.FieldValue(int32(1)), "name": dosa.FieldValue("foo")},
		{"name": dosa.FieldValue("bar"), "address": dosa.FieldValue("bar")},
	}
	assert.Equal(t, getFields(fields), []string{"address", "id", "name"})
}

func TestPrintResults(t *testing.T) {
	results := []map[string]dosa.FieldValue{
		{
			"id":   dosa.FieldValue(int32(1)),
			"uuid": dosa.FieldValue(dosa.UUID("3e4befa0-69d3-11e8-95b0-d55aa227a290")),
		},
	}
	err := printResults(results)
	assert.NoError(t, err)
}

func TestPrintResultsEmpty(t *testing.T) {
	results := []map[string]dosa.FieldValue{}
	err := printResults(results)
	assert.Error(t, err)
}

func TestStrToFieldValue(t *testing.T) {
	tDateSec := "2018-06-11T13:03:01Z"
	tDateMsec := "2018-06-11T13:03:01.000Z"
	tUnixMsec := "1528722181000"
	ts, _ := time.Parse(time.RFC3339Nano, tDateSec)
	cases := []struct {
		inType dosa.Type
		inStr  string
		expFv  dosa.FieldValue
	}{
		{dosa.Int32, "42", dosa.FieldValue(int32(42))},
		{dosa.Int64, "42", dosa.FieldValue(int64(42))},
		{dosa.Bool, "false", dosa.FieldValue(false)},
		{dosa.String, "42", dosa.FieldValue("42")},
		{dosa.Double, "42.00", dosa.FieldValue(float64(42))},
		{dosa.Timestamp, tDateSec, dosa.FieldValue(ts)},
		{dosa.Timestamp, tDateMsec, dosa.FieldValue(ts)},
		{dosa.Timestamp, tUnixMsec, dosa.FieldValue(ts)},
		{dosa.TUUID, "3e4befa0-69d3-11e8-95b0-d55aa227a290", dosa.FieldValue(dosa.UUID("3e4befa0-69d3-11e8-95b0-d55aa227a290"))},
	}

	for _, c := range cases {
		outFv, err := strToFieldValue(c.inType, c.inStr)
		assert.Equal(t, c.expFv, outFv)
		assert.Nil(t, err)
	}
}
