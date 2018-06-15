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

package main

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/mocks"
)

func TestQuery_ServiceDefault(t *testing.T) {
	tcs := []struct {
		serviceName string
		expected    string
	}{
		//  service = "" -> default
		{
			expected: _defServiceName,
		},
		//  service = "foo" -> foo
		{
			serviceName: "foo",
			expected:    "foo",
		},
	}
	for _, tc := range tcs {
		for _, cmd := range []string{"read", "range"} {
			os.Args = []string{
				"dosa",
				"--service", tc.serviceName,
				"query",
				cmd,
				"--namePrefix", "foo",
				"--scope", "bar",
				"--path", "../../testentity",
				"TestEntity",
				"StrKey:eq:foo",
			}
			main()
			assert.Equal(t, tc.expected, options.ServiceName)
		}
	}
}

func TestQuery_Read_Happy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mc := mocks.NewMockConnector(ctrl)
	mc.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, ei *dosa.EntityInfo, keys map[string]dosa.FieldValue, minimumFields []string) {
			assert.NotNil(t, ei)
			assert.Equal(t, dosa.FieldValue("foo"), keys["strkey"])
			assert.Equal(t, []string{"strkey", "int64key"}, minimumFields)
		}).Return(map[string]dosa.FieldValue{}, nil).MinTimes(1)

	mc.EXPECT().Shutdown().Return(nil)

	table, err := dosa.FindEntityByName("../../testentity", "TestEntity")
	assert.NoError(t, err)
	reg, err := newSimpleRegistrar(scope, namePrefix, table)
	assert.NoError(t, err)

	provideClient := func(opts GlobalOptions, scope, prefix, path, structName string) (ShellQueryClient, error) {
		return newShellQueryClient(reg, mc), nil
	}

	queryRead := QueryRead{
		QueryCmd: &QueryCmd{
			QueryOptions: &QueryOptions{
				Fields: "StrKey,Int64Key",
			},
			Scope:         scopeFlag("scope"),
			NamePrefix:    "foo",
			Path:          "../../testentity",
			provideClient: provideClient,
		},
	}
	queryRead.Args.EntityName = "TestEntity"
	queryRead.Args.Queries = []string{"StrKey:eq:foo"}

	err = queryRead.Execute([]string{})
	assert.NoError(t, err)
}

func TestQuery_Range_Happy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mc := mocks.NewMockConnector(ctrl)
	mc.EXPECT().Range(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition, minimumFields []string, token string, limit int) {
			assert.NotNil(t, ei)
			assert.Len(t, columnConditions, 1)
			assert.Len(t, columnConditions["int64key"], 1)
			assert.Equal(t, []string{"strkey", "int64key"}, minimumFields)
		}).Return([]map[string]dosa.FieldValue{}, "", nil)

	mc.EXPECT().Shutdown().Return(nil)

	table, err := dosa.FindEntityByName("../../testentity", "TestEntity")
	assert.NoError(t, err)
	reg, err := newSimpleRegistrar(scope, namePrefix, table)
	assert.NoError(t, err)

	provideClient := func(opts GlobalOptions, scope, prefix, path, structName string) (ShellQueryClient, error) {
		return newShellQueryClient(reg, mc), nil
	}

	queryRange := QueryRange{
		QueryCmd: &QueryCmd{
			QueryOptions: &QueryOptions{
				Fields: "StrKey,Int64Key",
			},
			Scope:         scopeFlag("scope"),
			NamePrefix:    "foo",
			Path:          "../../testentity",
			provideClient: provideClient,
		},
	}
	queryRange.Args.EntityName = "TestEntity"
	queryRange.Args.Queries = []string{"Int64Key:lt:200"}

	err = queryRange.Execute([]string{})
	assert.NoError(t, err)
}

func TestQuery_NewQueryObj(t *testing.T) {
	qo := newQueryObj("StrKey", "eq", "foo")
	assert.NotNil(t, qo)
	assert.Equal(t, "StrKey", qo.fieldName)
	assert.Equal(t, "eq", qo.op)
	assert.Equal(t, "foo", qo.valueStr)
}

func TestQuery_ParseQuery(t *testing.T) {
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

func TestQuery_SetQueryFieldValues(t *testing.T) {
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

func TestQuery_StrToFieldValue(t *testing.T) {
	tStr := "2018-06-11T13:03:01Z"
	ts, _ := time.Parse(time.RFC3339, tStr)
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
		{dosa.Timestamp, tStr, dosa.FieldValue(ts)},
		{dosa.TUUID, "3e4befa0-69d3-11e8-95b0-d55aa227a290", dosa.FieldValue(dosa.UUID("3e4befa0-69d3-11e8-95b0-d55aa227a290"))},
	}

	for _, c := range cases {
		outFv, err := strToFieldValue(c.inType, c.inStr)
		assert.Equal(t, c.expFv, outFv)
		assert.Nil(t, err)
	}
}

func TestQuery_ScopeRequired(t *testing.T) {
	for _, cmd := range []string{"read", "range"} {
		c := StartCapture()
		exit = func(r int) {}
		os.Args = []string{
			"dosa",
			"query",
			cmd,
			"--namePrefix", "foo",
			"--path", "../../testentity",
			"TestEntity",
			"StrKey:eq:foo",
		}
		main()
		assert.Contains(t, c.stop(true), "-s, --scope' was not specified")
	}
}

func TestQuery_PrefixRequired(t *testing.T) {
	for _, cmd := range []string{"read", "range"} {
		c := StartCapture()
		exit = func(r int) {}
		os.Args = []string{
			"dosa",
			"query",
			cmd,
			"--scope", "foo",
			"--path", "../../testentity",
			"TestEntity",
			"StrKey:eq:foo",
		}
		main()
		assert.Contains(t, c.stop(true), "--namePrefix' was not specified")
	}
}

func TestQuery_PathRequired(t *testing.T) {
	for _, cmd := range []string{"read", "range"} {
		c := StartCapture()
		exit = func(r int) {}
		os.Args = []string{
			"dosa",
			"query",
			cmd,
			"--scope", "foo",
			"--namePrefix", "foo",
			"StrKey:eq:foo",
		}
		main()
		assert.Contains(t, c.stop(true), "--path' was not specified")
	}
}

func TestQuery_NoEntityFound(t *testing.T) {
	for _, cmd := range []string{"read", "range"} {
		c := StartCapture()
		exit = func(r int) {}
		os.Args = []string{
			"dosa",
			"query",
			cmd,
			"--scope", "foo",
			"--namePrefix", "foo",
			"--path", "../../testentity",
			"TestEntity1",
			"StrKey:eq:foo",
		}
		main()
		assert.Contains(t, c.stop(true), "no entity named TestEntity1 found")
	}
}
