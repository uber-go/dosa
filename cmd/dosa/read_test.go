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
	"context"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/mocks"
)

func TestClient_Read(t *testing.T) {
	reg, _ := newSimpleRegistrar(scope, namePrefix, table)
	fieldsToRead := []string{"ID", "Email"}
	results := map[string]dosa.FieldValue{
		"id":    int64(2),
		"name":  "bar",
		"email": "bar@email.com",
	}

	// success case
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockConn := mocks.NewMockConnector(ctrl)
	mockConn.EXPECT().CheckSchema(ctx, gomock.Any(), gomock.Any(), gomock.Any()).Return(int32(1), nil).AnyTimes()
	mockConn.EXPECT().Read(ctx, gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *dosa.EntityInfo, columnValues map[string]dosa.FieldValue, columnsToRead []string) {
			assert.Equal(t, dosa.FieldValue(int64(10)), columnValues["id"])
			assert.Equal(t, []string{"id", "email"}, columnsToRead)
		}).Return(results, nil).MinTimes(1)
	c := newShellQueryClient(reg, mockConn)
	assert.NoError(t, c.Initialize(ctx))
	fvs, err := c.Read(ctx, []*queryObj{query1}, fieldsToRead, 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(fvs))
	assert.Equal(t, results["id"], fvs[0]["ID"])
	assert.Equal(t, results["name"], fvs[0]["Name"])
	assert.Equal(t, results["email"], fvs[0]["Email"])

	// error in query, input non-supported operators
	fvs, err = c.Read(ctx, []*queryObj{query2}, fieldsToRead, 1)
	assert.Nil(t, fvs)
	assert.Contains(t, err.Error(), "wrong operator used for read")

	// error in column name converting
	fvs, err = c.Read(ctx, []*queryObj{query1}, []string{"badcol"}, 1)
	assert.Nil(t, fvs)
	assert.Contains(t, err.Error(), "badcol")
}

func TestClient_BuildReadArgs(t *testing.T) {
	// success case
	args, err := buildReadArgs([]*queryObj{query1})
	assert.NotNil(t, args)
	assert.NoError(t, err)
	fv, ok := args["id"]
	assert.True(t, ok)
	assert.Equal(t, dosa.FieldValue(int64(10)), fv)

	// fail case, input non-supported operator
	args, err = buildReadArgs([]*queryObj{query2})
	assert.Nil(t, args)
	assert.Contains(t, err.Error(), "wrong operator used for read")
}

func TestClient_ColToFieldName(t *testing.T) {
	colToField := map[string]string{
		"id":    "ID",
		"name":  "Name",
		"email": "Email",
	}

	rowsCol := []map[string]dosa.FieldValue{
		{
			"id":   dosa.FieldValue(int(10)),
			"name": dosa.FieldValue("foo"),
		},
		// contains column "address" which not defined in struct
		{
			"id":      dosa.FieldValue(int(20)),
			"address": dosa.FieldValue("mars"),
		},
	}

	expRowsField := []map[string]dosa.FieldValue{
		{
			"ID":   dosa.FieldValue(int(10)),
			"Name": dosa.FieldValue("foo"),
		},
		// value of column "address" should not be returned
		{
			"ID": dosa.FieldValue(int(20)),
		},
	}

	rowsField := convertColToField(rowsCol, colToField)
	assert.True(t, reflect.DeepEqual(expRowsField, rowsField))
}
