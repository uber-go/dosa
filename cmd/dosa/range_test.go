// Copyright (c) 2020 Uber Technologies, Inc.
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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/mocks"
)

func TestClient_Range(t *testing.T) {
	reg, _ := newSimpleRegistrar(scope, namePrefix, table)
	fieldsToRead := []string{"ID", "Email"}
	results := map[string]dosa.FieldValue{
		"id":    int64(2),
		"name":  "bar",
		"email": "bar@email.com",
	}

	// success case
	resLimit := 10
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockConn := mocks.NewMockConnector(ctrl)
	mockConn.EXPECT().CheckSchema(ctx, gomock.Any(), gomock.Any(), gomock.Any()).Return(int32(1), nil).AnyTimes()
	mockConn.EXPECT().Range(ctx, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *dosa.EntityInfo, _ map[string][]*dosa.Condition, minimumFields []string, token string, limit int) {
			assert.Equal(t, "", token)
			assert.Equal(t, []string{"id", "email"}, minimumFields)
			assert.Equal(t, resLimit, limit)
		}).Return([]map[string]dosa.FieldValue{results}, "", nil).MinTimes(1)
	c := newShellQueryClient(reg, mockConn)
	assert.NoError(t, c.Initialize(ctx))
	fvs, err := c.Range(ctx, []*queryObj{query1, query2}, fieldsToRead, resLimit)
	assert.NoError(t, err)
	assert.NotNil(t, fvs)
	assert.Equal(t, 1, len(fvs))
	assert.Equal(t, results["id"], fvs[0]["ID"])
	assert.Equal(t, results["name"], fvs[0]["Name"])
	assert.Equal(t, results["email"], fvs[0]["Email"])

	// error in query, input non-supported operators
	fvs, err = c.Range(ctx, []*queryObj{query3}, fieldsToRead, resLimit)
	assert.Nil(t, fvs)
	assert.Contains(t, err.Error(), "wrong operator used for range")

	// error in column name converting
	fvs, err = c.Range(ctx, []*queryObj{query1}, []string{"badcol"}, resLimit)
	assert.Nil(t, fvs)
	assert.Contains(t, err.Error(), "badcol")
}

func TestClient_BuildRangeOp(t *testing.T) {
	limit := 1

	// success case
	rop, err := buildRangeOp([]*queryObj{query1, query2}, limit)
	assert.NotNil(t, rop)
	assert.NoError(t, err)
	assert.Equal(t, limit, rop.LimitRows())
	conditions := rop.Conditions()
	assert.Len(t, conditions, 1)
	assert.Len(t, conditions["ID"], 2)

	// fail case, input non-supported operator
	rop, err = buildRangeOp([]*queryObj{query3}, limit)
	assert.Nil(t, rop)
	assert.Contains(t, err.Error(), "wrong operator used for range")
}
