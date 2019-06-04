// Copyright (c) 2019 Uber Technologies, Inc.
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
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/devnull"
	"github.com/uber-go/dosa/mocks"
)

type ClientTestEntity struct {
	dosa.Entity `dosa:"primaryKey=(ID)"`
	dosa.Index  `dosa:"key=Name, name=username"`
	ID          int64
	Name        string
	Email       string
}

var (
	table, _      = dosa.FindEntityByName(".", "ClientTestEntity")
	ctx           = context.TODO()
	scope         = "test"
	namePrefix    = "team.service"
	nullConnector = devnull.NewConnector()
	query1        = &queryObj{fieldName: "ID", colName: "id", op: "eq", valueStr: "10", value: dosa.FieldValue(int64(10))}
	query2        = &queryObj{fieldName: "ID", colName: "id", op: "lt", valueStr: "10", value: dosa.FieldValue(int64(10))}
	query3        = &queryObj{fieldName: "ID", colName: "id", op: "ne", valueStr: "10", value: dosa.FieldValue(int64(10))}
)

func TestNewClient(t *testing.T) {
	// initialize registrar
	reg, err := newSimpleRegistrar(scope, namePrefix, table)
	assert.NoError(t, err)
	assert.NotNil(t, reg)

	// initialize a pseudo-connected client
	client := newShellQueryClient(reg, nullConnector)
	err = client.Initialize(ctx)
	assert.NoError(t, err)
}

func TestClient_Initialize(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	reg, _ := newSimpleRegistrar(scope, namePrefix, table)

	// CheckSchema error
	errConn := mocks.NewMockConnector(ctrl)
	errConn.EXPECT().CheckSchema(ctx, gomock.Any(), gomock.Any(), gomock.Any()).Return(int32(-1), errors.New("CheckSchema error")).AnyTimes()
	c1 := newShellQueryClient(reg, errConn)
	assert.Error(t, c1.Initialize(ctx))

	// success case
	c2 := dosa.NewClient(reg, nullConnector)
	assert.NoError(t, c2.Initialize(ctx))
}
