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

package dosa_test

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

type ClientTestEntity1 struct {
	dosa.Entity `dosa:"primaryKey=(ID)"`
	ID          int64
	Name        string
	Email       string
}

type ClientTestEntity2 struct {
	dosa.Entity `dosa:"primaryKey=(UUID,Color)"`
	UUID        string
	Color       string
	IsActive    bool
	ignoreme    int32
}

var (
	cte1          = &ClientTestEntity1{ID: int64(1), Name: "foo", Email: "foo@uber.com"}
	cte2          = &ClientTestEntity2{UUID: "b1f23fa3-f453-45b4-a5d5-6d73078ac3bd", Color: "blue", IsActive: true}
	ctx           = context.TODO()
	scope         = "test"
	namePrefix    = "team.service"
	nullConnector = &devnull.Connector{}
	rootFQN, _    = dosa.ToFQN(scope)
	childFQN, _   = rootFQN.Child(namePrefix)
	cte1FQN, _    = childFQN.Child("clienttestentity1")
	cte2FQN, _    = childFQN.Child("clienttestentity2")
)

func ExampleNewClient() {
	// initialize registrar
	reg, err := dosa.NewRegistrar("test", "myteam.myservice", cte1)
	if err != nil {
		// registration will fail if the object is tagged incorrectly
		panic("dosa.NewRegister returned an error")
	}

	// use a devnull connector for example purposes
	conn := &devnull.Connector{}

	// create the client using the registry and connector
	client, err := dosa.NewClient(reg, conn)
	if err != nil {
		errors.Wrap(err, "dosa.NewClient returned an error")
	}

	err = client.Initialize(context.Background())
	if err != nil {
		errors.Wrap(err, "client.Initialize returned an error")
	}
}

func TestNewClient(t *testing.T) {
	// initialize registrar
	reg, err := dosa.NewRegistrar("test", "myteam.myservice", cte1)
	assert.NoError(t, err)
	assert.NotNil(t, reg)

	// initialize a pseudo-connected client
	client, err := dosa.NewClient(reg, nullConnector)
	assert.NoError(t, err)
	err = client.Initialize(ctx)
	assert.NoError(t, err)
}

func TestClient_Initialize(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	emptyReg, _ := dosa.NewRegistrar("test", "team.service")
	reg, _ := dosa.NewRegistrar("test", "team.service", cte1)

	// find error
	c1, _ := dosa.NewClient(emptyReg, nullConnector)
	assert.Error(t, c1.Initialize(ctx))

	// CheckSchema error
	errConn := mocks.NewMockConnector(ctrl)
	errConn.EXPECT().CheckSchema(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("CheckSchema error")).AnyTimes()
	c2, _ := dosa.NewClient(reg, errConn)
	assert.Error(t, c2.Initialize(ctx))

	// happy path
	c3, _ := dosa.NewClient(reg, nullConnector)
	assert.NoError(t, c3.Initialize(ctx))

	// already initialized
	assert.NoError(t, c3.Initialize(ctx))
}

func TestClient_Read(t *testing.T) {
	reg1, _ := dosa.NewRegistrar(scope, namePrefix, cte1)
	reg2, _ := dosa.NewRegistrar(scope, namePrefix, cte1, cte2)
	fieldsToRead := []string{"ID", "Email"}
	results := map[string]dosa.FieldValue{
		"id":    int64(2),
		"name":  "bar",
		"email": "bar@email.com",
	}

	// uninitialized
	c1, _ := dosa.NewClient(reg1, nullConnector)
	assert.Error(t, c1.Read(ctx, fieldsToRead, cte1))

	// unregistered object
	c1.Initialize(ctx)
	err := c1.Read(ctx, dosa.All(), cte2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ClientTestEntity2")

	// happy path, mock connector
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockConn := mocks.NewMockConnector(ctrl)
	mockConn.EXPECT().CheckSchema(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]int32{1}, nil).AnyTimes()
	mockConn.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *dosa.EntityInfo, columnValues map[string]dosa.FieldValue, columnsToRead []string) {
			assert.Equal(t, columnValues["id"], cte1.ID)
			assert.Equal(t, columnsToRead, []string{"id", "email"})

		}).Return(results, nil).MinTimes(1)
	c3, _ := dosa.NewClient(reg2, mockConn)
	assert.NoError(t, c3.Initialize(ctx))
	assert.NoError(t, c3.Read(ctx, fieldsToRead, cte1))
	assert.Equal(t, cte1.ID, results["id"])
	assert.Equal(t, cte1.Name, results["name"])
	assert.Equal(t, cte1.Email, results["email"])
}

func TestClient_Read_Errors(t *testing.T) {
	reg1, _ := dosa.NewRegistrar(scope, namePrefix, cte1)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	readError := errors.New("oops")
	mockConn := mocks.NewMockConnector(ctrl)
	mockConn.EXPECT().CheckSchema(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]int32{1}, nil).AnyTimes()
	mockConn.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any(), dosa.All()).
		Do(func(_ context.Context, _ *dosa.EntityInfo, columnValues map[string]dosa.FieldValue, columnsToRead []string) {
			assert.Equal(t, columnValues["id"], cte1.ID)
		}).Return(nil, readError)

	c1, _ := dosa.NewClient(reg1, mockConn)
	assert.NoError(t, c1.Initialize(ctx))
	err := c1.Read(ctx, dosa.All(), cte1)
	assert.Error(t, err)
	assert.Equal(t, err, readError)
	err = c1.Read(ctx, []string{"badcol"}, cte1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "badcol")
}

func TestClient_Upsert(t *testing.T) {
	reg1, _ := dosa.NewRegistrar("test", "team.service", cte1)
	reg2, _ := dosa.NewRegistrar("test", "team.service", cte1, cte2)
	fieldsToUpdate := []string{"Email"}
	updatedEmail := "bar@email.com"

	// uninitialized
	c1, _ := dosa.NewClient(reg1, nullConnector)
	assert.Error(t, c1.Upsert(ctx, fieldsToUpdate, cte1))

	// unregistered object error
	c2, _ := dosa.NewClient(reg1, nullConnector)
	c2.Initialize(ctx)
	assert.Error(t, c2.Upsert(ctx, fieldsToUpdate, cte2))

	// happy path, mock connector
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockConn := mocks.NewMockConnector(ctrl)
	mockConn.EXPECT().CheckSchema(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]int32{1}, nil).AnyTimes()
	mockConn.EXPECT().Upsert(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *dosa.EntityInfo, columnValues map[string]dosa.FieldValue) {
			assert.Equal(t, columnValues["id"], cte1.ID)
			assert.Equal(t, columnValues["email"], cte1.Email)
			cte1.Email = updatedEmail
		}).
		Return(nil).MinTimes(1)
	c3, _ := dosa.NewClient(reg2, mockConn)
	assert.NoError(t, c3.Initialize(ctx))
	assert.NoError(t, c3.Upsert(ctx, fieldsToUpdate, cte1))
	assert.Equal(t, cte1.Email, updatedEmail)
}

func TestClient_Upsert_Errors(t *testing.T) {
	reg1, _ := dosa.NewRegistrar(scope, namePrefix, cte1)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	readError := errors.New("oops")
	mockConn := mocks.NewMockConnector(ctrl)
	mockConn.EXPECT().CheckSchema(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]int32{1}, nil).AnyTimes()
	mockConn.EXPECT().Upsert(gomock.Any(), gomock.Any(), gomock.Any()).Return(readError)

	c1, _ := dosa.NewClient(reg1, mockConn)
	assert.NoError(t, c1.Initialize(ctx))
	// TODO: This is a bug, fails with Cannot provide empty list to OnlyFieldValues
	// err := c1.Upsert(ctx, dosa.All(), cte1)
	err := c1.Upsert(ctx, []string{"ID"}, cte1)
	assert.Error(t, err)
	assert.Equal(t, err, readError)
	err = c1.Upsert(ctx, []string{"badcol"}, cte1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "badcol")
}

func TestClient_Range(t *testing.T) {
	reg1, _ := dosa.NewRegistrar(scope, namePrefix, cte1)
	fieldsToRead := []string{"ID", "Email"}
	resultRow := map[string]dosa.FieldValue{
		"id":    int64(2),
		"name":  "bar",
		"email": "bar@email.com",
	}

	// uninitialized
	c1, _ := dosa.NewClient(reg1, nullConnector)
	rop := dosa.NewRangeOp(cte1).Fields(fieldsToRead).Eq("ID", "123").Offset("tokeytoketoke")
	_, _, err := c1.Range(ctx, rop)
	assert.Equal(t, dosa.ErrNotInitialized, err)

	c1.Initialize(ctx)

	// bad entity
	rop = dosa.NewRangeOp(cte2)
	_, _, err = c1.Range(ctx, rop)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ClientTestEntity2")

	// bad column in range
	// we don't test other failed RangeOpConditions since those are unit tested elsewhere
	rop = dosa.NewRangeOp(cte1).Eq("borkborkbork", int64(1))
	_, _, err = c1.Range(ctx, rop)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ClientTestEntity1")
	assert.Contains(t, err.Error(), "borkborkbork")

	// bad projected column
	rop = dosa.NewRangeOp(cte1).Fields([]string{"borkborkbork"})
	_, _, err = c1.Range(ctx, rop)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ClientTestEntity1")
	assert.Contains(t, err.Error(), "borkborkbork")

	// success case
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockConn := mocks.NewMockConnector(ctrl)
	mockConn.EXPECT().CheckSchema(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]int32{1}, nil).AnyTimes()
	mockConn.EXPECT().Range(ctx, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]map[string]dosa.FieldValue{resultRow}, "continuation-token", nil)
	c2, _ := dosa.NewClient(reg1, mockConn)
	c2.Initialize(ctx)
	rop = dosa.NewRangeOp(cte1)
	rows, token, err := c2.Range(ctx, rop)
	assert.NoError(t, err)
	assert.NotNil(t, rows)
	assert.Equal(t, 1, len(rows))
	for _, obj := range rows {
		assert.Equal(t, resultRow["id"], obj.(*ClientTestEntity1).ID)
		assert.Equal(t, resultRow["name"], obj.(*ClientTestEntity1).Name)
		assert.Equal(t, resultRow["email"], obj.(*ClientTestEntity1).Email)
	}
	assert.Equal(t, "continuation-token", token)

	// no resulting rows, just use the devnull connector
	rop = dosa.NewRangeOp(cte1)
	_, _, err = c1.Range(ctx, rop)
	assert.Equal(t, dosa.ErrNotFound, err)
}

func TestClient_ScanEverything(t *testing.T) {
	reg1, _ := dosa.NewRegistrar(scope, namePrefix, cte1)
	fieldsToRead := []string{"ID", "Email"}
	resultRow := map[string]dosa.FieldValue{
		"id":          int64(2),
		"name":        "bar",
		"email":       "bar@email.com",
		"straycolumn": "this_should_be_discarded",
	}

	// uninitialized
	c1, _ := dosa.NewClient(reg1, nullConnector)
	sop := dosa.NewScanOp(cte1).Fields(fieldsToRead).Offset("tokeytoketoke")
	_, _, err := c1.ScanEverything(ctx, sop)
	assert.Equal(t, dosa.ErrNotInitialized, err)

	c1.Initialize(ctx)

	// bad entity
	sop = dosa.NewScanOp(cte2)
	_, _, err = c1.ScanEverything(ctx, sop)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ClientTestEntity2")

	// bad projected column
	sop = dosa.NewScanOp(cte1).Fields([]string{"borkborkbork"})
	_, _, err = c1.ScanEverything(ctx, sop)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ClientTestEntity1")
	assert.Contains(t, err.Error(), "borkborkbork")

	// success case
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockConn := mocks.NewMockConnector(ctrl)
	mockConn.EXPECT().CheckSchema(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]int32{1}, nil).AnyTimes()
	mockConn.EXPECT().Scan(ctx, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]map[string]dosa.FieldValue{resultRow}, "continuation-token", nil)
	c2, _ := dosa.NewClient(reg1, mockConn)
	c2.Initialize(ctx)
	sop = dosa.NewScanOp(cte1)
	rows, token, err := c2.ScanEverything(ctx, sop)
	assert.NoError(t, err)
	assert.NotNil(t, rows)
	assert.Equal(t, 1, len(rows))
	for _, obj := range rows {
		assert.Equal(t, resultRow["id"], obj.(*ClientTestEntity1).ID)
		assert.Equal(t, resultRow["name"], obj.(*ClientTestEntity1).Name)
		assert.Equal(t, resultRow["email"], obj.(*ClientTestEntity1).Email)
	}
	assert.Equal(t, "continuation-token", token)

	// no resulting rows, just use the devnull connector
	sop = dosa.NewScanOp(cte1)
	_, _, err = c1.ScanEverything(ctx, sop)
	assert.Equal(t, dosa.ErrNotFound, err)
}

func TestClient_Remove(t *testing.T) {
	reg1, _ := dosa.NewRegistrar(scope, namePrefix, cte1)

	// uninitialized
	c1, _ := dosa.NewClient(reg1, nullConnector)
	err := c1.Remove(ctx, cte1)
	assert.Equal(t, dosa.ErrNotInitialized, err)

	c1.Initialize(ctx)

	// bad entity
	err = c1.Remove(ctx, cte2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ClientTestEntity2")
	// success case
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockConn := mocks.NewMockConnector(ctrl)
	mockConn.EXPECT().CheckSchema(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]int32{1}, nil).AnyTimes()
	mockConn.EXPECT().Remove(ctx, gomock.Any(), map[string]dosa.FieldValue{"id": dosa.FieldValue(int64(123))}).Return(nil)
	c2, _ := dosa.NewClient(reg1, mockConn)
	c2.Initialize(ctx)
	err = c2.Remove(ctx, &ClientTestEntity1{ID: int64(123)})
	assert.NoError(t, err)

}

func TestClient_Unimplemented(t *testing.T) {
	reg1, _ := dosa.NewRegistrar(scope, namePrefix, cte1)

	c, _ := dosa.NewClient(reg1, nullConnector)
	assert.Panics(t, func() {
		c.CreateIfNotExists(ctx, &ClientTestEntity1{})
	})
	assert.Panics(t, func() {
		c.MultiRead(ctx, dosa.All(), &ClientTestEntity1{})
	})
	assert.Panics(t, func() {
		c.MultiUpsert(ctx, dosa.All(), &ClientTestEntity1{})
	})
	assert.Panics(t, func() {
		c.MultiRemove(ctx, &ClientTestEntity1{})
	})
	assert.Panics(t, func() {
		c.Search(ctx, &dosa.SearchOp{})
	})
}

func TestAdminClient_CreateScope(t *testing.T) {
	c, err := dosa.NewAdminClient(nullConnector)
	assert.NoError(t, err)
	assert.NotNil(t, c)

	err = c.CreateScope(scope)
	assert.NoError(t, err)
}

func TestAdminClient_TruncateScope(t *testing.T) {
	c, err := dosa.NewAdminClient(nullConnector)
	assert.NoError(t, err)
	assert.NotNil(t, c)

	err = c.TruncateScope(scope)
	assert.NoError(t, err)
}

func TestAdminClient_Unimplemented(t *testing.T) {
	c, _ := dosa.NewAdminClient(nullConnector)
	assert.Panics(t, func() {
		c.CheckSchema(ctx, cte1FQN, cte2FQN)
	})
	assert.Panics(t, func() {
		c.UpsertSchema(ctx, cte1FQN, cte2FQN)
	})
	assert.Panics(t, func() {
		c.DropScope(scope)
	})
}
