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
	"io/ioutil"
	"os"
	"path/filepath"
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
	cte1name      = "clienttestentity1"
	cte2name      = "clienttestentity2"
	nullConnector = &devnull.Connector{}
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
	client := dosa.NewClient(reg, conn)

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
	client := dosa.NewClient(reg, nullConnector)
	err = client.Initialize(ctx)
	assert.NoError(t, err)
}

func TestClient_Initialize(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	emptyReg, _ := dosa.NewRegistrar("test", "team.service")
	reg, _ := dosa.NewRegistrar("test", "team.service", cte1)

	// find error
	c1 := dosa.NewClient(emptyReg, nullConnector)
	assert.Error(t, c1.Initialize(ctx))

	// CheckSchema error
	errConn := mocks.NewMockConnector(ctrl)
	errConn.EXPECT().CheckSchema(ctx, gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("CheckSchema error")).AnyTimes()
	c2 := dosa.NewClient(reg, errConn)
	assert.Error(t, c2.Initialize(ctx))

	// happy path
	c3 := dosa.NewClient(reg, nullConnector)
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
	c1 := dosa.NewClient(reg1, nullConnector)
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
	mockConn.EXPECT().CheckSchema(ctx, gomock.Any(), gomock.Any(), gomock.Any()).Return([]int32{1}, nil).AnyTimes()
	mockConn.EXPECT().Read(ctx, gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *dosa.EntityInfo, columnValues map[string]dosa.FieldValue, columnsToRead []string) {
			assert.Equal(t, columnValues["id"], cte1.ID)
			assert.Equal(t, columnsToRead, []string{"id", "email"})

		}).Return(results, nil).MinTimes(1)
	c3 := dosa.NewClient(reg2, mockConn)
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
	mockConn.EXPECT().CheckSchema(ctx, gomock.Any(), gomock.Any(), gomock.Any()).Return([]int32{1}, nil).AnyTimes()
	mockConn.EXPECT().Read(ctx, gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *dosa.EntityInfo, columnValues map[string]dosa.FieldValue, columnsToRead []string) {
			assert.Equal(t, columnValues["id"], cte1.ID)
			assert.NotEmpty(t, columnsToRead)
		}).Return(nil, readError)

	c1 := dosa.NewClient(reg1, mockConn)
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
	c1 := dosa.NewClient(reg1, nullConnector)
	assert.Error(t, c1.Upsert(ctx, fieldsToUpdate, cte1))

	// unregistered object error
	c2 := dosa.NewClient(reg1, nullConnector)
	c2.Initialize(ctx)
	assert.Error(t, c2.Upsert(ctx, fieldsToUpdate, cte2))

	// happy path, mock connector
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockConn := mocks.NewMockConnector(ctrl)
	mockConn.EXPECT().CheckSchema(ctx, gomock.Any(), gomock.Any(), gomock.Any()).Return([]int32{1}, nil).AnyTimes()
	mockConn.EXPECT().Upsert(ctx, gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *dosa.EntityInfo, columnValues map[string]dosa.FieldValue) {
			assert.Equal(t, columnValues["id"], cte1.ID)
			assert.Equal(t, columnValues["email"], cte1.Email)
			cte1.Email = updatedEmail
		}).
		Return(nil).MinTimes(1)
	c3 := dosa.NewClient(reg2, mockConn)
	assert.NoError(t, c3.Initialize(ctx))
	assert.NoError(t, c3.Upsert(ctx, fieldsToUpdate, cte1))
	assert.Equal(t, cte1.Email, updatedEmail)
}
func TestClient_CreateIfNotExists(t *testing.T) {
	reg1, _ := dosa.NewRegistrar("test", "team.service", cte1)
	reg2, _ := dosa.NewRegistrar("test", "team.service", cte1, cte2)
	updatedEmail := "bar@email.com"

	// uninitialized
	c1 := dosa.NewClient(reg1, nullConnector)
	assert.Error(t, c1.CreateIfNotExists(ctx, cte1))

	// unregistered object error
	c2 := dosa.NewClient(reg1, nullConnector)
	c2.Initialize(ctx)
	assert.Error(t, c2.CreateIfNotExists(ctx, cte2))

	// happy path, mock connector
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockConn := mocks.NewMockConnector(ctrl)
	mockConn.EXPECT().CheckSchema(ctx, gomock.Any(), gomock.Any(), gomock.Any()).Return([]int32{1}, nil).AnyTimes()
	mockConn.EXPECT().CreateIfNotExists(ctx, gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *dosa.EntityInfo, columnValues map[string]dosa.FieldValue) {
			assert.Equal(t, columnValues["id"], cte1.ID)
			assert.Equal(t, columnValues["email"], cte1.Email)
			cte1.Email = updatedEmail
		}).
		Return(nil).MinTimes(1)
	c3 := dosa.NewClient(reg2, mockConn)
	assert.NoError(t, c3.Initialize(ctx))
	assert.NoError(t, c3.CreateIfNotExists(ctx, cte1))
	assert.Equal(t, cte1.Email, updatedEmail)
}

func TestClient_Upsert_Errors(t *testing.T) {
	reg1, _ := dosa.NewRegistrar(scope, namePrefix, cte1)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	readError := errors.New("oops")
	mockConn := mocks.NewMockConnector(ctrl)
	mockConn.EXPECT().CheckSchema(ctx, gomock.Any(), gomock.Any(), gomock.Any()).Return([]int32{1}, nil).AnyTimes()

	c1 := dosa.NewClient(reg1, mockConn)
	assert.NoError(t, c1.Initialize(ctx))
	mockConn.EXPECT().Upsert(ctx, gomock.Any(), gomock.Not(dosa.All())).Return(nil)
	err := c1.Upsert(ctx, dosa.All(), cte1)
	assert.NoError(t, err)

	mockConn.EXPECT().Upsert(ctx, gomock.Any(), map[string]dosa.FieldValue{"id": dosa.FieldValue(int64(2))}).Return(readError)
	err = c1.Upsert(ctx, []string{"ID"}, cte1)
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
	c1 := dosa.NewClient(reg1, nullConnector)
	rop := dosa.NewRangeOp(cte1).Fields(fieldsToRead).Eq("ID", "123").Offset("tokeytoketoke")
	_, _, err := c1.Range(ctx, rop)
	assert.True(t, dosa.ErrorIsNotInitialized(err))

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
	mockConn.EXPECT().CheckSchema(ctx, gomock.Any(), gomock.Any(), gomock.Any()).Return([]int32{1}, nil).AnyTimes()
	mockConn.EXPECT().Range(ctx, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]map[string]dosa.FieldValue{resultRow}, "continuation-token", nil)
	c2 := dosa.NewClient(reg1, mockConn)
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
	assert.True(t, dosa.ErrorIsNotFound(err))
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
	c1 := dosa.NewClient(reg1, nullConnector)
	sop := dosa.NewScanOp(cte1).Fields(fieldsToRead).Offset("tokeytoketoke")
	_, _, err := c1.ScanEverything(ctx, sop)
	assert.True(t, dosa.ErrorIsNotInitialized(err))

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
	mockConn.EXPECT().CheckSchema(ctx, gomock.Any(), gomock.Any(), gomock.Any()).Return([]int32{1}, nil).AnyTimes()
	mockConn.EXPECT().Scan(ctx, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]map[string]dosa.FieldValue{resultRow}, "continuation-token", nil)
	c2 := dosa.NewClient(reg1, mockConn)
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
	assert.True(t, dosa.ErrorIsNotFound(err))
}

func TestClient_Remove(t *testing.T) {
	reg1, _ := dosa.NewRegistrar(scope, namePrefix, cte1)

	// uninitialized
	c1 := dosa.NewClient(reg1, nullConnector)
	err := c1.Remove(ctx, cte1)
	assert.True(t, dosa.ErrorIsNotInitialized(err))

	c1.Initialize(ctx)

	// bad entity
	err = c1.Remove(ctx, cte2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ClientTestEntity2")
	// success case
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockConn := mocks.NewMockConnector(ctrl)
	mockConn.EXPECT().CheckSchema(ctx, gomock.Any(), gomock.Any(), gomock.Any()).Return([]int32{1}, nil).AnyTimes()
	mockConn.EXPECT().Remove(ctx, gomock.Any(), map[string]dosa.FieldValue{"id": dosa.FieldValue(int64(123))}).Return(nil)
	c2 := dosa.NewClient(reg1, mockConn)
	c2.Initialize(ctx)
	err = c2.Remove(ctx, &ClientTestEntity1{ID: int64(123)})
	assert.NoError(t, err)

}

func TestClient_Unimplemented(t *testing.T) {
	reg1, _ := dosa.NewRegistrar(scope, namePrefix, cte1)

	c := dosa.NewClient(reg1, nullConnector)
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
	c := dosa.NewAdminClient(nullConnector)
	assert.NotNil(t, c)

	err := c.CreateScope(context.TODO(), scope)
	assert.NoError(t, err)
}

func TestAdminClient_TruncateScope(t *testing.T) {
	c := dosa.NewAdminClient(nullConnector)
	assert.NotNil(t, c)

	err := c.TruncateScope(context.TODO(), scope)
	assert.NoError(t, err)
}

func TestAdminClient_DropScope(t *testing.T) {
	c := dosa.NewAdminClient(nullConnector)
	assert.NotNil(t, c)

	err := c.DropScope(context.TODO(), scope)
	assert.NoError(t, err)
}

func TestAdminClient_CheckSchema(t *testing.T) {
	// write some entities to disk
	tmpdir := ".testcheckschema"
	os.RemoveAll(tmpdir)
	defer os.RemoveAll(tmpdir)
	content := `
package main

import "github.com/uber-go/dosa"

type TestEntityA struct {
	dosa.Entity ` + "`dosa:\"primaryKey=(ID)\"`" + `
	ID int32
}
type TestEntityB struct {
	dosa.Entity ` + "`dosa:\"primaryKey=(ID)\"`" + `
	ID int32
}
`
	assert.NoError(t, os.MkdirAll(tmpdir, 0770))
	assert.NoError(t, ioutil.WriteFile(filepath.Join(tmpdir, "f1.go"), []byte(content), 0700))

	data := []struct {
		dirs        []string
		excludes    []string
		scope       string
		namePrefix  string
		errContains string
	}{
		// cannot get schema
		{
			dirs:        []string{"/foo/bar/baz"},
			scope:       scope,
			errContains: "/foo/bar/baz",
		},
		// connector error
		{
			dirs:        []string{tmpdir},
			scope:       scope,
			namePrefix:  "error",
			errContains: "connector error",
		},
		// happy path
		{
			dirs:       []string{tmpdir},
			scope:      scope,
			namePrefix: namePrefix,
		},
	}

	// calls with "error" prefix will fail, rest succeed
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockConn := mocks.NewMockConnector(ctrl)
	mockConn.EXPECT().CheckSchema(ctx, scope, "error", gomock.Any()).Return(nil, errors.New("connector error")).Times(1)
	mockConn.EXPECT().CheckSchema(ctx, scope, namePrefix, gomock.Any()).Return([]int32{1, 1}, nil).Times(1)

	for _, d := range data {
		_, err := dosa.NewAdminClient(mockConn).
			Directories(d.dirs).
			Scope(d.scope).
			CheckSchema(ctx, d.namePrefix)
		if d.errContains != "" {
			assert.Contains(t, err.Error(), d.errContains)
			continue
		}
		assert.NoError(t, err)
	}
}

func TestAdminClient_UpsertSchema(t *testing.T) {
	// write some entities to disk
	tmpdir := ".testupsertschema"
	os.RemoveAll(tmpdir)
	defer os.RemoveAll(tmpdir)
	content := `
package main

import "github.com/uber-go/dosa"

type TestEntityA struct {
	dosa.Entity ` + "`dosa:\"primaryKey=(ID)\"`" + `
	ID int32
}
type TestEntityB struct {
	dosa.Entity ` + "`dosa:\"primaryKey=(ID)\"`" + `
	ID int32
}
`
	assert.NoError(t, os.MkdirAll(tmpdir, 0770))
	assert.NoError(t, ioutil.WriteFile(filepath.Join(tmpdir, "f1.go"), []byte(content), 0700))

	data := []struct {
		dirs        []string
		scope       string
		namePrefix  string
		errContains string
	}{
		// cannot get schema
		{
			dirs:        []string{"/foo/bar/baz"},
			scope:       scope,
			errContains: "/foo/bar/baz",
		},
		// connector error
		{
			dirs:        []string{tmpdir},
			scope:       scope,
			namePrefix:  "error",
			errContains: "connector error",
		},
		// happy path
		{
			dirs:       []string{tmpdir},
			scope:      scope,
			namePrefix: namePrefix,
		},
	}

	// calls with "error" prefix will fail, rest succeed
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockConn := mocks.NewMockConnector(ctrl)
	mockConn.EXPECT().UpsertSchema(ctx, scope, "error", gomock.Any()).Return(nil, errors.New("connector error")).Times(1)
	mockConn.EXPECT().UpsertSchema(ctx, scope, namePrefix, gomock.Any()).Return([]int32{1, 1}, nil).Times(1)

	for _, d := range data {
		_, err := dosa.NewAdminClient(mockConn).
			Directories(d.dirs).
			Scope(d.scope).
			UpsertSchema(ctx, d.namePrefix)
		if d.errContains != "" {
			assert.Contains(t, err.Error(), d.errContains)
			continue
		}
		assert.NoError(t, err)
	}
}

func TestAdminClient_GetSchema(t *testing.T) {
	// write some entities to disk
	tmpdir := ".testgetschema"
	os.RemoveAll(tmpdir)
	defer os.RemoveAll(tmpdir)
	path1 := filepath.Join(tmpdir, "f1.go")
	path2 := filepath.Join(tmpdir, "f2.go")
	content := `
package main

import "github.com/uber-go/dosa"

type TestEntityA struct {
	dosa.Entity ` + "`dosa:\"primaryKey=(ID)\"`" + `
	ID int32
}
type TestEntityB struct {
	dosa.Entity ` + "`dosa:\"primaryKey=(ID)\"`" + `
	ID int32
}
`
	invalid := `
package main

import "github.com/uber-go/dosa"

type TestEntityC struct {
	dosa.Entity ` + "`dosa:\"invalidtag\"`" + `
	ID int32
}
`
	assert.NoError(t, os.MkdirAll(tmpdir, 0770))
	assert.NoError(t, ioutil.WriteFile(path1, []byte(content), 0700))
	assert.NoError(t, ioutil.WriteFile(path2, []byte(invalid), 0700))

	data := []struct {
		dirs        []string
		excludes    []string
		scope       string
		namePrefix  string
		errContains string
	}{
		// invalid scope
		{
			scope:       "***",
			errContains: "invalid scope name",
		},
		// invalid directory
		{
			dirs:        []string{"/foo/bar/baz"},
			scope:       scope,
			errContains: "/foo/bar/baz",
		},
		// no entities found
		{
			dirs:        []string{tmpdir},
			excludes:    []string{"f1.go", "f2.go", "f3.go"},
			scope:       scope,
			errContains: "no entities found",
		},
		// invalid struct tag
		{
			dirs:        []string{tmpdir},
			scope:       scope,
			errContains: "invalid dosa struct tag",
		},
	}

	for _, d := range data {
		_, err := dosa.NewAdminClient(nullConnector).
			Directories(d.dirs).
			Excludes(d.excludes).
			Scope(d.scope).
			UpsertSchema(ctx, d.namePrefix)
		if d.errContains != "" {
			assert.Contains(t, err.Error(), d.errContains)
			continue
		}
		assert.NoError(t, err)
	}
}

func TestErrorIsNotFound(t *testing.T) {
	assert.False(t, dosa.ErrorIsNotFound(errors.New("not a IsNotFound error")))
	assert.False(t, dosa.ErrorIsNotFound(&dosa.ErrNotInitialized{}))
	assert.True(t, dosa.ErrorIsNotFound(&dosa.ErrNotFound{}))
	assert.True(t, dosa.ErrorIsNotFound(errors.Wrap(&dosa.ErrNotFound{}, "wrapped")))
	assert.Equal(t, (&dosa.ErrNotFound{}).Error(), "not found")
}

func TestErrNotInitialized_Error(t *testing.T) {
	assert.False(t, dosa.ErrorIsNotInitialized(errors.New("not a IsNotInitializedError")))
	assert.False(t, dosa.ErrorIsNotInitialized(&dosa.ErrNotFound{}))
	assert.True(t, dosa.ErrorIsNotInitialized(&dosa.ErrNotInitialized{}))
	assert.True(t, dosa.ErrorIsNotInitialized(errors.Wrap(&dosa.ErrNotInitialized{}, "wrapped")))
	assert.Equal(t, (&dosa.ErrNotInitialized{}).Error(), "client not initialized")

}

func TestErrorIsAlreadyExists(t *testing.T) {
	assert.False(t, dosa.ErrorIsAlreadyExists(errors.New("not an already exists error")))
	assert.False(t, dosa.ErrorIsAlreadyExists(&dosa.ErrNotInitialized{}))
	assert.False(t, dosa.ErrorIsAlreadyExists(&dosa.ErrNotFound{}))
	assert.True(t, dosa.ErrorIsAlreadyExists(errors.Wrap(&dosa.ErrAlreadyExists{}, "wrapped")))
	assert.Equal(t, "already exists", (&dosa.ErrAlreadyExists{}).Error())
}
