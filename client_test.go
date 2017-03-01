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
	"fmt"
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
	cte1 = &ClientTestEntity1{ID: int64(1), Name: "foo", Email: "foo@uber.com"}
	cte2 = &ClientTestEntity2{UUID: "b1f23fa3-f453-45b4-a5d5-6d73078ac3bd", Color: "blue", IsActive: true}
)

func ExampleNewClient() {
	// initialize registrar
	entities := []dosa.DomainObject{cte1}
	reg, err := dosa.NewRegistrar("test", "myteam.myservice", entities...)
	if err != nil {
		// registration will most likely fail as a result of programmer error
		panic("dosa.NewRegister returned an error")
	}

	// use a devnull connector for example purposes
	conn := &devnull.Connector{}

	// initialize a pseudo-connected client
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
	entities := []dosa.DomainObject{cte1}
	reg, err := dosa.NewRegistrar("test", "myteam.myservice", entities...)
	assert.NoError(t, err)
	assert.NotNil(t, reg)

	// use a devnull connector for test test test purposes
	conn := &devnull.Connector{}

	// initialize a pseudo-connected client
	client, err := dosa.NewClient(reg, conn)
	assert.NoError(t, err)
	err = client.Initialize(context.TODO())
	assert.NoError(t, err)
}

func TestClient_Initialize(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.TODO()
	emptyReg, _ := dosa.NewRegistrar("test", "team.service", []dosa.DomainObject{}...)
	reg, _ := dosa.NewRegistrar("test", "team.service", []dosa.DomainObject{cte1}...)
	conn := &devnull.Connector{}

	// find error
	c1, _ := dosa.NewClient(emptyReg, conn)
	assert.Error(t, c1.Initialize(ctx))

	// CheckSchema error
	errConn := mocks.NewMockConnector(ctrl)
	errConn.EXPECT().CheckSchema(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("CheckSchema error")).AnyTimes()
	c2, _ := dosa.NewClient(reg, errConn)
	assert.Error(t, c2.Initialize(ctx))

	// happy path
	c3, _ := dosa.NewClient(reg, conn)
	assert.NoError(t, c3.Initialize(ctx))

	// already initialized
	assert.NoError(t, c3.Initialize(ctx))
}

func TestClient_Read(t *testing.T) {
	ctx := context.TODO()
	scope := "test"
	namePrefix := "team.service"
	ctes1 := []dosa.DomainObject{cte1}
	ctes2 := []dosa.DomainObject{cte1, cte2}
	reg1, _ := dosa.NewRegistrar(scope, namePrefix, ctes1...)
	reg2, _ := dosa.NewRegistrar(scope, namePrefix, ctes2...)
	conn := &devnull.Connector{}
	fieldsToRead := []string{"ID", "Email"}
	results := map[string]dosa.FieldValue{
		"id":    int64(2),
		"name":  "bar",
		"email": "bar@email.com",
	}

	// uninitialized
	c1, _ := dosa.NewClient(reg1, conn)
	assert.Error(t, c1.Read(ctx, fieldsToRead, cte1))

	// find error
	c2, _ := dosa.NewClient(reg1, conn)
	c2.Initialize(ctx)
	assert.Error(t, c2.Read(ctx, fieldsToRead, cte2))

	// happy path, mock connector
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockConn := mocks.NewMockConnector(ctrl)
	mockConn.EXPECT().CheckSchema(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]int32{1}, nil).AnyTimes()
	mockConn.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *dosa.EntityInfo, columnValues map[string]dosa.FieldValue, columnsToRead []string) {
			fmt.Println("columnValues")
			fmt.Println(columnValues)
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

func TestClient_Upsert(t *testing.T) {
	ctx := context.TODO()
	ctes1 := []dosa.DomainObject{cte1}
	ctes2 := []dosa.DomainObject{cte1, cte2}
	reg1, _ := dosa.NewRegistrar("test", "team.service", ctes1...)
	reg2, _ := dosa.NewRegistrar("test", "team.service", ctes2...)
	conn := &devnull.Connector{}
	fieldsToUpdate := []string{"Email"}
	updatedEmail := "bar@email.com"

	// uninitialized
	c1, _ := dosa.NewClient(reg1, conn)
	assert.Error(t, c1.Upsert(ctx, fieldsToUpdate, cte1))

	// find error
	c2, _ := dosa.NewClient(reg1, conn)
	c2.Initialize(ctx)
	assert.Error(t, c2.Read(ctx, fieldsToUpdate, cte2))

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
