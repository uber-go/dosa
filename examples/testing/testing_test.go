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

package testingexamples_test

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/uber-go/dosa"
	examples "github.com/uber-go/dosa/examples/testing"
	"github.com/uber-go/dosa/mocks"
)

var (
	ctx  = context.TODO()
	uuid = dosa.NewUUID()
	user = &examples.User{UUID: uuid}
)

func TestNewDatastore(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// mock error from Initialize call
	c1 := mocks.NewMockClient(ctrl)
	c1.EXPECT().Initialize(gomock.Any()).Return(errors.New("Initialize Error")).Times(1)
	ds1, err1 := examples.NewDatastore(c1)
	assert.Error(t, err1)
	assert.Nil(t, ds1)

	// happy path
	c2 := mocks.NewMockClient(ctrl)
	c2.EXPECT().Initialize(gomock.Any()).Return(nil).Times(1)
	ds2, err2 := examples.NewDatastore(c2)
	assert.NoError(t, err2)
	assert.NotNil(t, ds2)
}

func TestGetUser(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// mock error from Read call
	c1 := mocks.NewMockClient(ctrl)
	c1.EXPECT().Initialize(gomock.Any()).Return(nil).Times(1)
	c1.EXPECT().Read(gomock.Any(), nil, user).Return(errors.New("Read Error")).Times(1)
	ds1, _ := examples.NewDatastore(c1)

	u1, err1 := ds1.GetUser(ctx, uuid)
	assert.Error(t, err1)
	assert.Nil(t, u1)

	// happy path
	c2 := mocks.NewMockClient(ctrl)
	c2.EXPECT().Initialize(gomock.Any()).Return(nil).Times(1)
	c2.EXPECT().Read(gomock.Any(), nil, user).Return(nil).Times(1)
	ds2, _ := examples.NewDatastore(c2)

	u2, err2 := ds2.GetUser(ctx, uuid)
	assert.NoError(t, err2)
	assert.Equal(t, u2, user)
}
