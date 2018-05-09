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

// Package testingexamples is the Examples: Testing.
//
// The example here demonstrates how to write tests for code that uses the DOSA
// client. For now, we are suggesting the use of a mock client that we have
// generated using
// MockGen (https://github.com/golang/mock). It's available
// as
// (github.com/uber-go/dosa/mocks).MockClient.
//
// MockClient Usage
//
// Given an entity defined as:
//
//   type User struct {
//       dosa.Entity `dosa:"primaryKey=UUID"`
//       UUID        dosa.UUID
//       Name        string
//       Email       string
//       CreatedOn   time.Time
//   }
//
// And a method that operates on that entity called GetUser:
//
//   type Datastore struct {
//       client dosa.Client
//   }
//
//   func (d *Datastore) GetUser(ctx context.Context, uuid dosa.UUID) (*User, error) {
//       user := &User{uuid: uuid}
//       readCtx, readCancel := context.WithTimeout(ctx, 1 * time.Second)
//       if err := d.client.Read(readCts
//   }
//
// The client behavior can then be mocked using MockClient:
//
//   package datastore_test
//
//   import (
//       "github.com/golang/mock/gomock"
//       "github.com/stretchr/testify/assert"
//
//       "github.com/uber-go/dosa"
//       examples "github.com/uber-go/dosa/examples/testing"
//       "github.com/uber-go/dosa/mocks"
//   )
//
//   func TestGetUser(t *testing.T) {
//       ctrl := gomock.NewController(t)
//       defer ctrl.Finish()
//
//       // mock error from `Read` call
//       c1 := mocks.NewMockClient(ctrl)
//       c1.EXPECT().Initialize(gomock.Any()).Return(nil).Times(1)
//       c1.EXPECT().Read(gomock.Any(), nil, user).Return(errors.New("Read Error")).Times(1)
//       ds1, _ := examples.NewDatastore(c1)
//
//       u1, err1 := ds1.GetUser(ctx, uuid)
//       assert.Error(t, err1)
//       assert.Nil(t, u1)
//
//       // happy path
//       c2 := mocks.NewMockClient(ctrl)
//       c2.EXPECT().Initialize(gomock.Any()).Return(nil).Times(1)
//       c2.EXPECT().Read(gomock.Any(), nil, user).Return(nil).Times(1)
//       ds2, _ := examples.NewDatastore(c2)
//
//       u2, err2 := ds2.GetUser(ctx, uuid)
//       assert.NoError(t, err2)
//       assert.Equal(t, u2, user)
//   }
//
// A complete, runnable example of this can be found in our testing examples package (https://github.com/uber-go/dosa/tree/master/examples/testing).
//
//
package testingexamples
