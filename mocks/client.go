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

// Automatically generated by MockGen. DO NOT EDIT!
// Source: github.com/uber-go/dosa (interfaces: Client,AdminClient)

package mocks

import (
	context "context"

	gomock "github.com/golang/mock/gomock"
	dosa "github.com/uber-go/dosa"
)

// MockClient is a mock of Client interface
type MockClient struct {
	ctrl     *gomock.Controller
	recorder *_MockClientRecorder
}

// Recorder for MockClient (not exported)
type _MockClientRecorder struct {
	mock *MockClient
}

// NewMockClient creates a new mock
func NewMockClient(ctrl *gomock.Controller) *MockClient {
	mock := &MockClient{ctrl: ctrl}
	mock.recorder = &_MockClientRecorder{mock}
	return mock
}

// EXPECT adds an expectation
func (_m *MockClient) EXPECT() *_MockClientRecorder {
	return _m.recorder
}

// CreateIfNotExists is a mock implementation of MockClient.CreateIfNotExists
func (_m *MockClient) CreateIfNotExists(_param0 context.Context, _param1 dosa.DomainObject) error {
	ret := _m.ctrl.Call(_m, "CreateIfNotExists", _param0, _param1)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockClientRecorder) CreateIfNotExists(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "CreateIfNotExists", arg0, arg1)
}

// Initialize is a mock implementation of MockClient.Initialize
func (_m *MockClient) Initialize(_param0 context.Context) error {
	ret := _m.ctrl.Call(_m, "Initialize", _param0)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockClientRecorder) Initialize(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Initialize", arg0)
}

// Range is a mock implementation of MockClient.Range
func (_m *MockClient) Range(_param0 context.Context, _param1 *dosa.RangeOp) ([]dosa.DomainObject, string, error) {
	ret := _m.ctrl.Call(_m, "Range", _param0, _param1)
	ret0, _ := ret[0].([]dosa.DomainObject)
	ret1, _ := ret[1].(string)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

func (_mr *_MockClientRecorder) Range(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Range", arg0, arg1)
}

// Read is a mock implementation of MockClient.Read
func (_m *MockClient) Read(_param0 context.Context, _param1 []string, _param2 dosa.DomainObject) error {
	ret := _m.ctrl.Call(_m, "Read", _param0, _param1, _param2)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockClientRecorder) Read(arg0, arg1, arg2 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Read", arg0, arg1, arg2)
}

// Remove is a mock implementation of MockClient.Remove
func (_m *MockClient) Remove(_param0 context.Context, _param1 dosa.DomainObject) error {
	ret := _m.ctrl.Call(_m, "Remove", _param0, _param1)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockClientRecorder) Remove(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Remove", arg0, arg1)
}

// ScanEverything is a mock implementation of MockClient.ScanEverything
func (_m *MockClient) ScanEverything(_param0 context.Context, _param1 *dosa.ScanOp) ([]dosa.DomainObject, string, error) {
	ret := _m.ctrl.Call(_m, "ScanEverything", _param0, _param1)
	ret0, _ := ret[0].([]dosa.DomainObject)
	ret1, _ := ret[1].(string)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

func (_mr *_MockClientRecorder) ScanEverything(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ScanEverything", arg0, arg1)
}

// Upsert is a mock implementation of MockClient.Upsert
func (_m *MockClient) Upsert(_param0 context.Context, _param1 []string, _param2 dosa.DomainObject) error {
	ret := _m.ctrl.Call(_m, "Upsert", _param0, _param1, _param2)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockClientRecorder) Upsert(arg0, arg1, arg2 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Upsert", arg0, arg1, arg2)
}

// MockAdminClient is a mock of AdminClient interface
type MockAdminClient struct {
	ctrl     *gomock.Controller
	recorder *_MockAdminClientRecorder
}

// Recorder for MockAdminClient (not exported)
type _MockAdminClientRecorder struct {
	mock *MockAdminClient
}

// NewMockAdminClient creates a new mock
func NewMockAdminClient(ctrl *gomock.Controller) *MockAdminClient {
	mock := &MockAdminClient{ctrl: ctrl}
	mock.recorder = &_MockAdminClientRecorder{mock}
	return mock
}

// EXPECT adds an expectation
func (_m *MockAdminClient) EXPECT() *_MockAdminClientRecorder {
	return _m.recorder
}

// CheckSchema is a mock implementation of MockAdminClient.CheckSchema
func (_m *MockAdminClient) CheckSchema(_param0 context.Context, _param1 string) (*dosa.SchemaStatus, error) {
	ret := _m.ctrl.Call(_m, "CheckSchema", _param0, _param1)
	ret0, _ := ret[0].(*dosa.SchemaStatus)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockAdminClientRecorder) CheckSchema(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "CheckSchema", arg0, arg1)
}

// CheckSchemaStatus is a mock implementation of MockAdminClient.CheckSchemaStatus
func (_m *MockAdminClient) CheckSchemaStatus(_param0 context.Context, _param1 string, _param2 int32) (*dosa.SchemaStatus, error) {
	ret := _m.ctrl.Call(_m, "CheckSchemaStatus", _param0, _param1, _param2)
	ret0, _ := ret[0].(*dosa.SchemaStatus)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockAdminClientRecorder) CheckSchemaStatus(arg0, arg1, arg2 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "CheckSchemaStatus", arg0, arg1, arg2)
}

// CreateScope is a mock implementation of MockAdminClient.CreateScope
func (_m *MockAdminClient) CreateScope(_param0 context.Context, _param1 string) error {
	ret := _m.ctrl.Call(_m, "CreateScope", _param0, _param1)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockAdminClientRecorder) CreateScope(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "CreateScope", arg0, arg1)
}

// Directories is a mock implementation of MockAdminClient.Directories
func (_m *MockAdminClient) Directories(_param0 []string) dosa.AdminClient {
	ret := _m.ctrl.Call(_m, "Directories", _param0)
	ret0, _ := ret[0].(dosa.AdminClient)
	return ret0
}

func (_mr *_MockAdminClientRecorder) Directories(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Directories", arg0)
}

// DropScope is a mock implementation of MockAdminClient.DropScope
func (_m *MockAdminClient) DropScope(_param0 context.Context, _param1 string) error {
	ret := _m.ctrl.Call(_m, "DropScope", _param0, _param1)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockAdminClientRecorder) DropScope(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "DropScope", arg0, arg1)
}

// Excludes is a mock implementation of MockAdminClient.Excludes
func (_m *MockAdminClient) Excludes(_param0 []string) dosa.AdminClient {
	ret := _m.ctrl.Call(_m, "Excludes", _param0)
	ret0, _ := ret[0].(dosa.AdminClient)
	return ret0
}

func (_mr *_MockAdminClientRecorder) Excludes(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Excludes", arg0)
}

// GetSchema is a mock implementation of MockAdminClient.GetSchema
func (_m *MockAdminClient) GetSchema() ([]*dosa.EntityDefinition, error) {
	ret := _m.ctrl.Call(_m, "GetSchema")
	ret0, _ := ret[0].([]*dosa.EntityDefinition)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockAdminClientRecorder) GetSchema() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "GetSchema")
}

// Scope is a mock implementation of MockAdminClient.Scope
func (_m *MockAdminClient) Scope(_param0 string) dosa.AdminClient {
	ret := _m.ctrl.Call(_m, "Scope", _param0)
	ret0, _ := ret[0].(dosa.AdminClient)
	return ret0
}

func (_mr *_MockAdminClientRecorder) Scope(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Scope", arg0)
}

// TruncateScope is a mock implementation of MockAdminClient.TruncateScope
func (_m *MockAdminClient) TruncateScope(_param0 context.Context, _param1 string) error {
	ret := _m.ctrl.Call(_m, "TruncateScope", _param0, _param1)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockAdminClientRecorder) TruncateScope(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "TruncateScope", arg0, arg1)
}

// UpsertSchema is a mock implementation of MockAdminClient.UpsertSchema
func (_m *MockAdminClient) UpsertSchema(_param0 context.Context, _param1 string) (*dosa.SchemaStatus, error) {
	ret := _m.ctrl.Call(_m, "UpsertSchema", _param0, _param1)
	ret0, _ := ret[0].(*dosa.SchemaStatus)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockAdminClientRecorder) UpsertSchema(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "UpsertSchema", arg0, arg1)
}
