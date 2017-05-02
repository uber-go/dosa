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
// Source: github.com/uber-go/dosa (interfaces: Connector)

package mocks

import (
	context "context"
	gomock "github.com/golang/mock/gomock"
	dosa "github.com/uber-go/dosa"
)

// Mock of Connector interface
type MockConnector struct {
	ctrl     *gomock.Controller
	recorder *_MockConnectorRecorder
}

// Recorder for MockConnector (not exported)
type _MockConnectorRecorder struct {
	mock *MockConnector
}

func NewMockConnector(ctrl *gomock.Controller) *MockConnector {
	mock := &MockConnector{ctrl: ctrl}
	mock.recorder = &_MockConnectorRecorder{mock}
	return mock
}

func (_m *MockConnector) EXPECT() *_MockConnectorRecorder {
	return _m.recorder
}

func (_m *MockConnector) CheckSchema(_param0 context.Context, _param1 string, _param2 string, _param3 []*dosa.EntityDefinition) (int32, error) {
	ret := _m.ctrl.Call(_m, "CheckSchema", _param0, _param1, _param2, _param3)
	ret0, _ := ret[0].(int32)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockConnectorRecorder) CheckSchema(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "CheckSchema", arg0, arg1, arg2, arg3)
}

func (_m *MockConnector) CheckSchemaStatus(_param0 context.Context, _param1 string, _param2 string, _param3 int32) (*dosa.SchemaStatus, error) {
	ret := _m.ctrl.Call(_m, "CheckSchemaStatus", _param0, _param1, _param2, _param3)
	ret0, _ := ret[0].(*dosa.SchemaStatus)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockConnectorRecorder) CheckSchemaStatus(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "CheckSchemaStatus", arg0, arg1, arg2, arg3)
}

func (_m *MockConnector) CreateIfNotExists(_param0 context.Context, _param1 *dosa.EntityInfo, _param2 map[string]dosa.FieldValue) error {
	ret := _m.ctrl.Call(_m, "CreateIfNotExists", _param0, _param1, _param2)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockConnectorRecorder) CreateIfNotExists(arg0, arg1, arg2 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "CreateIfNotExists", arg0, arg1, arg2)
}

func (_m *MockConnector) CreateScope(_param0 context.Context, _param1 string) error {
	ret := _m.ctrl.Call(_m, "CreateScope", _param0, _param1)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockConnectorRecorder) CreateScope(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "CreateScope", arg0, arg1)
}

func (_m *MockConnector) DropScope(_param0 context.Context, _param1 string) error {
	ret := _m.ctrl.Call(_m, "DropScope", _param0, _param1)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockConnectorRecorder) DropScope(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "DropScope", arg0, arg1)
}

func (_m *MockConnector) MultiRead(_param0 context.Context, _param1 *dosa.EntityInfo, _param2 []map[string]dosa.FieldValue, _param3 []string) ([]*dosa.FieldValuesOrError, error) {
	ret := _m.ctrl.Call(_m, "MultiRead", _param0, _param1, _param2, _param3)
	ret0, _ := ret[0].([]*dosa.FieldValuesOrError)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockConnectorRecorder) MultiRead(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "MultiRead", arg0, arg1, arg2, arg3)
}

func (_m *MockConnector) MultiRemove(_param0 context.Context, _param1 *dosa.EntityInfo, _param2 []map[string]dosa.FieldValue) ([]error, error) {
	ret := _m.ctrl.Call(_m, "MultiRemove", _param0, _param1, _param2)
	ret0, _ := ret[0].([]error)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockConnectorRecorder) MultiRemove(arg0, arg1, arg2 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "MultiRemove", arg0, arg1, arg2)
}

func (_m *MockConnector) MultiUpsert(_param0 context.Context, _param1 *dosa.EntityInfo, _param2 []map[string]dosa.FieldValue) ([]error, error) {
	ret := _m.ctrl.Call(_m, "MultiUpsert", _param0, _param1, _param2)
	ret0, _ := ret[0].([]error)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockConnectorRecorder) MultiUpsert(arg0, arg1, arg2 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "MultiUpsert", arg0, arg1, arg2)
}

func (_m *MockConnector) Range(_param0 context.Context, _param1 *dosa.EntityInfo, _param2 map[string][]*dosa.Condition, _param3 []string, _param4 string, _param5 int) ([]map[string]dosa.FieldValue, string, error) {
	ret := _m.ctrl.Call(_m, "Range", _param0, _param1, _param2, _param3, _param4, _param5)
	ret0, _ := ret[0].([]map[string]dosa.FieldValue)
	ret1, _ := ret[1].(string)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

func (_mr *_MockConnectorRecorder) Range(arg0, arg1, arg2, arg3, arg4, arg5 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Range", arg0, arg1, arg2, arg3, arg4, arg5)
}

func (_m *MockConnector) Read(_param0 context.Context, _param1 *dosa.EntityInfo, _param2 map[string]dosa.FieldValue, _param3 []string) (map[string]dosa.FieldValue, error) {
	ret := _m.ctrl.Call(_m, "Read", _param0, _param1, _param2, _param3)
	ret0, _ := ret[0].(map[string]dosa.FieldValue)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockConnectorRecorder) Read(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Read", arg0, arg1, arg2, arg3)
}

func (_m *MockConnector) Remove(_param0 context.Context, _param1 *dosa.EntityInfo, _param2 map[string]dosa.FieldValue) error {
	ret := _m.ctrl.Call(_m, "Remove", _param0, _param1, _param2)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockConnectorRecorder) Remove(arg0, arg1, arg2 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Remove", arg0, arg1, arg2)
}

func (_m *MockConnector) Scan(_param0 context.Context, _param1 *dosa.EntityInfo, _param2 []string, _param3 string, _param4 int) ([]map[string]dosa.FieldValue, string, error) {
	ret := _m.ctrl.Call(_m, "Scan", _param0, _param1, _param2, _param3, _param4)
	ret0, _ := ret[0].([]map[string]dosa.FieldValue)
	ret1, _ := ret[1].(string)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

func (_mr *_MockConnectorRecorder) Scan(arg0, arg1, arg2, arg3, arg4 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Scan", arg0, arg1, arg2, arg3, arg4)
}

func (_m *MockConnector) ScopeExists(_param0 context.Context, _param1 string) (bool, error) {
	ret := _m.ctrl.Call(_m, "ScopeExists", _param0, _param1)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockConnectorRecorder) ScopeExists(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ScopeExists", arg0, arg1)
}

func (_m *MockConnector) Search(_param0 context.Context, _param1 *dosa.EntityInfo, _param2 dosa.FieldNameValuePair, _param3 []string, _param4 string, _param5 int) ([]map[string]dosa.FieldValue, string, error) {
	ret := _m.ctrl.Call(_m, "Search", _param0, _param1, _param2, _param3, _param4, _param5)
	ret0, _ := ret[0].([]map[string]dosa.FieldValue)
	ret1, _ := ret[1].(string)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

func (_mr *_MockConnectorRecorder) Search(arg0, arg1, arg2, arg3, arg4, arg5 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Search", arg0, arg1, arg2, arg3, arg4, arg5)
}

func (_m *MockConnector) Shutdown() error {
	ret := _m.ctrl.Call(_m, "Shutdown")
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockConnectorRecorder) Shutdown() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Shutdown")
}

func (_m *MockConnector) TruncateScope(_param0 context.Context, _param1 string) error {
	ret := _m.ctrl.Call(_m, "TruncateScope", _param0, _param1)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockConnectorRecorder) TruncateScope(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "TruncateScope", arg0, arg1)
}

func (_m *MockConnector) Upsert(_param0 context.Context, _param1 *dosa.EntityInfo, _param2 map[string]dosa.FieldValue) error {
	ret := _m.ctrl.Call(_m, "Upsert", _param0, _param1, _param2)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockConnectorRecorder) Upsert(arg0, arg1, arg2 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Upsert", arg0, arg1, arg2)
}

func (_m *MockConnector) UpsertSchema(_param0 context.Context, _param1 string, _param2 string, _param3 []*dosa.EntityDefinition) (*dosa.SchemaStatus, error) {
	ret := _m.ctrl.Call(_m, "UpsertSchema", _param0, _param1, _param2, _param3)
	ret0, _ := ret[0].(*dosa.SchemaStatus)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockConnectorRecorder) UpsertSchema(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "UpsertSchema", arg0, arg1, arg2, arg3)
}
