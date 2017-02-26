// Automatically generated by MockGen. DO NOT EDIT!
// Source: connector.go

package mocks

import (
	context "context"

	gomock "github.com/golang/mock/gomock"
	"github.com/uber-go/dosa"
)

// Mock of dosa.FieldValue interface
type MockFieldValue struct {
	ctrl     *gomock.Controller
	recorder *_MockFieldValueRecorder
}

// Recorder for MockFieldValue (not exported)
type _MockFieldValueRecorder struct {
	mock *MockFieldValue
}

func NewMockFieldValue(ctrl *gomock.Controller) *MockFieldValue {
	mock := &MockFieldValue{ctrl: ctrl}
	mock.recorder = &_MockFieldValueRecorder{mock}
	return mock
}

func (_m *MockFieldValue) EXPECT() *_MockFieldValueRecorder {
	return _m.recorder
}

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

func (_m *MockConnector) CreateIfNotExists(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	ret := _m.ctrl.Call(_m, "CreateIfNotExists", ctx, ei, values)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockConnectorRecorder) CreateIfNotExists(arg0, arg1, arg2 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "CreateIfNotExists", arg0, arg1, arg2)
}

func (_m *MockConnector) Read(ctx context.Context, ei *dosa.EntityInfo, keys map[string]dosa.FieldValue, fieldsToRead []string) (map[string]dosa.FieldValue, error) {
	ret := _m.ctrl.Call(_m, "Read", ctx, ei, keys, fieldsToRead)
	ret0, _ := ret[0].(map[string]dosa.FieldValue)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockConnectorRecorder) Read(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Read", arg0, arg1, arg2, arg3)
}

func (_m *MockConnector) MultiRead(ctx context.Context, ei *dosa.EntityInfo, keys []map[string]dosa.FieldValue, fieldsToRead []string) ([]dosa.FieldValuesOrError, error) {
	ret := _m.ctrl.Call(_m, "MultiRead", ctx, ei, keys, fieldsToRead)
	ret0, _ := ret[0].([]dosa.FieldValuesOrError)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockConnectorRecorder) MultiRead(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "MultiRead", arg0, arg1, arg2, arg3)
}

func (_m *MockConnector) Upsert(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	ret := _m.ctrl.Call(_m, "Upsert", ctx, ei, values)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockConnectorRecorder) Upsert(arg0, arg1, arg2 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Upsert", arg0, arg1, arg2)
}

func (_m *MockConnector) MultiUpsert(ctx context.Context, ei *dosa.EntityInfo, multiValues []map[string]dosa.FieldValue) ([]error, error) {
	ret := _m.ctrl.Call(_m, "MultiUpsert", ctx, ei, multiValues)
	ret0, _ := ret[0].([]error)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockConnectorRecorder) MultiUpsert(arg0, arg1, arg2 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "MultiUpsert", arg0, arg1, arg2)
}

func (_m *MockConnector) Remove(ctx context.Context, ei *dosa.EntityInfo, keys map[string]dosa.FieldValue) error {
	ret := _m.ctrl.Call(_m, "Remove", ctx, ei, keys)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockConnectorRecorder) Remove(arg0, arg1, arg2 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Remove", arg0, arg1, arg2)
}

func (_m *MockConnector) MultiRemove(ctx context.Context, ei *dosa.EntityInfo, multiKeys []map[string]dosa.FieldValue) ([]error, error) {
	ret := _m.ctrl.Call(_m, "MultiRemove", ctx, ei, multiKeys)
	ret0, _ := ret[0].([]error)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockConnectorRecorder) MultiRemove(arg0, arg1, arg2 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "MultiRemove", arg0, arg1, arg2)
}

func (_m *MockConnector) Range(ctx context.Context, ei *dosa.EntityInfo, conditions []dosa.Condition, fieldsToRead []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	ret := _m.ctrl.Call(_m, "Range", ctx, ei, conditions, fieldsToRead, token, limit)
	ret0, _ := ret[0].([]map[string]dosa.FieldValue)
	ret1, _ := ret[1].(string)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

func (_mr *_MockConnectorRecorder) Range(arg0, arg1, arg2, arg3, arg4, arg5 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Range", arg0, arg1, arg2, arg3, arg4, arg5)
}

func (_m *MockConnector) Search(ctx context.Context, ei *dosa.EntityInfo, FieldNameValuePair []string, fieldsToRead []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	ret := _m.ctrl.Call(_m, "Search", ctx, ei, FieldNameValuePair, fieldsToRead, token, limit)
	ret0, _ := ret[0].([]map[string]dosa.FieldValue)
	ret1, _ := ret[1].(string)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

func (_mr *_MockConnectorRecorder) Search(arg0, arg1, arg2, arg3, arg4, arg5 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Search", arg0, arg1, arg2, arg3, arg4, arg5)
}

func (_m *MockConnector) Scan(ctx context.Context, ei *dosa.EntityInfo, fieldsToRead []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	ret := _m.ctrl.Call(_m, "Scan", ctx, ei, fieldsToRead, token, limit)
	ret0, _ := ret[0].([]map[string]dosa.FieldValue)
	ret1, _ := ret[1].(string)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

func (_mr *_MockConnectorRecorder) Scan(arg0, arg1, arg2, arg3, arg4 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Scan", arg0, arg1, arg2, arg3, arg4)
}

func (_m *MockConnector) CheckSchema(ctx context.Context, scope string, namePrefix string, ed []*dosa.EntityDefinition) ([]int32, error) {
	ret := _m.ctrl.Call(_m, "CheckSchema", ctx, scope, namePrefix, ed)
	ret0, _ := ret[0].([]int32)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockConnectorRecorder) CheckSchema(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "CheckSchema", arg0, arg1, arg2, arg3)
}

func (_m *MockConnector) UpsertSchema(ctx context.Context, scope string, namePrefix string, ed []*dosa.EntityDefinition) ([]int32, error) {
	ret := _m.ctrl.Call(_m, "UpsertSchema", ctx, scope, namePrefix, ed)
	ret0, _ := ret[0].([]int32)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockConnectorRecorder) UpsertSchema(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "UpsertSchema", arg0, arg1, arg2, arg3)
}

func (_m *MockConnector) CreateScope(ctx context.Context, scope string) error {
	ret := _m.ctrl.Call(_m, "CreateScope", ctx, scope)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockConnectorRecorder) CreateScope(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "CreateScope", arg0, arg1)
}

func (_m *MockConnector) TruncateScope(ctx context.Context, scope string) error {
	ret := _m.ctrl.Call(_m, "TruncateScope", ctx, scope)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockConnectorRecorder) TruncateScope(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "TruncateScope", arg0, arg1)
}

func (_m *MockConnector) DropScope(ctx context.Context, scope string) error {
	ret := _m.ctrl.Call(_m, "DropScope", ctx, scope)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockConnectorRecorder) DropScope(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "DropScope", arg0, arg1)
}

func (_m *MockConnector) ScopeExists(ctx context.Context, scope string) (bool, error) {
	ret := _m.ctrl.Call(_m, "ScopeExists", ctx, scope)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockConnectorRecorder) ScopeExists(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ScopeExists", arg0, arg1)
}

func (_m *MockConnector) Shutdown() error {
	ret := _m.ctrl.Call(_m, "Shutdown")
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockConnectorRecorder) Shutdown() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Shutdown")
}
