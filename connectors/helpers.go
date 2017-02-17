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

package connector

import (
	"github.com/uber-go/dosa"
	dosarpc "github.com/uber/dosa-idl/.gen/dosa"
	"time"
)

// RawValueAsInterface converts a value from the wire to an object implementing the interface
// based on the dosa type. For example, a TUUID type will get a dosa.UUID object
func RawValueAsInterface(val dosarpc.RawValue, typ dosa.Type) interface{} {
	switch typ {
	case dosa.TUUID:
		uuid, _ := dosa.BytesToUUID(val.BinaryValue) // TODO: should we handle this error?
		return uuid
	case dosa.String:
		return *val.StringValue
	case dosa.Int32:
		return *val.Int32Value
	case dosa.Int64:
		return *val.Int64Value
	case dosa.Double:
		return *val.DoubleValue
	case dosa.Blob:
		return val.BinaryValue
	case dosa.Timestamp:
		return time.Unix(0, *val.Int64Value)
	case dosa.Bool:
		return *val.BoolValue
	}
	panic("bad type")
}

// RawValueFromInterface takes an interface, introspects the type, and then
// returns a RawValue object that represents this. It panics if the type
// is not in the list, which should be a dosa bug
func RawValueFromInterface(i interface{}) *dosarpc.RawValue {
	// TODO: Do we do type compatibility checks here? We should know the schema,
	// but the callers are all well known and should match the types
	switch v := i.(type) {
	case string:
		return &dosarpc.RawValue{StringValue: &v}
	case bool:
		return &dosarpc.RawValue{BoolValue: &v}
	case int64:
		return &dosarpc.RawValue{Int64Value: &v}
	case int32:
		return &dosarpc.RawValue{Int32Value: &v}
	case float64:
		return &dosarpc.RawValue{DoubleValue: &v}
	case []byte:
		return &dosarpc.RawValue{BinaryValue: v}
	case time.Time:
		time := v.UnixNano()
		return &dosarpc.RawValue{Int64Value: &time}
	case dosa.UUID:
		bytes, _ := v.Bytes() // TODO: should we handle this error?
		return &dosarpc.RawValue{BinaryValue: bytes}
	}
	panic("bad type")
}

// RPCTypeFromClientType returns the RPC ElemType from a dosa Type
func RPCTypeFromClientType(t dosa.Type) dosarpc.ElemType {
	switch t {
	case dosa.Bool:
		return dosarpc.ElemTypeBool
	case dosa.Blob:
		return dosarpc.ElemTypeBlob
	case dosa.String:
		return dosarpc.ElemTypeString
	case dosa.Int32:
		return dosarpc.ElemTypeInt32
	case dosa.Int64:
		return dosarpc.ElemTypeInt64
	case dosa.Double:
		return dosarpc.ElemTypeDouble
	case dosa.Timestamp:
		return dosarpc.ElemTypeTimestamp
	case dosa.TUUID:
		return dosarpc.ElemTypeUUID
	}
	panic("bad type")
}
