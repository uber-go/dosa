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

package yarpc

import (
	"time"

	"github.com/uber-go/dosa"
	dosarpc "github.com/uber/dosa-idl/.gen/dosa"
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
		// If we set nil to BinaryValue, thrift cannot encode it
		// as it thought we didn't set any field in the union
		if v == nil {
			v = []byte{}
		}
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

// RPCTypeFromClientType returns the RPC ElemType from a DOSA Type
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

// RPCTypeToClientType returns the DOSA Type from RPC ElemType
func RPCTypeToClientType(t dosarpc.ElemType) dosa.Type {
	switch t {
	case dosarpc.ElemTypeBool:
		return dosa.Bool
	case dosarpc.ElemTypeBlob:
		return dosa.Blob
	case dosarpc.ElemTypeString:
		return dosa.String
	case dosarpc.ElemTypeInt32:
		return dosa.Int32
	case dosarpc.ElemTypeInt64:
		return dosa.Int64
	case dosarpc.ElemTypeDouble:
		return dosa.Double
	case dosarpc.ElemTypeTimestamp:
		return dosa.Timestamp
	case dosarpc.ElemTypeUUID:
		return dosa.TUUID
	}
	panic("bad type")
}

// EntityDefinitionToThrift converts the client EntityDefinition to the RPC EntityDefinition
func EntityDefinitionToThrift(ed *dosa.EntityDefinition) *dosarpc.EntityDefinition {
	ck := make([]*dosarpc.ClusteringKey, len(ed.Key.ClusteringKeys))
	for ckinx, clusteringKey := range ed.Key.ClusteringKeys {
		// TODO: The client uses 'descending' but the RPC uses 'ascending'? Fix this insanity!
		ascending := !clusteringKey.Descending
		name := clusteringKey.Name
		ck[ckinx] = &dosarpc.ClusteringKey{Name: &name, Asc: &ascending}
	}
	pk := dosarpc.PrimaryKey{PartitionKeys: ed.Key.PartitionKeys, ClusteringKeys: ck}
	fd := make(map[string]*dosarpc.FieldDesc, len(ed.Columns))
	for _, column := range ed.Columns {
		rpcType := RPCTypeFromClientType(column.Type)
		fd[column.Name] = &dosarpc.FieldDesc{Type: &rpcType}
	}
	name := ed.Name
	return &dosarpc.EntityDefinition{PrimaryKey: &pk, FieldDescs: fd, Name: &name}
}

// FromThriftToEntityDefinition converts the RPC EntityDefinition to client EntityDefinition
func FromThriftToEntityDefinition(ed *dosarpc.EntityDefinition) *dosa.EntityDefinition {
	fields := make([]*dosa.ColumnDefinition, len(ed.FieldDescs))
	i := 0
	for k, v := range ed.FieldDescs {
		fields[i] = &dosa.ColumnDefinition{
			Name: k,
			Type: RPCTypeToClientType(*v.Type),
			// TODO Tag
		}
		i++
	}
	pk := ed.PrimaryKey.PartitionKeys
	ck := make([]*dosa.ClusteringKey, len(ed.PrimaryKey.ClusteringKeys))
	for i, v := range ed.PrimaryKey.ClusteringKeys {
		ck[i] = &dosa.ClusteringKey{
			Name:       *v.Name,
			Descending: !*v.Asc,
		}
	}

	return &dosa.EntityDefinition{
		Name:    *ed.Name,
		Columns: fields,
		Key: &dosa.PrimaryKey{
			PartitionKeys:  pk,
			ClusteringKeys: ck,
		},
	}
}
func encodeOperator(o dosa.Operator) *dosarpc.Operator {
	var op dosarpc.Operator
	switch o {
	case dosa.Eq:
		op = dosarpc.OperatorEq
	case dosa.Gt:
		op = dosarpc.OperatorGt
	case dosa.GtOrEq:
		op = dosarpc.OperatorGtOrEq
	case dosa.Lt:
		op = dosarpc.OperatorLt
	case dosa.LtOrEq:
		op = dosarpc.OperatorLtOrEq
	}
	return &op
}

func decodeResults(ei *dosa.EntityInfo, invals dosarpc.FieldValueMap) map[string]dosa.FieldValue {
	result := map[string]dosa.FieldValue{}
	// TODO: create a typemap to make this faster
	for name, value := range invals {
		for _, col := range ei.Def.Columns {
			if col.Name == name {
				result[name] = RawValueAsInterface(*value.ElemValue, col.Type)
				break
			}
		}
	}
	return result
}

func makeRPCFieldsToRead(fieldsToRead []string) map[string]struct{} {
	var rpcFieldsToRead map[string]struct{}
	if fieldsToRead != nil {
		rpcFieldsToRead = map[string]struct{}{}
		for _, field := range fieldsToRead {
			rpcFieldsToRead[field] = struct{}{}
		}
	}
	return rpcFieldsToRead
}
func entityInfoToSchemaRef(ei *dosa.EntityInfo) *dosarpc.SchemaRef {
	scope := ei.Ref.Scope
	namePrefix := ei.Ref.NamePrefix
	entityName := ei.Ref.EntityName
	version := ei.Ref.Version
	sr := dosarpc.SchemaRef{
		Scope:      &scope,
		NamePrefix: &namePrefix,
		EntityName: &entityName,
		Version:    &version,
	}
	return &sr
}

func fieldValueMapFromClientMap(values map[string]dosa.FieldValue) dosarpc.FieldValueMap {
	fields := dosarpc.FieldValueMap{}
	for name, value := range values {
		rpcValue := &dosarpc.Value{ElemValue: RawValueFromInterface(value)}
		fields[name] = rpcValue
	}
	return fields
}
