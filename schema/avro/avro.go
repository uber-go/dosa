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

package avro

import (
	"encoding/json"
	"fmt"

	gv "github.com/elodina/go-avro"
	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
)

const (
	clusteringKeys = "ClusteringKeys"
	partitionKeys  = "PartitionKeys"
	nameKey        = "Name"
	descendingKey  = "Descending"
	dosaTypeKey    = "dosaType"
	indexKeys      = "Indexes"
	indexKeyField  = "Key"
)

// map from dosa type to avro type
var avroTypes = map[dosa.Type]gv.Schema{
	dosa.String:    &gv.StringSchema{},
	dosa.Blob:      &gv.BytesSchema{},
	dosa.Bool:      &gv.BooleanSchema{},
	dosa.Double:    &gv.DoubleSchema{},
	dosa.Int32:     &gv.IntSchema{},
	dosa.Int64:     &gv.LongSchema{},
	dosa.Timestamp: &gv.LongSchema{},
	dosa.TUUID:     &gv.StringSchema{},
}

// Record implements Schema and represents Avro record type.
type Record struct {
	Name       string                 `json:"name,omitempty"`
	Namespace  string                 `json:"namespace,omitempty"`
	Doc        string                 `json:"doc,omitempty"`
	Aliases    []string               `json:"aliases,omitempty"`
	Properties map[string]interface{} `json:"meta, omitempty"`
	Fields     []*Field               `json:"fields"`
}

// String returns a JSON representation of RecordSchema.
func (s *Record) String() string {
	bytes, err := json.MarshalIndent(s, "", "    ")
	if err != nil {
		panic(err)
	}

	return string(bytes)
}

// MarshalJSON serializes the given schema as JSON.
func (s *Record) MarshalJSON() ([]byte, error) {
	m := make(map[string]interface{})

	m["type"] = "record"

	if len(s.Name) > 0 {
		m["name"] = s.Name
	}

	if len(s.Namespace) > 0 {
		m["namespace"] = s.Namespace
	}

	if len(s.Doc) > 0 {
		m["doc"] = s.Doc
	}
	if len(s.Aliases) > 0 {
		m["aliases"] = s.Aliases
	}

	m["fields"] = s.Fields
	for k, v := range s.Properties {
		m[k] = v
	}
	return json.Marshal(m)
}

// Field represents a schema field for Avro record.
type Field struct {
	Name       string      `json:"name,omitempty"`
	Doc        string      `json:"doc,omitempty"`
	Default    interface{} `json:"default"`
	Type       gv.Schema   `json:"type,omitempty"`
	Properties map[string]string
}

// MarshalJSON serializes the given schema field as JSON.
func (s *Field) MarshalJSON() ([]byte, error) {
	m := make(map[string]interface{})
	if s.Type != nil {
		m["type"] = s.Type
	}
	if len(s.Name) > 0 {
		m["name"] = s.Name
	}

	if s.Type.Type() == gv.Null || (s.Type.Type() == gv.Union && s.Type.(*gv.UnionSchema).Types[0].Type() == gv.Null) || s.Default != nil {
		m["default"] = s.Default
	}
	if len(s.Doc) > 0 {
		m["doc"] = s.Doc
	}

	for k, v := range s.Properties {
		m[k] = v
	}
	return json.Marshal(m)
}

// ToAvro converts dosa entity definition to avro schema
func ToAvro(namePrefix string, ed *dosa.EntityDefinition) ([]byte, error) {
	fields := make([]*Field, len(ed.Columns))
	for i, c := range ed.Columns {
		props := make(map[string]string)
		props[dosaTypeKey] = c.Type.String()
		// TODO add tags
		fields[i] = &Field{
			Name:       c.Name,
			Type:       avroTypes[c.Type],
			Properties: props,
			Default:    nil,
		}
	}

	meta := make(map[string]interface{})
	meta[partitionKeys] = ed.Key.PartitionKeys
	meta[clusteringKeys] = ed.Key.ClusteringKeys
	meta[indexKeys] = ed.Indexes

	ar := &Record{
		Name:       ed.Name,
		Namespace:  namePrefix,
		Fields:     fields,
		Properties: meta,
	}

	bs, err := ar.MarshalJSON()
	if err != nil {
		return nil, errors.Wrap(err, "failed to serialize avro schema into json")
	}
	return bs, nil
}

// FromAvro converts avro schema to dosa entity definition
func FromAvro(data string) (*dosa.EntityDefinition, error) {
	schema, err := gv.ParseSchema(data)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse avro schema from json")
	}

	pks, err := decodePartitionKeysFromSchema(schema)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse avro schema for partition keys")
	}

	cks, err := decodeClusteringKeysFromSchema(schema)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse avro schema for clustering keys")
	}

	rs, ok := schema.(*gv.RecordSchema)
	if !ok {
		return nil, errors.New("fail to parse avro schema")
	}

	cols, err := decodeFields(rs.Fields)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse avro schema for fields")
	}

	// index
	idx, err := decodeIndexes(schema)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse avro schema for index")
	}

	return &dosa.EntityDefinition{
		Name: schema.GetName(),
		Key: &dosa.PrimaryKey{
			PartitionKeys:  pks,
			ClusteringKeys: cks,
		},
		Columns: cols,
		Indexes: idx,
	}, nil
}

func decodeFields(fields []*gv.SchemaField) ([]*dosa.ColumnDefinition, error) {
	cols := make([]*dosa.ColumnDefinition, len(fields))
	for i, f := range fields {
		dosaType, ok := f.Prop(dosaTypeKey)
		if !ok {
			return nil, fmt.Errorf("cannot find %s key in the field", dosaTypeKey)
		}

		t, ok := dosaType.(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert %s to string", dosaTypeKey)
		}

		col := &dosa.ColumnDefinition{
			Name: f.Name,
			Type: dosa.FromString(t),
		}
		cols[i] = col
	}
	return cols, nil
}

func decodePartitionKeysFromSchema(schema gv.Schema) ([]string, error) {
	if prop, ok := schema.Prop(partitionKeys); ok {
		return decodePartitionKeys(prop)
	}
	return nil, fmt.Errorf("cannot find %s key in the schema", partitionKeys)
}

func decodePartitionKeys(prop interface{}) ([]string, error) {
	realPks, ok := prop.([]interface{})
	if !ok {
		return nil, fmt.Errorf("failed to parse partition keys: %v", prop)
	}

	pks := make([]string, len(realPks))
	for i, v := range realPks {
		pks[i], ok = v.(string)
		if !ok {
			return nil, fmt.Errorf("failed to parse partition keys: %v", prop)
		}
	}
	return pks, nil
}

func decodeIndexes(schema gv.Schema) (map[string]*dosa.IndexDefinition, error) {
	idx := make(map[string]*dosa.IndexDefinition)
	if prop, ok := schema.Prop(indexKeys); ok {
		realIndexes, ok := prop.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("failed to parse index: %v", prop)
		}

		for key, index := range realIndexes {
			realIndex, ok := index.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("failed to parse index: %v", index)
			}

			realKey, ok := realIndex[indexKeyField].(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("failed to parse index key: %v", index)
			}

			pks, err := decodePartitionKeys(realKey[partitionKeys])
			if err != nil {
				return nil, errors.Wrap(err, "failed to parse avro index partition keys")
			}

			cks, err := decodeClusteringKeys(realKey[clusteringKeys])
			if err != nil {
				return nil, errors.Wrap(err, "failed to parse avro index clustering keys")
			}

			idx[key] = &dosa.IndexDefinition{
				Key: &dosa.PrimaryKey{
					PartitionKeys:  pks,
					ClusteringKeys: cks,
				},
			}
		}
		return idx, nil
	}
	return nil, fmt.Errorf("cannot find %s index in the schema", indexKeys)
}

func decodeClusteringKeysFromSchema(schema gv.Schema) ([]*dosa.ClusteringKey, error) {
	if prop, ok := schema.Prop(clusteringKeys); ok {
		return decodeClusteringKeys(prop)
	}
	return nil, fmt.Errorf("cannot find %s key in the schema", clusteringKeys)
}

func decodeClusteringKeys(prop interface{}) ([]*dosa.ClusteringKey, error) {
	// cluster key is optional
	if prop == nil {
		return nil, nil
	}

	realCks, ok := prop.([]interface{})
	if !ok {
		return nil, fmt.Errorf("failed to parse clustering keys: %v", prop)
	}

	cks := make([]*dosa.ClusteringKey, len(realCks))
	for i, v := range realCks {
		pair, ok := v.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("failed to parse clustering key: %v", v)
		}

		name, ok := pair[nameKey]
		if !ok {
			return nil, fmt.Errorf("cannot find %s key in %v", nameKey, pair)
		}
		ck := &dosa.ClusteringKey{}
		ck.Name, ok = name.(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert %v to string", name)
		}

		descending, ok := pair[descendingKey]
		if !ok {
			return nil, fmt.Errorf("cannot find %s key in %v", descendingKey, pair)
		}
		ck.Descending, ok = descending.(bool)
		if !ok {
			return nil, fmt.Errorf("failed to convert %v to bool", descending)
		}

		cks[i] = ck
	}

	return cks, nil
}
