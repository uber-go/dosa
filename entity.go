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

package dosa

// Table represents a parsed entity format on the client side
// In addition to shared EntityDefinition, it records struct name and field names.
type Table struct {
	EntityDefinition
	StructName string
	FieldNames map[string]string // map from column name -> field name
}

// ClusteringKey stores name and ordering of a clustering key
type ClusteringKey struct {
	Name       string
	Descending bool
}

// PrimaryKey stores information about partition keys and clustering keys
type PrimaryKey struct {
	PartitionKeys  []string
	ClusteringKeys []ClusteringKey
}

// ColumnDefinition stores information about a column
type ColumnDefinition struct {
	Name string // normalized column name
	Type Type
	// TODO: change as need to support tags like pii, searchable, etc
	// currently it's in the form of a map from tag name to (optional) tag value
	Tags map[string]string
}

// EntityDefinition stores information about a DOSA entity
type EntityDefinition struct {
	Name    string // normalized entity name
	Key     *PrimaryKey
	Columns []ColumnDefinition
}
