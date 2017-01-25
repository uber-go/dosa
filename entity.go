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

import "github.com/pkg/errors"

// Table represents a parsed entity format on the client side
// In addition to shared EntityDefinition, it records struct name and field names.
type Table struct {
	EntityDefinition
	StructName string
	ColToField map[string]string // map from column name -> field name
	FieldToCol map[string]string // map from field name -> column name
}

// ClusteringKey stores name and ordering of a clustering key
type ClusteringKey struct {
	Name       string
	Descending bool
}

// PrimaryKey stores information about partition keys and clustering keys
type PrimaryKey struct {
	PartitionKeys  []string
	ClusteringKeys []*ClusteringKey
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
	Columns []*ColumnDefinition
}

// EnsureValid ensures the entity definition is valid.
// All the names used (entity name, column name) must be valid.
// No duplicate names can be used in column names or key names.
// The primary key must not be nil and must contain at least one partition key.
func (e *EntityDefinition) EnsureValid() error {
	if e == nil {
		return errors.New("EntityDefinition is nil")
	}

	if err := IsValidName(e.Name); err != nil {
		return errors.Wrap(err, "EntityDefinition has invalid name")
	}

	columnNamesSeen := map[string]struct{}{}
	for _, c := range e.Columns {
		if c == nil {
			return errors.New("EntityDefinition has nil column")
		}
		if err := IsValidName(c.Name); err != nil {
			return errors.Wrap(err, "EntityDefinition has invalid column name")
		}
		if _, ok := columnNamesSeen[c.Name]; ok {
			return errors.Errorf("duplicated column found: %q", c.Name)
		}
		if c.Type == Invalid {
			return errors.Errorf("invalid type for column: %q", c.Name)
		}
		columnNamesSeen[c.Name] = struct{}{}
	}

	if e.Key == nil {
		return errors.New("EntityDefinition has nil primary key")
	}

	if len(e.Key.PartitionKeys) == 0 {
		return errors.New("EntityDefinition does not have partition key")
	}

	keyNamesSeen := map[string]struct{}{}
	for _, p := range e.Key.PartitionKeys {
		if _, ok := columnNamesSeen[p]; !ok {
			return errors.Errorf("partition key does not refer to a column: %q", p)
		}
		if _, ok := keyNamesSeen[p]; ok {
			return errors.Errorf("a column cannot be used twice in key: %q", p)
		}
		keyNamesSeen[p] = struct{}{}
	}

	for _, c := range e.Key.ClusteringKeys {
		if c == nil {
			return errors.New("EntityDefinition has invalid nil clustering key")
		}

		if _, ok := columnNamesSeen[c.Name]; !ok {
			return errors.Errorf("clustering key does not refer to a column: %q", c.Name)
		}

		if _, ok := keyNamesSeen[c.Name]; ok {
			return errors.Errorf("a column cannot be used twice in key: %q", c.Name)
		}
		keyNamesSeen[c.Name] = struct{}{}
	}

	return nil
}
