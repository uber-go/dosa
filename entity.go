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

import (
	"bytes"
	"strings"

	"reflect"

	"github.com/pkg/errors"
)

// Table represents a parsed entity format on the client side
// In addition to shared EntityDefinition, it records struct name and field names.
type Table struct {
	EntityDefinition
	Indexes    []*IndexDefinition
	StructName string
	ColToField map[string]string // map from column name -> field name
	FieldToCol map[string]string // map from field name -> column name
}

// ClusteringKey stores name and ordering of a clustering key
type ClusteringKey struct {
	Name       string
	Descending bool
}

// String takes a ClusteringKey and returns "column-name ASC|DESC"
func (ck ClusteringKey) String() string {
	if ck.Descending {
		return ck.Name + " DESC"
	}
	return ck.Name + " ASC"
}

// PrimaryKey stores information about partition keys and clustering keys
type PrimaryKey struct {
	PartitionKeys  []string
	ClusteringKeys []*ClusteringKey
}

// formatClusteringKeys takes an array of ClusteringKeys and returns
// a string that shows all of them, separated by commas
func formatClusteringKeys(keys []*ClusteringKey) string {
	pieces := make([]string, len(keys))
	for index, ck := range keys {
		pieces[index] = ck.String()
	}
	return strings.Join(pieces, ", ")
}

func formatPartitionKeys(keys []string) string {
	if len(keys) > 1 {
		return "(" + strings.Join(keys, ", ") + ")"
	}
	return keys[0]
}

// String method produces the following output:
// for multiple partition keys: ((partition-key, ...), clustering-key ASC/DESC, ...)
// for one partition key: (partition-key, clustering-key ASC/DESC, ...)
func (pk PrimaryKey) String() string {
	var b bytes.Buffer
	b.WriteByte('(')
	b.WriteString(formatPartitionKeys(pk.PartitionKeys))
	if pk.ClusteringKeys != nil && len(pk.ClusteringKeys) > 0 {
		b.WriteString(", ")
		b.WriteString(formatClusteringKeys(pk.ClusteringKeys))
	}
	b.WriteByte(')')
	return b.String()
}

// ColumnDefinition stores information about a column
type ColumnDefinition struct {
	Name string // normalized column name
	Type Type
	// TODO: change as need to support tags like pii, searchable, etc
	// currently it's in the form of a map from tag name to (optional) tag value
	Tags map[string]string
}

// IndexDefinition stores information about a DOSA entity's index
type IndexDefinition struct {
	Name string // normalized index name
	Key  *PrimaryKey
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

	if err := e.ensureNonNullablePrimaryKeys(); err != nil {
		return errors.Wrap(err, "Nullable types are not allowed as primary key(s)")
	}

	return nil
}

func (e *EntityDefinition) ensureNonNullablePrimaryKeys() error {
	columnTypes := e.ColumnTypes()

	for k := range e.PartitionKeySet() {
		if isNullableType(columnTypes[k]) {
			return errors.Errorf("primary key is of nullable type: %q", k)
		}
	}

	for k := range e.ClusteringKeySet() {
		if isNullableType(columnTypes[k]) {
			return errors.Errorf("clustering key is of nullable type: %q", k)
		}
	}
	return nil
}

// ColumnTypes returns a map of column name to column type for all columns.
func (e *EntityDefinition) ColumnTypes() map[string]Type {
	m := make(map[string]Type)
	for _, c := range e.Columns {
		m[c.Name] = c.Type
	}
	return m
}

// PartitionKeySet returns a set of all partition keys.
func (e *EntityDefinition) PartitionKeySet() map[string]struct{} {
	m := make(map[string]struct{})
	for _, p := range e.Key.PartitionKeys {
		m[p] = struct{}{}
	}
	return m
}

// ClusteringKeySet returns a set of all clustering keys.
func (e *EntityDefinition) ClusteringKeySet() map[string]struct{} {
	m := make(map[string]struct{})
	for _, c := range e.Key.ClusteringKeys {
		m[c.Name] = struct{}{}
	}
	return m
}

// KeySet returns a set of all keys, including partition keys and clustering keys.
func (e *EntityDefinition) KeySet() map[string]struct{} {
	m := e.ClusteringKeySet()
	pks := e.PartitionKeySet()
	for p := range pks {
		m[p] = struct{}{}
	}
	return m
}

// IsCompatible checks if two entity definitions are compatible or not.
// e1.g. edA.IsCompatible(edB) return true, means edA is compatible with edB.
// edA is the one to compare and edB is the one to be compared.
func (e *EntityDefinition) IsCompatible(e2 *EntityDefinition) error {
	// for better naming
	e1 := e

	// entity name should be the same
	if e1.Name != e2.Name {
		return errors.Errorf("entity name mismatch: (%s vs %s)", e1.Name, e2.Name)
	}

	// primary key should be exactly same
	pks1 := e1.Key.PartitionKeys
	pks2 := e2.Key.PartitionKeys

	b := reflect.DeepEqual(pks1, pks2)
	if !b {
		return errors.Errorf("partition key mismatch: (%v vs %v)", pks1, pks2)
	}

	cks1 := e1.Key.ClusteringKeys
	cks2 := e2.Key.ClusteringKeys
	if len(cks2) != 0 || len(cks1) != 0 {
		b = reflect.DeepEqual(cks1, cks2)
		if !b {
			return errors.Errorf("clustering key mismatch: (%v vs %v)", cks1, cks2)
		}
	}
	// only allow to add new columns
	colsMap1 := e1.ColumnTypes()
	colsMap2 := e2.ColumnTypes()

	for name, colType2 := range colsMap2 {
		colType1, ok := colsMap1[name]
		if !ok {
			return errors.Errorf("the column %s in entity %s but not in entity %s", name, e1.Name, e2.Name)
		}
		if colType1 != colType2 {
			return errors.Errorf("the type for column %s mismatch: (%v vs %v)", name, colType1, colType2)
		}
	}

	// TODO Handle tags comparison in the future

	return nil
}

// FindColumnDefinition finds the column definition by the column name
func (e *EntityDefinition) FindColumnDefinition(name string) *ColumnDefinition {
	for _, cd := range e.Columns {
		if cd.Name == name {
			return cd
		}
	}
	return nil
}
