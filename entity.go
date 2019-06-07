// Copyright (c) 2019 Uber Technologies, Inc.
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

	"time"

	"github.com/pkg/errors"
)

// Table represents a parsed entity format on the client side
// In addition to shared EntityDefinition, it records struct name and field names.
type Table struct {
	EntityDefinition
	StructName string
	ColToField map[string]string // map from column name -> field name
	FieldToCol map[string]string // map from field name -> column name
	TTL        time.Duration
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

// Clone returns a deep copy of PrimaryKey
func (pk PrimaryKey) Clone() *PrimaryKey {
	npk := &PrimaryKey{}
	if pk.PartitionKeys != nil {
		npk.PartitionKeys = make([]string, len(pk.PartitionKeys))

		for i, k := range pk.PartitionKeys {
			npk.PartitionKeys[i] = k
		}

	}

	if pk.ClusteringKeys != nil {
		npk.ClusteringKeys = make([]*ClusteringKey, len(pk.ClusteringKeys))
		for i, c := range pk.ClusteringKeys {
			npk.ClusteringKeys[i] = &ClusteringKey{
				Name:       c.Name,
				Descending: c.Descending,
			}
		}
	}

	return npk
}

// ClusteringKeySet returns a set of all clustering keys.
func (pk PrimaryKey) ClusteringKeySet() map[string]struct{} {
	m := make(map[string]struct{})
	for _, c := range pk.ClusteringKeys {
		m[c.Name] = struct{}{}
	}
	return m
}

// PartitionKeySet returns the set of partition keys
func (pk PrimaryKey) PartitionKeySet() map[string]struct{} {
	m := make(map[string]struct{})
	for _, p := range pk.PartitionKeys {
		m[p] = struct{}{}
	}
	return m
}

// PrimaryKeySet returns the union of the set of partition keys and clustering keys
func (pk PrimaryKey) PrimaryKeySet() map[string]struct{} {
	m := pk.ClusteringKeySet()
	for _, p := range pk.PartitionKeys {
		m[p] = struct{}{}
	}
	return m
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
	Name      string // normalized column name
	Type      Type
	IsPointer bool // used by client only to indicate whether this field is pointer
	// TODO: change as need to support tags like pii, etc
	// currently it's in the form of a map from tag name to (optional) tag value
	Tags map[string]string
}

// Clone returns a deep copy of ColumnDefinition
func (cd *ColumnDefinition) Clone() *ColumnDefinition {
	// TODO: clone tag
	return &ColumnDefinition{
		Name: cd.Name,
		Type: cd.Type,
	}
}

// IndexDefinition stores information about a DOSA entity's index
type IndexDefinition struct {
	Key     *PrimaryKey
	Columns []string
}

// Clone returns a deep copy of IndexDefinition
func (id *IndexDefinition) Clone() *IndexDefinition {
	var columns []string
	copy(columns, id.Columns)
	return &IndexDefinition{
		Key:     id.Key.Clone(),
		Columns: columns,
	}
}

// EntityDefinition stores information about a DOSA entity
type EntityDefinition struct {
	Name    string // normalized entity name
	Key     *PrimaryKey
	Columns []*ColumnDefinition
	Indexes map[string]*IndexDefinition
	ETL     ETLState
}

// Clone returns a deep copy of EntityDefinition
func (e *EntityDefinition) Clone() *EntityDefinition {
	newEd := &EntityDefinition{
		Name: e.Name,
		Key:  e.Key.Clone(),
		ETL:  e.ETL,
	}

	if e.Columns != nil {
		newEd.Columns = make([]*ColumnDefinition, len(e.Columns))
		for i, col := range e.Columns {
			newEd.Columns[i] = col.Clone()
		}
	}

	if e.Indexes != nil {
		newEd.Indexes = make(map[string]*IndexDefinition)
		if e.Indexes == nil {
			newEd.Indexes = nil
		}
		for k, index := range e.Indexes {
			newEd.Indexes[k] = index.Clone()
		}
	}

	return newEd
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
		return err
	}

	// validate index
	for indexName, index := range e.Indexes {
		if err := IsValidName(indexName); err != nil {
			return errors.Wrap(err, "IndexDefinition has invalid name")
		}

		if index == nil {
			return errors.New("IndexDefinition is nil")
		}

		if index.Key == nil {
			return errors.New("IndexDefinition has nil key")
		}

		if len(index.Key.PartitionKeys) == 0 {
			return errors.New("index does not have partition key")
		}

		keyNamesSeen := map[string]struct{}{}
		for _, p := range index.Key.PartitionKeys {
			if _, ok := columnNamesSeen[p]; !ok {
				return errors.Errorf("index partition key does not refer to a column: %q", p)
			}
			if _, ok := keyNamesSeen[p]; ok {
				return errors.Errorf("a column cannot be used twice in index key: %q", p)
			}
			keyNamesSeen[p] = struct{}{}
		}

		for _, c := range index.Key.ClusteringKeys {
			if c == nil {
				return errors.New("IndexDefinition has invalid nil clustering key")
			}

			if _, ok := columnNamesSeen[c.Name]; !ok {
				return errors.Errorf("clustering key does not refer to a column: %q", c.Name)
			}

			if _, ok := keyNamesSeen[c.Name]; ok {
				return errors.Errorf("a column cannot be used twice in index key: %q", c.Name)
			}
			keyNamesSeen[c.Name] = struct{}{}
		}

		columnsTagFieldsSeen := map[string]struct{}{}
		for _, c := range index.Columns {
			if _, ok := columnNamesSeen[c]; !ok {
				return errors.Errorf("columns tag field does not refer to a column: %q", c)
			}
			if _, ok := columnsTagFieldsSeen[c]; ok {
				return errors.Errorf("a column cannot be used twice in column tags: %q", c)
			}
			columnsTagFieldsSeen[c] = struct{}{}
		}
	}

	return nil
}

func (e *EntityDefinition) ensureNonNullablePrimaryKeys() error {
	columns := e.ColumnMap()

	for k := range e.PartitionKeySet() {
		if isInvalidPrimaryKeyType(columns[k]) {
			return errors.Errorf("primary key is of nullable type: %q", k)
		}
	}

	for k := range e.Key.ClusteringKeySet() {
		if isInvalidPrimaryKeyType(columns[k]) {
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

// ColumnMap returns a map of column name to column definition for all columns.
func (e *EntityDefinition) ColumnMap() map[string]*ColumnDefinition {
	m := make(map[string]*ColumnDefinition)
	for _, c := range e.Columns {
		m[c.Name] = c
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

// KeySet returns a set of all keys, including partition keys and clustering keys.
func (e *EntityDefinition) KeySet() map[string]struct{} {
	m := e.Key.ClusteringKeySet()
	pks := e.PartitionKeySet()
	for p := range pks {
		m[p] = struct{}{}
	}
	return m
}

// CanBeUpsertedOn checks upsertability: Can I be upserted on top of the prior definition?
func (e *EntityDefinition) CanBeUpsertedOn(older *EntityDefinition) error {
	// Better name
	newer := e

	// entity name should be the same
	if newer.Name != older.Name {
		return errors.Errorf("entity name mismatch: (%s vs %s)", newer.Name, older.Name)
	}

	// primary key should be exactly same
	pksNewer := newer.Key.PartitionKeys
	pksOlder := older.Key.PartitionKeys

	b := reflect.DeepEqual(pksNewer, pksOlder)
	if !b {
		return errors.Errorf("partition key mismatch: (%v vs %v)", pksNewer, pksOlder)
	}

	cksNewer := newer.Key.ClusteringKeys
	cksOlder := older.Key.ClusteringKeys
	if len(cksOlder) != 0 || len(cksNewer) != 0 {
		if !reflect.DeepEqual(cksNewer, cksOlder) {
			return errors.Errorf("clustering key mismatch: (%v vs %v)", cksNewer, cksOlder)
		}
	}
	// only allow to add new columns
	colsMapNewer := newer.ColumnTypes()
	colsMapOlder := older.ColumnTypes()

	for name, colTypeOlder := range colsMapOlder {
		colTypeNewer, ok := colsMapNewer[name]
		if !ok {
			return errors.Errorf("the column %s in old entity %s but not in new entity", name, older.Name)
		}
		if colTypeNewer != colTypeOlder {
			return errors.Errorf("the type for column %s mismatch: (%v vs %v)", name, colTypeNewer, colTypeOlder)
		}
	}

	// Index can only be added, not mutated
	if len(older.Indexes) > len(newer.Indexes) {
		return errors.Errorf("Old entity %s has %d indexes but new entity has %d indexes", older.Name, len(older.Indexes), len(newer.Indexes))
	}

	if older.Indexes != nil {
		for name, indexOlder := range older.Indexes {
			indexNewer, ok := newer.Indexes[name]
			if !ok {
				return errors.Errorf("Index %s in the old entity %s are missing in the new entity", name, older.Name)
			}

			if !reflect.DeepEqual(indexNewer, indexOlder) {
				return errors.Errorf("index %q mismatch: (%v vs %v)", name, indexNewer, indexOlder)
			}
		}
	}
	// TODO Handle tags in the future

	// ETL tag cannot be disabled
	if newer.ETL != EtlOn && older.ETL == EtlOn {
		return errors.Errorf("ETL tag cannot be disabled once it's on")
	}

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

// UniqueKey adds any missing keys from the entity's primary key to the keys
// specified in the index, to guarantee that the returned key is unique
// This method is used to create materialized views
func (e *EntityDefinition) UniqueKey(oldKey *PrimaryKey) *PrimaryKey {
	indexHas := oldKey.PrimaryKeySet()
	result := *oldKey

	// look for missing primary keys
	for _, key := range e.Key.PartitionKeys {
		if _, ok := indexHas[key]; !ok {
			result.ClusteringKeys = append(result.ClusteringKeys, &ClusteringKey{
				Name: key})
		}
	}

	// look for missing clustering keys
	for _, key := range e.Key.ClusteringKeys {
		if _, ok := indexHas[key.Name]; !ok {
			result.ClusteringKeys = append(result.ClusteringKeys, &ClusteringKey{
				Name: key.Name})
		}
	}

	return &result
}
