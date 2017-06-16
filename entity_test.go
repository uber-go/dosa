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

package dosa_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
)

func TestEntityDefinitionEnsureValid(t *testing.T) {
	type testData struct {
		e     *dosa.EntityDefinition
		valid bool
		msg   string
	}

	invalidName := getValidEntityDefinition()
	invalidName.Name = "foo=bar"

	nilColumn := getValidEntityDefinition()
	nilColumn.Columns = append(nilColumn.Columns, nil)

	invalidColumnName := getValidEntityDefinition()
	invalidColumnName.Columns[0].Name = "AAA"

	dupColumnNames := getValidEntityDefinition()
	dupColumnNames.Columns = append(dupColumnNames.Columns, &dosa.ColumnDefinition{Name: "bar", Type: dosa.Int64})

	invalidColumnType := getValidEntityDefinition()
	invalidColumnType.Columns[0].Type = dosa.Invalid

	nilPK := getValidEntityDefinition()
	nilPK.Key = nil

	noPartitionKey := getValidEntityDefinition()
	noPartitionKey.Key.PartitionKeys = []string{}

	invalidPartitionKeyName := getValidEntityDefinition()
	invalidPartitionKeyName.Key.PartitionKeys[0] = "fox"

	dupParitionKeyNames := getValidEntityDefinition()
	dupParitionKeyNames.Key.PartitionKeys = append(dupParitionKeyNames.Key.PartitionKeys, "foo")

	nilClusteringKey := getValidEntityDefinition()
	nilClusteringKey.Key.ClusteringKeys = append(nilClusteringKey.Key.ClusteringKeys, nil)

	invalidClusteringKeyName := getValidEntityDefinition()
	invalidClusteringKeyName.Key.ClusteringKeys[0].Name = "fox"

	dupClusteringKeyName := getValidEntityDefinition()
	dupClusteringKeyName.Key.ClusteringKeys[0].Name = "foo"

	noClusteringKey := getValidEntityDefinition()
	noClusteringKey.Key.ClusteringKeys = []*dosa.ClusteringKey{}

	data := []testData{
		{
			e:     nil,
			valid: false,
			msg:   "EntityDefinition is nil",
		},
		{
			e:     invalidName,
			valid: false,
			msg:   "name must contain only",
		},
		{
			e:     nilColumn,
			valid: false,
			msg:   "has nil column",
		},
		{
			e:     invalidColumnName,
			valid: false,
			msg:   "has invalid column name",
		},
		{
			e:     dupColumnNames,
			valid: false,
			msg:   "duplicated column found",
		},
		{
			e:     invalidColumnType,
			valid: false,
			msg:   "invalid type for column",
		},
		{
			e:     invalidColumnType,
			valid: false,
			msg:   "\"foo\"",
		},
		{
			e:     nilPK,
			valid: false,
			msg:   "nil primary key",
		},
		{
			e:     noPartitionKey,
			valid: false,
			msg:   "does not have partition key",
		},
		{
			e:     invalidPartitionKeyName,
			valid: false,
			msg:   "partition key does not refer to a column",
		},
		{
			e:     invalidPartitionKeyName,
			valid: false,
			msg:   "\"fox\"",
		},
		{
			e:     dupParitionKeyNames,
			valid: false,
			msg:   "a column cannot be used twice in key",
		},
		{
			e:     dupParitionKeyNames,
			valid: false,
			msg:   "\"foo\"",
		},
		{
			e:     invalidClusteringKeyName,
			valid: false,
			msg:   "\"fox\"",
		},
		{
			e:     invalidClusteringKeyName,
			valid: false,
			msg:   "does not refer to",
		},
		{
			e:     dupClusteringKeyName,
			valid: false,
			msg:   "a column cannot be used twice in key",
		},
		{
			e:     getValidEntityDefinition(),
			valid: true,
			msg:   "should be a valid EntityDefinition",
		},
		{
			e:     noClusteringKey,
			valid: true,
			msg:   "no clustering key is ok",
		},
		{
			e:     nilClusteringKey,
			valid: false,
			msg:   "nil clustering key",
		},
	}

	for _, entry := range data {
		err := entry.e.EnsureValid()
		if entry.valid {
			assert.NoError(t, err, entry.msg)
		} else {
			assert.Error(t, err, entry.msg)
			assert.Contains(t, err.Error(), entry.msg)
		}
	}
}

func TestEntityDefinitionEnsureValidForIndex(t *testing.T) {
	type testData struct {
		e     *dosa.EntityDefinition
		valid bool
		msg   string
	}

	invalidName := getValidEntityDefinition()
	invalidName.Indexes["index3=1123"] = invalidName.Indexes["index1"]

	nilPK := getValidEntityDefinition()
	nilPK.Indexes["index1"].Key = nil

	noPartitionKey := getValidEntityDefinition()
	noPartitionKey.Indexes["index1"].Key.PartitionKeys = []string{}

	invalidPartitionKeyName := getValidEntityDefinition()
	invalidPartitionKeyName.Indexes["index1"].Key.PartitionKeys[0] = "fox"

	dupParitionKeyNames := getValidEntityDefinition()
	dupParitionKeyNames.Indexes["index1"].Key.PartitionKeys = append(dupParitionKeyNames.Key.PartitionKeys, "foo")

	nilClusteringKey := getValidEntityDefinition()
	nilClusteringKey.Indexes["index1"].Key.ClusteringKeys = append(nilClusteringKey.Key.ClusteringKeys, nil)

	invalidClusteringKeyName := getValidEntityDefinition()
	invalidClusteringKeyName.Indexes["index1"].Key.ClusteringKeys[0].Name = "fox"

	dupClusteringKeyName := getValidEntityDefinition()
	dupClusteringKeyName.Indexes["index1"].Key.ClusteringKeys[0].Name = "qux"

	noClusteringKey := getValidEntityDefinition()
	noClusteringKey.Indexes["index1"].Key.ClusteringKeys = []*dosa.ClusteringKey{}

	data := []testData{
		{
			e:     invalidName,
			valid: false,
			msg:   "name must contain only",
		},
		{
			e:     nilPK,
			valid: false,
			msg:   "nil key",
		},
		{
			e:     noPartitionKey,
			valid: false,
			msg:   "does not have partition key",
		},
		{
			e:     invalidPartitionKeyName,
			valid: false,
			msg:   "partition key does not refer to a column",
		},
		{
			e:     invalidPartitionKeyName,
			valid: false,
			msg:   "\"fox\"",
		},
		{
			e:     dupParitionKeyNames,
			valid: false,
			msg:   "a column cannot be used twice in index key",
		},
		{
			e:     dupParitionKeyNames,
			valid: false,
			msg:   "\"foo\"",
		},
		{
			e:     invalidClusteringKeyName,
			valid: false,
			msg:   "\"fox\"",
		},
		{
			e:     invalidClusteringKeyName,
			valid: false,
			msg:   "does not refer to",
		},
		{
			e:     dupClusteringKeyName,
			valid: false,
			msg:   "a column cannot be used twice in index key",
		},
		{
			e:     noClusteringKey,
			valid: true,
			msg:   "no clustering key is ok",
		},
		{
			e:     nilClusteringKey,
			valid: false,
			msg:   "nil clustering key",
		},
	}

	for _, entry := range data {
		err := entry.e.EnsureValid()
		if entry.valid {
			assert.NoError(t, err, entry.msg)
		} else {
			assert.Error(t, err, entry.msg)
			assert.Contains(t, err.Error(), entry.msg)
		}
	}
}

func TestEntityDefinitionHelpers(t *testing.T) {
	ed := getValidEntityDefinition()

	expectedColumnTypes := map[string]dosa.Type{
		"foo": dosa.TUUID,
		"bar": dosa.Int64,
		"qux": dosa.Blob,
	}
	assert.Equal(t, expectedColumnTypes, ed.ColumnTypes())

	expectedPartitionKeySet := map[string]struct{}{"foo": {}}
	assert.Equal(t, expectedPartitionKeySet, ed.PartitionKeySet())

	expectedClusteringKeySet := map[string]struct{}{"bar": {}}
	assert.Equal(t, expectedClusteringKeySet, ed.ClusteringKeySet())

	expectedKeySet := map[string]struct{}{"foo": {}, "bar": {}}
	assert.Equal(t, expectedKeySet, ed.KeySet())
}

func getValidEntityDefinition() *dosa.EntityDefinition {
	return &dosa.EntityDefinition{
		Name: "testentity",
		Key: &dosa.PrimaryKey{
			PartitionKeys: []string{"foo"},
			ClusteringKeys: []*dosa.ClusteringKey{
				{
					Name:       "bar",
					Descending: true,
				},
			},
		},
		Indexes: map[string]*dosa.IndexDefinition{

			"index1": {
				Key: &dosa.PrimaryKey{
					PartitionKeys: []string{"qux"},
					ClusteringKeys: []*dosa.ClusteringKey{
						{
							Name:       "bar",
							Descending: true,
						},
					},
				},
			},

			"index2": {
				Key: &dosa.PrimaryKey{
					PartitionKeys: []string{"bar"},
				},
			},
		},
		Columns: []*dosa.ColumnDefinition{
			{
				Name: "foo",
				Type: dosa.TUUID,
			},
			{
				Name: "bar",
				Type: dosa.Int64,
			},
			{
				Name: "qux",
				Type: dosa.Blob,
			},
		},
	}
}

func TestEntityDefinitionIsCompatible(t *testing.T) {
	validEd := getValidEntityDefinition()
	// entity name not match
	errEd := getValidEntityDefinition()
	errEd.Name = errEd.Name + "error"
	err := validEd.IsCompatible(errEd)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "entity name")

	// partition key's size doesn't match
	// less
	errEd = getValidEntityDefinition()
	errEd.Key.PartitionKeys = []string{}
	err = validEd.IsCompatible(errEd)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "partition")

	// more
	errEd = getValidEntityDefinition()
	errEd.Key.PartitionKeys = append(errEd.Key.PartitionKeys, "bar")
	err = validEd.IsCompatible(errEd)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "partition")

	// not same partition key
	errEd = getValidEntityDefinition()
	errEd.Key.PartitionKeys = []string{"bar"}
	err = validEd.IsCompatible(errEd)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "partition")

	// clustering key's size doesn't match
	// less
	errEd = getValidEntityDefinition()
	errEd.Key.ClusteringKeys = nil
	err = validEd.IsCompatible(errEd)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "clustering")

	// more
	errEd = getValidEntityDefinition()
	errEd.Key.ClusteringKeys = append(errEd.Key.ClusteringKeys, &dosa.ClusteringKey{Name: "qux", Descending: false})
	err = validEd.IsCompatible(errEd)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "clustering")

	// empty clustering key
	errEd = getValidEntityDefinition()
	errEd.Key.ClusteringKeys = nil

	errEd1 := getValidEntityDefinition()
	errEd1.Key.ClusteringKeys = make([]*dosa.ClusteringKey, 0)
	err = errEd.IsCompatible(errEd1)
	assert.NoError(t, err)

	// not same clustering key
	// name not match
	errEd = getValidEntityDefinition()
	errEd.Key.ClusteringKeys[0].Name = "qux"
	err = validEd.IsCompatible(errEd)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "clustering")

	// descending not match
	errEd = getValidEntityDefinition()
	errEd.Key.ClusteringKeys[0].Descending = !errEd.Key.ClusteringKeys[0].Descending
	err = validEd.IsCompatible(errEd)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "clustering")

	// column size is less
	errEd = getValidEntityDefinition()
	errEd.Columns = append(errEd.Columns, &dosa.ColumnDefinition{Name: "abc", Type: dosa.Bool})
	err = validEd.IsCompatible(errEd)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "column")

	// column not match
	// name not match
	errEd = getValidEntityDefinition()
	errEd.Columns[0].Name = errEd.Columns[0].Name + "error"
	err = validEd.IsCompatible(errEd)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "column")

	// type not match
	errEd = getValidEntityDefinition()
	errEd.Columns[0].Type = dosa.Invalid
	err = validEd.IsCompatible(errEd)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "type")

	// same entity
	// name not match
	aEd := getValidEntityDefinition()
	err = validEd.IsCompatible(aEd)
	assert.NoError(t, err)
	// reverse
	err = aEd.IsCompatible(validEd)
	assert.NoError(t, err)

	// add new column
	aEd = getValidEntityDefinition()
	aEd.Columns = append(aEd.Columns, &dosa.ColumnDefinition{Name: "col", Type: dosa.Bool})
	err = aEd.IsCompatible(validEd)
	assert.NoError(t, err)

	// reverse
	err = validEd.IsCompatible(aEd)
	assert.Error(t, err)
}

func TestEntityDefinition_FindColumnDefinition(t *testing.T) {
	ed := getValidEntityDefinition()

	// for each known column, make sure we find the column definition with the same name
	for _, name := range []string{"foo", "bar", "qux"} {
		assert.Equal(t, name, ed.FindColumnDefinition(name).Name)
	}

	assert.Nil(t, ed.FindColumnDefinition("notacolumn"))

}
