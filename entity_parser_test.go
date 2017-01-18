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
	"testing"

	"github.com/stretchr/testify/assert"
)

type SinglePrimaryKeyNoParen struct {
	Entity     `dosa:"primaryKey=PrimaryKey"`
	PrimaryKey int64
	Data       string
}

// happy path: A single primaryKey becomes the partition key
func TestSinglePrimaryKeyNoParen(t *testing.T) {
	dosaTable, err := TableFromInstance(&SinglePrimaryKeyNoParen{})
	assert.Nil(t, err)
	assert.Equal(t, []string{"PrimaryKey"}, dosaTable.Key.PartitionKeys)
	assert.Equal(t, 0, len(dosaTable.Key.ClusteringKeys))
}

type SinglePrimaryKey struct {
	Entity     `dosa:"primaryKey=(PrimaryKey)"`
	PrimaryKey int64
	Data       string
}

// happy path: A single primaryKey becomes the partition key
func TestSinglePrimaryKey(t *testing.T) {
	dosaTable, err := TableFromInstance(&SinglePrimaryKey{})
	assert.Nil(t, err)
	assert.Equal(t, []string{"PrimaryKey"}, dosaTable.Key.PartitionKeys)
	assert.Equal(t, 0, len(dosaTable.Key.ClusteringKeys))
}

func BenchmarkSingleKey(b *testing.B) {
	for i := 0; i < b.N; i++ {
		TableFromInstance(&SinglePrimaryKey{})
	}
}

type SinglePartitionKey struct {
	Entity     `dosa:"primaryKey=PrimaryKey"`
	PrimaryKey int64
	data       string
}

func TestSinglePartitionKey(t *testing.T) {
	dosaTable, err := TableFromInstance(&SinglePartitionKey{})
	assert.Nil(t, err)
	assert.Equal(t, []string{"PrimaryKey"}, dosaTable.Key.PartitionKeys)
	assert.Equal(t, 0, len(dosaTable.Key.ClusteringKeys))
}

// unhappy path: this struct doesn't have anything specified for pk
type NoPrimaryKey struct {
	Entity     `dosa:"primaryKey="`
	PrimaryKey int64
	data       string
}

// unhappy path: If there is no field marked with a primary nor partition key, throw an error
func TestNoPrimaryKey(t *testing.T) {
	dosaTable, err := TableFromInstance(&NoPrimaryKey{})
	assert.Nil(t, dosaTable)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "\"NoPrimaryKey\"")
}

// unhappy path: this struct has an empty primary key
type EmptyPrimaryKey struct {
	Entity     `dosa:"primaryKey=()"`
	PrimaryKey int64
	data       string
}

// unhappy path: If there is no field marked with a primary nor partition key, throw an error
func TestEmptyPrimaryKey(t *testing.T) {
	dosaTable, err := TableFromInstance(&EmptyPrimaryKey{})
	assert.Nil(t, dosaTable)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "\"EmptyPrimaryKey\"")
}

type PrimaryKeyWithSecondaryRange struct {
	Entity     `dosa:"primaryKey=(PartKey,PrimaryKey  )"`
	PartKey    int64
	PrimaryKey int64
	data       string
}

func TestPrimaryKeyWithSecondaryRange(t *testing.T) {
	dosaTable, err := TableFromInstance(&PrimaryKeyWithSecondaryRange{})
	assert.Nil(t, err)
	assert.Equal(t, []string{"PartKey"}, dosaTable.Key.PartitionKeys)
	assert.Equal(t, []*ClusteringKey{{"PrimaryKey", false}}, dosaTable.Key.ClusteringKeys)
}

type PrimaryKeyWithDescendingRange struct {
	Entity     `dosa:"primaryKey=(PartKey,PrimaryKey desc )"`
	PartKey    int64
	PrimaryKey int64
	data       string
}

func TestPrimaryKeyWithDescendingRange(t *testing.T) {
	dosaTable, err := TableFromInstance(&PrimaryKeyWithDescendingRange{})
	assert.Nil(t, err)
	assert.Equal(t, []string{"PartKey"}, dosaTable.Key.PartitionKeys)
	assert.Equal(t, []*ClusteringKey{{"PrimaryKey", true}}, dosaTable.Key.ClusteringKeys)
}

type MultiComponentPrimaryKey struct {
	Entity         `dosa:"primaryKey=((PartKey, AnotherPartKey))"`
	PartKey        int64
	AnotherPartKey int64
	data           string
}

func TestMultiComponentPrimaryKey(t *testing.T) {
	dosaTable, err := TableFromInstance(&MultiComponentPrimaryKey{})
	assert.Nil(t, err)
	assert.Equal(t, []string{"PartKey", "AnotherPartKey"}, dosaTable.Key.PartitionKeys)
	assert.Nil(t, dosaTable.Key.ClusteringKeys)
}

type InvalidDosaAttribute struct {
	Entity `dosa:"oopsie,primaryKey=Oops"`
	Oops   int64
}

func TestInvalidDosaAttribute(t *testing.T) {
	dosaTable, err := TableFromInstance(&InvalidDosaAttribute{})
	assert.Nil(t, dosaTable)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "\"oopsie\"")
}

func TestStringify(t *testing.T) {
	dosaTable, _ := TableFromInstance(&SinglePrimaryKey{})
	assert.Contains(t, dosaTable.String(), dosaTable.Name)
	assert.Contains(t, dosaTable.String(), "primarykey")
}

type MissingCloseParen struct {
	Entity            `dosa:"primaryKey=(MissingCloseParen"`
	MissingCloseParen int64
}

func TestMissingCloseParen(t *testing.T) {
	dosaTable, err := TableFromInstance(&MissingCloseParen{})
	assert.Nil(t, dosaTable)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "MissingCloseParen")
}

type MissingAnnotation struct {
	Entity
	Oops int64
}

func TestMissingAnnotation(t *testing.T) {
	dosaTable, err := TableFromInstance(&MissingAnnotation{})
	assert.Nil(t, dosaTable)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "struct")
	assert.Contains(t, err.Error(), "tag")
}

type AllTypes struct {
	Entity      `dosa:"primaryKey=BoolType"`
	BoolType    bool
	Int32Type   int32
	Int64Type   int64
	Float64Type float64
}

func TestAllTypes(t *testing.T) {
	dosaTable, err := TableFromInstance(&AllTypes{})
	assert.NotNil(t, dosaTable)
	assert.Nil(t, err)
}

type UnsupportedType struct {
	Entity    `dosa:"primaryKey=BoolType"`
	BoolType  bool
	UnsupType float32
}

func TestUnsupportedType(t *testing.T) {
	dosaTable, err := TableFromInstance(&UnsupportedType{})
	assert.Nil(t, dosaTable)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "float32")
}
