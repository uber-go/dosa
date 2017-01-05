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
	"github.com/stretchr/testify/assert"
	"testing"
)

type SinglePrimaryKeyNoParen struct {
	Entity     `dosa:"primaryKey=PrimaryKey"`
	PrimaryKey int
	Data       string
}

type SinglePrimaryKey struct {
	Entity     `dosa:"primaryKey=(PrimaryKey)"`
	PrimaryKey int
	Data       string
}

func TestNonPointer(t *testing.T) {
	dosaTable, err := TableFromInstance(SinglePrimaryKey{})
	assert.Nil(t, dosaTable)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "\"struct\"")
}

// happy path: A single primaryKey becomes the partition key
func TestSinglePrimaryKey(t *testing.T) {
	dosaTable, err := TableFromInstance(&SinglePrimaryKey{})
	assert.Nil(t, err)
	assert.Equal(t, []string{"PrimaryKey"}, dosaTable.Keys.partitionKeys)
	assert.Equal(t, 0, len(dosaTable.Keys.primaryKeys))
}

func BenchmarkSingleKey(b *testing.B) {
	for i := 0; i < b.N; i++ {
		TableFromInstance(&SinglePrimaryKey{})
	}
}

type SinglePartitionKey struct {
	Entity     `dosa:"primaryKey=PrimaryKey"`
	PrimaryKey int
	data       string
}

func TestSinglePartitionKey(t *testing.T) {
	dosaTable, err := TableFromInstance(&SinglePartitionKey{})
	assert.Nil(t, err)
	assert.Equal(t, []string{"PrimaryKey"}, dosaTable.Keys.partitionKeys)
	assert.Equal(t, 0, len(dosaTable.Keys.primaryKeys))
}

// unhappy path: this struct doesn't have any DOSA annotations
type NoPrimaryKey struct {
	Entity     `dosa:"primaryKey="`
	PrimaryKey int
	data       string
}

// unhappy path: If there is no field marked with a primary nor partition key, throw an error
func TestNoPrimaryKey(t *testing.T) {
	dosaTable, err := TableFromInstance(&NoPrimaryKey{})
	assert.Nil(t, dosaTable)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "\"NoPrimaryKey\"")
}

type PrimaryKeyWithSecondaryRange struct {
	Entity     `dosa:"primaryKey=(PartKey,PrimaryKey)"`
	PartKey    int
	PrimaryKey int
	data       string
}

func TestPrimaryKeyWithSecondaryRange(t *testing.T) {
	dosaTable, err := TableFromInstance(&PrimaryKeyWithSecondaryRange{})
	assert.Nil(t, err)
	assert.Equal(t, []string{"PrimaryKey"}, dosaTable.Keys.primaryKeys)
	assert.Equal(t, []string{"PartKey"}, dosaTable.Keys.partitionKeys)
}

type InvalidDosaAttribute struct {
	Entity `dosa:"oopsie,primaryKey=Oops"`
	Oops   int
}

func TestInvalidDosaAttribute(t *testing.T) {
	dosaTable, err := TableFromInstance(&InvalidDosaAttribute{})
	assert.Nil(t, dosaTable)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "\"oopsie\"")
}

func TestStringify(t *testing.T) {
	dosaTable, _ := TableFromInstance(&SinglePrimaryKey{})
	assert.Contains(t, dosaTable.String(), dosaTable.TableName)
	assert.Contains(t, dosaTable.String(), "PrimaryKey")
}
