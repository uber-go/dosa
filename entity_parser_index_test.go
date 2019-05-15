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

package dosa

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type SingleIndexNoParen struct {
	Entity       `dosa:"primaryKey=PrimaryKey"`
	SearchByData Index `dosa:"key=Data"`
	PrimaryKey   int64
	Data         string
}

func TestSingleIndexNoParen(t *testing.T) {
	dosaTable, err := TableFromInstance(&SingleIndexNoParen{})
	assert.Nil(t, err)
	assert.Equal(t, map[string]*IndexDefinition{
		"searchbydata": {
			Key:     &PrimaryKey{PartitionKeys: []string{"data"}},
			Columns: []string{},
		},
	}, dosaTable.Indexes)
}

type SingleIndexUnExported struct {
	Entity       `dosa:"primaryKey=PrimaryKey"`
	searchByData Index `dosa:"key=Data"`
	PrimaryKey   int64
	Data         string
}

func TestSingleIndexUnExported(t *testing.T) {
	dosaTable, err := TableFromInstance(&SingleIndexUnExported{})
	assert.Nil(t, err)
	assert.Equal(t, map[string]*IndexDefinition{}, dosaTable.Indexes)
}

type MultipleIndexes struct {
	Entity       `dosa:"primaryKey=PrimaryKey"`
	Index        `dosa:"key=Data, name=SearchByData"`
	SearchByDate Index `dosa:"key=Date"`
	PrimaryKey   int64
	Data         string
	Date         time.Time
}

func TestMultipleIndexes(t *testing.T) {
	dosaTable, err := TableFromInstance(&MultipleIndexes{})
	assert.Nil(t, err)
	assert.Equal(t, map[string]*IndexDefinition{
		"searchbydata": {
			Key:     &PrimaryKey{PartitionKeys: []string{"data"}},
			Columns: []string{},
		},
		"searchbydate": {
			Key:     &PrimaryKey{PartitionKeys: []string{"date"}},
			Columns: []string{},
		},
	}, dosaTable.Indexes)
}

type ComplexIndexes struct {
	Entity       `dosa:"primaryKey=PrimaryKey"`
	SearchByData Index `dosa:"key=(Data, Date, PrimaryKey DESC) name=index_data"`
	SearchByDate Index `dosa:"key=((Date, PrimaryKey), Data) name=index_date"`
	PrimaryKey   int64
	Data         string
	Date         time.Time
}

func TestComplexIndexes(t *testing.T) {
	dosaTable, err := TableFromInstance(&ComplexIndexes{})
	assert.Nil(t, err)
	assert.Equal(t, map[string]*IndexDefinition{
		"index_data": {
			Key: &PrimaryKey{
				PartitionKeys: []string{"data"},
				ClusteringKeys: []*ClusteringKey{
					{
						Name:       "date",
						Descending: false,
					},
					{
						Name:       "primarykey",
						Descending: true,
					},
				},
			},
			Columns: []string{},
		},
		"index_date": {
			Key: &PrimaryKey{
				PartitionKeys: []string{"date", "primarykey"},
				ClusteringKeys: []*ClusteringKey{
					{
						Name:       "data",
						Descending: false,
					},
				},
			},
			Columns: []string{},
		},
	}, dosaTable.Indexes)
}

type IndexesWithColumnsTag struct {
	Entity       `dosa:"primaryKey=(ID)"`
	SearchByCity Index `dosa:"key=(City, Payload) columns=(ID)"`
	SearchByID   Index `dosa:"key=(City) columns=(ID, Payload)"`

	ID      UUID
	City    string
	Payload []byte
}

func TestIndexesWithColumnsTag(t *testing.T) {
	dosaTable, err := TableFromInstance(&IndexesWithColumnsTag{})
	assert.Nil(t, err)
	assert.Equal(t, map[string]*IndexDefinition{
		"searchbycity": {
			Key: &PrimaryKey{
				PartitionKeys: []string{"city"},
				ClusteringKeys: []*ClusteringKey{
					{
						Name:       "payload",
						Descending: false,
					},
				},
			},
			Columns: []string{"id"},
		},
		"searchbyid": {
			Key: &PrimaryKey{
				PartitionKeys: []string{"city"},
			},
			Columns: []string{"id", "payload"},
		},
	}, dosaTable.Indexes)
}
