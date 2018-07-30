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
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
)

func createEntityDefinition() *dosa.EntityDefinition {
	return &dosa.EntityDefinition{
		Name: "test",
		Columns: []*dosa.ColumnDefinition{
			{
				Name: "stringcol",
				Type: dosa.String,
			},
			{
				Name: "uuidcol",
				Type: dosa.TUUID,
			},
			{
				Name: "int32col",
				Type: dosa.Int32,
			},
			{
				Name: "longcol",
				Type: dosa.Int64,
			},
			{
				Name: "doublecol",
				Type: dosa.Double,
			},
			{
				Name: "blobcol",
				Type: dosa.Blob,
			},
			{
				Name: "boolcol",
				Type: dosa.Bool,
			},
			{
				Name: "timestampcol",
				Type: dosa.Timestamp,
			},
		},
		Indexes: map[string]*dosa.IndexDefinition{
			"index1": {
				Key: &dosa.PrimaryKey{
					PartitionKeys: []string{"timestampcol"},
				},
			},
		},
		Key: &dosa.PrimaryKey{
			PartitionKeys: []string{
				"stringcol",
				"uuidcol",
			},
			ClusteringKeys: []*dosa.ClusteringKey{
				{
					Name:       "doublecol",
					Descending: false,
				},
				{
					Name:       "boolcol",
					Descending: true,
				},
			},
		},
	}
}

func TestToAvroSchema(t *testing.T) {
	ed := createEntityDefinition()
	namePrefix := "xxx.tt.yy"
	av, err := ToAvro(namePrefix, ed)
	assert.NoError(t, err)
	ed1, err := FromAvro(string(av))
	assert.NoError(t, err)
	assert.Equal(t, ed, ed1)
}

func TestDecodeFailure(t *testing.T) {
	data := []struct {
		Schema string
		Err    error
	}{
		{
			Schema: `{
				"ClusteringKeys":[
					{"Name":"doublecol","Descending":false},
					{"Name":"boolcol","Descending":true}
				],
				"fields":[
					{"dosaType":"String","name":"stringcol","type":"string"}
				],
				"name":"test",
				"PartitionKeys":{},
				"type":"record"
			}`,
			Err: errors.New("failed to parse partition keys"),
		},
		{
			Schema: `{
				"ClusteringKeys":[
					{"Name":"doublecol","Descending":false},
					{"Name":"boolcol","Descending":true}
				],
				"fields":[
					{"dosaType":"String","name":"stringcol","type":"string"}]
		 		,
				"name":"test",
				"PartitionKeys":[{},"uuidcol"],
				"type":"record"
			}`,
			Err: errors.New("failed to parse partition keys"),
		},
		{
			Schema: `{
				"ClusteringKeys":{},
				"fields":[
					{"dosaType":"String","name":"stringcol","type":"string"}]
		 		,
				"name":"test",
				"PartitionKeys":["stringcol","uuidcol"],
				"type":"record"
			}`,
			Err: errors.New("clustering keys"),
		},
		{
			Schema: `{
				"ClusteringKeys":[
					{"A":"doublecol","Descending":false}
				],
				"fields":[
					{"dosaType":"String","name":"stringcol","type":"string"}]
		 		,
				"name":"test",
				"PartitionKeys":["stringcol","uuidcol"],
				"type":"record"
			}`,
			Err: errors.New("cannot find Name key in map"),
		},
		{
			Schema: `{
				"ClusteringKeys":[
					{"Name":"doublecol","A":false}
				],
				"fields":[
					{"dosaType":"String","name":"stringcol","type":"string"}]
		 		,
				"name":"test",
				"PartitionKeys":["stringcol","uuidcol"],
				"type":"record"
			}`,
			Err: errors.New("cannot find Descending key in map"),
		},
		{
			Schema: `{
				"k":[
					{"Name":"doublecol","Descending":false}
				],
				"fields":[
					{"dosaType":"String","name":"stringcol","type":"string"}]
		 		,
				"name":"test",
				"PartitionKeys":["stringcol","uuidcol"],
				"type":"record"
			}`,
			Err: errors.New("cannot find ClusteringKeys key"),
		},
		{
			Schema: `{
				"ClusteringKeys":[
					{"Name":"doublecol","Descending":false}
				],
				"fields":[
					{"dosaType":"String","name":"stringcol","type":"string"}]
		 		,
				"name":"test",
				"p":["stringcol","uuidcol"],
				"type":"record"
			}`,
			Err: errors.New("cannot find PartitionKeys key"),
		},
		{
			Schema: `{
				"ClusteringKeys":[
					{"Name":"doublecol","Descending":false},
					{"Name":"boolcol","Descending":true}
				],
				"fields":[
					{"d":"String","name":"stringcol","type":"string"}
		 		],
				"name":"test",
				"PartitionKeys":["stringcol","uuidcol"],
				"type":"record"
			}`,
			Err: errors.New("cannot find dosaType key"),
		},
		{
			Schema: `{
				"ClusteringKeys":[
					{"Name":"doublecol","Descending":false},
					{"Name":"boolcol","Descending":true}
				],
				"fields":[
					{"d":{},"name":"stringcol","type":"string"}
		 		],
				"name":"test",
				"PartitionKeys":["stringcol","uuidcol"],
				"type":"record"
			}`,
			Err: errors.New("cannot find dosaType key"),
		},
		{
			Schema: `{
				"ClusteringKeys":[
					{"Name":"doublecol","Descending":false},
					{"Name":"boolcol","Descending":true}
				],
				"fields":[
					{"d":1,"name":"stringcol","type":"string"}
		 		],
				"name":"test",
				"PartitionKeys":["stringcol","uuidcol"],
				"type":"record"
			}`,
			Err: errors.New("cannot find dosaType key"),
		},
	}
	for _, d := range data {
		_, err := FromAvro(d.Schema)
		assert.Contains(t, err.Error(), d.Err.Error())
	}
}
