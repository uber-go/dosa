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

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestPrimaryKey(t *testing.T) {
	data := []struct {
		PrimaryKey string
		Error      error
		Result     *PrimaryKey
	}{
		{
			PrimaryKey: "pk1",
			Error:      nil,
			Result: &PrimaryKey{
				PartitionKeys:  []string{"pk1"},
				ClusteringKeys: nil,
			},
		},
		{
			PrimaryKey: "pk1,",
			Error:      errors.New("invalid primary key: pk1,"),
			Result:     nil,
		},
		{
			PrimaryKey: "pk1, pk2",
			Error:      errors.New("invalid primary key: pk1, pk2"),
			Result:     nil,
		},
		{
			PrimaryKey: "pk1 desc",
			Error:      errors.New("invalid primary key: pk1 desc"),
			Result:     nil,
		},
		{
			PrimaryKey: "(pk1, pk2,)",
			Error:      nil,
			Result: &PrimaryKey{
				PartitionKeys: []string{"pk1"},
				ClusteringKeys: []*ClusteringKey{
					{
						Name:       "pk2",
						Descending: false,
					},
				},
			},
		},
		{
			PrimaryKey: "(pk1        , pk2              )",
			Error:      nil,
			Result: &PrimaryKey{
				PartitionKeys: []string{"pk1"},
				ClusteringKeys: []*ClusteringKey{
					{
						Name:       "pk2",
						Descending: false,
					},
				},
			},
		},
		{
			PrimaryKey: "(pk1, , pk2,)",
			Error:      nil,
			Result: &PrimaryKey{
				PartitionKeys: []string{"pk1"},
				ClusteringKeys: []*ClusteringKey{
					{
						Name:       "pk2",
						Descending: false,
					},
				},
			},
		},
		{
			PrimaryKey: "(pk1, pk2, io-$%^*)",
			Error:      errors.New("invalid primary key: (pk1, pk2, io-$%^*)"),
			Result:     nil,
		},
		{
			PrimaryKey: "(pk1, pk2, pk3)",
			Error:      nil,
			Result: &PrimaryKey{
				PartitionKeys: []string{"pk1"},
				ClusteringKeys: []*ClusteringKey{
					{
						Name:       "pk2",
						Descending: false,
					},
					{
						Name:       "pk3",
						Descending: false,
					},
				},
			},
		},
		{
			PrimaryKey: "((pk1), pk2)",
			Error:      nil,
			Result: &PrimaryKey{
				PartitionKeys: []string{"pk1"},
				ClusteringKeys: []*ClusteringKey{
					{
						Name:       "pk2",
						Descending: false,
					},
				},
			},
		},
		{
			PrimaryKey: "((pk1), pk2, pk3)",
			Error:      nil,
			Result: &PrimaryKey{
				PartitionKeys: []string{"pk1"},
				ClusteringKeys: []*ClusteringKey{
					{
						Name:       "pk2",
						Descending: false,
					},
					{
						Name:       "pk3",
						Descending: false,
					},
				},
			},
		},
		{
			PrimaryKey: "((pk1, pk2), pk3)",
			Error:      nil,
			Result: &PrimaryKey{
				PartitionKeys: []string{"pk1", "pk2"},
				ClusteringKeys: []*ClusteringKey{
					{
						Name:       "pk3",
						Descending: false,
					},
				},
			},
		},
		{
			PrimaryKey: "((pk1, pk2), pk3, pk4)",
			Error:      nil,
			Result: &PrimaryKey{
				PartitionKeys: []string{"pk1", "pk2"},
				ClusteringKeys: []*ClusteringKey{
					{
						Name:       "pk3",
						Descending: false,
					},
					{
						Name:       "pk4",
						Descending: false,
					},
				},
			},
		}, {
			PrimaryKey: "((pk1, pk2), pk3 asc, pk4 zxdlk)",
			Error:      errors.New("invalid primary key: ((pk1, pk2), pk3 asc, pk4 zxdlk)"),
			Result:     nil,
		},
		{
			PrimaryKey: "((pk1, pk2), pk3 asc, pk4 desc, pk5 ASC, pk6 DESC, pk7)",
			Error:      nil,
			Result: &PrimaryKey{
				PartitionKeys: []string{"pk1", "pk2"},
				ClusteringKeys: []*ClusteringKey{
					{
						Name:       "pk3",
						Descending: false,
					},
					{
						Name:       "pk4",
						Descending: true,
					},
					{
						Name:       "pk5",
						Descending: false,
					},
					{
						Name:       "pk6",
						Descending: true,
					},
					{
						Name:       "pk7",
						Descending: false,
					},
				},
			},
		},
	}

	for _, d := range data {
		k, err := parsePrimaryKey("t", d.PrimaryKey)
		if nil == d.Error {
			assert.Nil(t, err)
			assert.Equal(t, k.PartitionKeys, d.Result.PartitionKeys)
			assert.Equal(t, k.ClusteringKeys, d.Result.ClusteringKeys)
		} else {
			assert.Contains(t, err.Error(), d.Error.Error())
		}
	}
}
