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

	"reflect"

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
			PrimaryKey: "pk1,,",
			Error:      nil,
			Result: &PrimaryKey{
				PartitionKeys:  []string{"pk1"},
				ClusteringKeys: nil,
			},
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
			PrimaryKey: "(pk1, pk2,),  , , ,",
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

func TestNameTag(t *testing.T) {
	defaultName := "default"
	data := []struct {
		Tag      string
		Error    error
		FullName string
		Name     string
	}{
		{
			Tag:      "name=ji",
			Error:    nil,
			Name:     "ji",
			FullName: "name=ji",
		},
		{
			Tag:      "name=ji,",
			Error:    nil,
			Name:     "ji",
			FullName: "name=ji,",
		},
		{
			Tag:      "name=ji,,,,",
			Error:    nil,
			Name:     "ji",
			FullName: "name=ji,,,,",
		},
		{
			Tag:      "name=ji12830",
			Error:    nil,
			Name:     "ji12830",
			FullName: "name=ji12830",
		},
		{
			Tag:      "name=ji12830 primaryKey=",
			Error:    nil,
			Name:     "ji12830",
			FullName: "name=ji12830",
		},
		{
			Tag:      "xxx name=ji12830 yyy",
			Error:    nil,
			Name:     "ji12830",
			FullName: "name=ji12830",
		},
		{
			Tag:      "name=ji^&*",
			Error:    errors.New("failed to normalize to a valid name for ji^&*"),
			Name:     "",
			FullName: "",
		},
	}

	for _, d := range data {
		fullName, name, err := parseNameTag(d.Tag, defaultName)
		if d.Error == nil {
			assert.Equal(t, name, d.Name)
			assert.Equal(t, fullName, d.FullName)
			assert.Nil(t, err)
		} else {
			assert.Contains(t, err.Error(), d.Error.Error())
		}
	}
}

func TestFieldParse(t *testing.T) {
	validFieldType := reflect.StructField{Name: "valid", Type: uuidType}
	invalidFieldType := reflect.StructField{Name: "invalid", Type: reflect.TypeOf([]string{})}

	data := []struct {
		StructField reflect.StructField
		Tag         string
		Error       error
		Column      *ColumnDefinition
	}{
		{
			StructField: invalidFieldType,
			Tag:         "",
			Error:       errors.New("Invalid type []string"),
		},
		{
			StructField: validFieldType,
			Tag:         "name=jj",
			Column: &ColumnDefinition{
				Name: "jj",
				Type: TUUID,
			},
		},
		{
			StructField: validFieldType,
			Tag:         "    name=jj    ",
			Column: &ColumnDefinition{
				Name: "jj",
				Type: TUUID,
			},
		},
		{
			StructField: validFieldType,
			Tag:         "    name=jj  sddf  ",
			Error:       errors.New("field valid with an invalid dosa field tag"),
		},
		{
			StructField: validFieldType,
			Tag:         "asdf    name=jj    ",
			Error:       errors.New("field valid with an invalid dosa field tag"),
		},
		{
			StructField: validFieldType,
			Tag:         "asdf    name=jj    asdfads",
			Error:       errors.New("field valid with an invalid dosa field tag"),
		},
		{
			StructField: validFieldType,
			Tag:         "  asdfljk  ",
			Error:       errors.New("field valid with an invalid dosa field tag"),
		},
		{
			StructField: validFieldType,
			Tag:         "  name=  ",
			Error:       errors.New("invalid name tag:   name="),
		},
		{
			StructField: validFieldType,
			Tag:         "name=",
			Error:       errors.New("invalid name tag: name="),
		},
		{
			StructField: validFieldType,
			Tag:         "name=x name=0",
			Error:       errors.New("field valid with an invalid dosa field tag"),
		},
	}
	for _, d := range data {
		cn, err := parseFieldTag(d.StructField, d.Tag)
		if d.Error != nil {
			assert.Contains(t, err.Error(), d.Error.Error())
		} else {
			assert.Equal(t, cn, d.Column)
			assert.Nil(t, err)
		}
	}

}

func TestEntityParse(t *testing.T) {
	structName := "testStruct"
	data := []struct {
		Tag        string
		TableName  string
		PrimaryKey *PrimaryKey
		Error      error
	}{
		{
			Tag:       "name=jj primaryKey=ok",
			TableName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			Error: nil,
		},
		{
			Tag:       "name=jj, primaryKey=ok",
			TableName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			Error: nil,
		},
		{
			Tag:       "name=jj, primaryKey=(ok)",
			TableName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			Error: nil,
		},
		{
			Tag:       "primaryKey=ok, name=jj",
			TableName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			Error: nil,
		},
		{
			Tag:       "primaryKey=(ok), name=jj",
			TableName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			Error: nil,
		},
		{
			Tag:       "primaryKey=(ok), , ,, name=jj",
			TableName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			Error: nil,
		},
		{
			Tag:       "primaryKey=(ok) name=jj",
			TableName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			Error: nil,
		},
		{
			Tag:       "primaryKey=((ok)) name=jj",
			TableName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			Error: nil,
		},
		{
			Tag:       "primaryKey=((ok, dd), a,b DESC,  c ASC) name=jj",
			TableName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys: []string{"ok", "dd"},
				ClusteringKeys: []*ClusteringKey{
					{
						Name:       "a",
						Descending: false,
					},
					{
						Name:       "b",
						Descending: true,
					},
					{
						Name:       "c",
						Descending: false,
					},
				},
			},
			Error: nil,
		},
		{
			Tag:       "name=jj, primaryKey=((ok, dd), a,b DESC,  c ASC) ",
			TableName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys: []string{"ok", "dd"},
				ClusteringKeys: []*ClusteringKey{
					{
						Name:       "a",
						Descending: false,
					},
					{
						Name:       "b",
						Descending: true,
					},
					{
						Name:       "c",
						Descending: false,
					},
				},
			},
			Error: nil,
		},
		{
			Tag:        "primaryKey=ok,adsf, name=jj",
			TableName:  "jj",
			PrimaryKey: nil,
			Error:      errors.New("failed to parse primary key ok,adsf, for DOSA object: invalid primary key: ok,adsf"),
		},
		{
			Tag:        "primaryK=adsf, name=jj",
			TableName:  "jj",
			PrimaryKey: nil,
			Error:      errors.New("dosa.Entity on object testStruct with an invalid dosa struct tag"),
		},
		{
			Tag:        "primaryKey=adsf, name=jj**",
			TableName:  "jj",
			PrimaryKey: nil,
			Error:      errors.New("invalid name tag:  name=jj**"),
		},
		{
			Tag:        "primaryKey=(ok) name=jj nxxx",
			TableName:  "jj",
			PrimaryKey: nil,
			Error:      errors.New("struct testStruct with an invalid dosa struct tag:   nxxx"),
		},
	}

	for _, d := range data {
		tableName, primaryKey, err := parseEntityTag(structName, d.Tag)
		if d.Error != nil {
			assert.Contains(t, err.Error(), d.Error.Error())
		} else {
			assert.Nil(t, err)
			assert.Equal(t, tableName, d.TableName)
			assert.Equal(t, primaryKey, d.PrimaryKey)
		}
	}
}
