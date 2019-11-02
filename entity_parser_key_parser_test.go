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
	"reflect"
	"testing"

	"time"

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
			PrimaryKey: "ABădNăm",
			Error:      nil,
			Result: &PrimaryKey{
				PartitionKeys:  []string{"ABădNăm"},
				ClusteringKeys: nil,
			},
		},
		{
			PrimaryKey: "pk1,,",
			Error:      errors.New("invalid partition key"),
			Result:     nil,
		},
		{
			PrimaryKey: "pk1, pk2",
			Error:      errors.New("invalid partition key"),
			Result:     nil,
		},
		{
			PrimaryKey: "pk1 desc",
			Error:      errors.New("invalid partition key"),
			Result:     nil,
		},
		{
			PrimaryKey: "(pk1, pk2,)",
			Error:      errors.New("invalid primary key"),
			Result:     nil,
		},
		{
			PrimaryKey: "(pk1, pk2,),  , , ,",
			Error:      errors.New("invalid primary key"),
			Result:     nil,
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
			Error:      errors.New("invalid primary key"),
			Result:     nil,
		},
		{
			PrimaryKey: "(pk1, pk2, io-$%^*)",
			Error:      nil,
			Result: &PrimaryKey{
				PartitionKeys: []string{"pk1"},
				ClusteringKeys: []*ClusteringKey{
					{
						Name:       "pk2",
						Descending: false,
					},
					{
						Name:       "io-$%^*",
						Descending: false,
					},
				},
			},
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
			Error:      errors.New("invalid clustering key order"),
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
		k, err := parsePrimaryKey(d.PrimaryKey)
		if nil == d.Error {
			assert.Nil(t, err)
			assert.Equal(t, d.Result.PartitionKeys, k.PartitionKeys)
			assert.Equal(t, d.Result.ClusteringKeys, k.ClusteringKeys)
		} else {
			assert.Contains(t, err.Error(), d.Error.Error())
		}
	}
}

func TestNameTag(t *testing.T) {
	defaultName := "default"
	data := []struct {
		TagMaps map[string]string
		Error   error
		Name    string
	}{
		{
			TagMaps: map[string]string{"name": "ji"},
			Error:   nil,
			Name:    "ji",
		},
		{
			TagMaps: map[string]string{"name": "ji,,,,"},
			Error:   errors.New("invalid"),
			Name:    "",
		},
		{
			TagMaps: map[string]string{"name": "ji12830"},
			Error:   nil,
			Name:    "ji12830",
		},
		{
			TagMaps: map[string]string{"name": "name=ji^&*"},
			Error:   errors.New("invalid"),
			Name:    "",
		},
		{
			TagMaps: map[string]string{},
			Error:   nil,
			Name:    defaultName,
		},
	}

	for _, d := range data {
		name, err := parseNameTag(defaultName, d.TagMaps)
		if d.Error == nil {
			assert.Equal(t, d.Name, name)
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
			Error:       errors.New("invalid dosa field tag"),
		},
		{
			StructField: validFieldType,
			Tag:         "asdf    name=jj    ",
			Error:       errors.New("invalid dosa field tag"),
		},
		{
			StructField: validFieldType,
			Tag:         "asdf    name=jj    asdfads",
			Error:       errors.New("invalid dosa field tag"),
		},
		{
			StructField: validFieldType,
			Tag:         "  asdfljk  ",
			Error:       errors.New("invalid dosa field tag"),
		},
		{
			StructField: validFieldType,
			Tag:         "  name=  ",
			Error:       errors.New("invalid dosa field tag"),
		},
		{
			StructField: validFieldType,
			Tag:         "name=",
			Error:       errors.New("invalid dosa field tag"),
		},
		{
			StructField: validFieldType,
			Tag:         "name=x, name=0",
			Error:       errors.New("duplicate tag"),
		},
	}
	for _, d := range data {
		cn, err := parseFieldTag(d.StructField, d.Tag)
		if d.Error != nil {
			assert.Contains(t, err.Error(), d.Error.Error())
		} else {
			assert.Equal(t, d.Column, cn)
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
		ETL        ETLState
		TTL        time.Duration
		Error      error
	}{
		{
			Tag:       "name=jj, primaryKey=ok",
			TableName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			ETL:   EtlOff,
			TTL:   NoTTL(),
			Error: nil,
		},
		{
			Tag:       "name=jj, primaryKey=(ok)",
			TableName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			ETL:   EtlOff,
			TTL:   NoTTL(),
			Error: nil,
		},
		{
			Tag:       "primaryKey=ok, name=jj",
			TableName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			ETL:   EtlOff,
			TTL:   NoTTL(),
			Error: nil,
		},
		{
			Tag:       "primaryKey=(ok), name=jj",
			TableName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			ETL:   EtlOff,
			TTL:   NoTTL(),
			Error: nil,
		},
		{
			Tag:       "primaryKey=(ok), , ,, name=jj",
			TableName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			ETL:   EtlOff,
			TTL:   NoTTL(),
			Error: errors.New("invalid dosa annotation"),
		},
		{
			Tag:       "primaryKey=(ok), name=jj",
			TableName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			ETL:   EtlOff,
			TTL:   NoTTL(),
			Error: nil,
		},
		{
			Tag:       "primaryKey=((ok)), name=jj",
			TableName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			ETL:   EtlOff,
			TTL:   NoTTL(),
			Error: nil,
		},
		{
			Tag:       "primaryKey=((ok, dd), a,b DESC,  c ASC), name=jj",
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
			ETL:   EtlOff,
			TTL:   NoTTL(),
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
			ETL:   EtlOff,
			TTL:   NoTTL(),
			Error: nil,
		},
		{
			Tag:       "name=jj, primaryKey=ok, etl=on",
			TableName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			Error: nil,
			ETL:   EtlOn,
			TTL:   NoTTL(),
		},
		{
			Tag:       "name=jj, primaryKey=ok, etl=ON, ttl=90s",
			TableName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			Error: nil,
			ETL:   EtlOn,
			TTL:   time.Second * 90,
		},
		{
			Tag:       "name=jj, primaryKey=ok, etl=On, ttl=80m",
			TableName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			Error: nil,
			ETL:   EtlOn,
			TTL:   time.Minute * 80,
		},
		{
			Tag:       "name=jj, primaryKey=ok, etl=On, ttl=-80m",
			TableName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			Error: errors.New("invalid ttl tag"),
			ETL:   EtlOn,
			TTL:   NoTTL(),
		},
		{
			Tag:       "name=jj, primaryKey=ok, etl=off",
			TableName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			Error: nil,
			ETL:   EtlOff,
			TTL:   NoTTL(),
		},
		{
			Tag:       "name=jj, primaryKey=ok, etl=OFF, ttl = 90h",
			TableName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			Error: nil,
			ETL:   EtlOff,
			TTL:   time.Hour * 90,
		},
		{
			Tag:       "name=jj, primaryKey=ok, etl=Off, ttl = 912ms",
			TableName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			Error: errors.New("invalid ttl tag"),
			ETL:   EtlOff,
			TTL:   time.Millisecond * 912,
		},
		{
			Tag:       "name=jj, primaryKey=ok, etl=Off, ttl=912d",
			TableName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			Error: errors.New("unknown unit d in duration"),
			ETL:   EtlOff,
			TTL:   NoTTL(),
		},
		{
			Tag:       "name=jj, primaryKey=ok, etl=Off, ttl",
			TableName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			Error: errors.New("invalid ttl tag"),
			ETL:   EtlOff,
			TTL:   NoTTL(),
		},
		{
			Tag:       "name=jj, primaryKey=ok, etl=Off, ttl=",
			TableName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			Error: errors.New("invalid ttl tag"),
			ETL:   EtlOff,
			TTL:   NoTTL(),
		},
		{
			Tag:       "name=jj, primaryKey=ok, etl=Off, ttl=1us",
			TableName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			Error: errors.New("invalid ttl tag"),
			ETL:   EtlOff,
			TTL:   NoTTL(),
		},
		{
			Tag:       "name=jj, primaryKey=ok, etl=",
			TableName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			Error: errors.New("invalid etl tag"),
			ETL:   EtlOff,
		},
		{
			Tag:       "name=jj, primaryKey=ok, etl",
			TableName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			Error: errors.New("invalid etl tag"),
			ETL:   EtlOff,
		},
		{
			Tag:        "primaryKey=ok,adsf, name=jj",
			TableName:  "jj",
			PrimaryKey: nil,
			Error:      errors.New("ok,adsf"),
		},
		{
			Tag:        "primaryK=adsf, name=jj",
			TableName:  "jj",
			PrimaryKey: nil,
			Error:      errors.New("dosa.Entity on object testStruct with an invalid annotation"),
		},
		{
			Tag:        "primaryKey=adsf, name=jj**",
			TableName:  "jj",
			PrimaryKey: nil,
			Error:      errors.New("invalid name tag"),
		},
		{
			Tag:        "primaryKey=(ok), name=jj, nxxx",
			TableName:  "jj",
			PrimaryKey: nil,
			Error:      errors.New("dosa.Entity on object testStruct with an invalid annotation"),
		},
	}

	for _, d := range data {
		tableName, ttl, etl, primaryKey, err := parseEntityTag(structName, d.Tag)
		if d.Error != nil {
			assert.Contains(t, err.Error(), d.Error.Error())
		} else {
			assert.Nil(t, err)
			assert.Equal(t, d.TableName, tableName)
			assert.Equal(t, d.PrimaryKey, primaryKey)
			assert.Equal(t, d.ETL, etl)
			assert.Equal(t, d.TTL, ttl)
		}
	}
}

func TestIndexParse(t *testing.T) {
	data := []struct {
		Tag               string
		InputIndexName    string
		ExpectedIndexName string
		PrimaryKey        *PrimaryKey
		Columns           []string
		Error             error
	}{
		{
			Tag:               "name=jj, key=ok",
			ExpectedIndexName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			InputIndexName: "SearchByKey",
			Error:          nil,
		},
		{
			Tag:               "name=jj, key=ok",
			ExpectedIndexName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			InputIndexName: "SearchByKey",
			Error:          nil,
		},
		{
			Tag:               "name=jj, key=(ok)",
			ExpectedIndexName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			InputIndexName: "SearchByKey",
			Error:          nil,
		},
		{
			Tag:               "key=ok, name=jj",
			ExpectedIndexName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			InputIndexName: "SearchByKey",
			Error:          nil,
		},
		{
			Tag:               "key=(ok), name=jj",
			ExpectedIndexName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			InputIndexName: "SearchByKey",
			Error:          nil,
		},
		{
			Tag:               "key=(ok), , ,, name=jj",
			ExpectedIndexName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			InputIndexName: "SearchByKey",
			Error:          errors.New("dosa.Index on object SearchByKey with an invalid annotation"),
		},
		{
			Tag:               "key=(ok), name=jj",
			ExpectedIndexName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			InputIndexName: "SearchByKey",
			Error:          nil,
		},
		{
			Tag:               "key=((ok)), name=jj",
			ExpectedIndexName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			InputIndexName: "SearchByKey",
			Error:          nil,
		},
		{
			Tag:               "key=((ok, dd), a,b DESC,  c ASC), name=jj",
			ExpectedIndexName: "jj",
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
			InputIndexName: "SearchByKey",
			Error:          nil,
		},
		{
			Tag:               "name=jj, key=((ok, dd), a,b DESC,  c ASC) ",
			ExpectedIndexName: "jj",
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
			InputIndexName: "SearchByKey",
			Error:          nil,
		},
		{
			Tag:               "key=ok,adsf, name=jj",
			ExpectedIndexName: "jj",
			PrimaryKey:        nil,
			InputIndexName:    "SearchByKey",
			Error:             errors.New("ok,adsf"),
		},
		{
			Tag:               "primaryK=adsf, name=jj",
			ExpectedIndexName: "jj",
			PrimaryKey:        nil,
			InputIndexName:    "SearchByKey",
			Error:             errors.New("dosa.Index on object SearchByKey with an invalid annotation"),
		},
		{
			Tag:               "key=adsf, name=jj**",
			ExpectedIndexName: "jj",
			PrimaryKey:        nil,
			InputIndexName:    "SearchByKey",
			Error:             errors.New("invalid name tag"),
		},
		{
			Tag:               "key=(ok), name=jj, nxxx",
			ExpectedIndexName: "jj",
			PrimaryKey:        nil,
			InputIndexName:    "SearchByKey",
			Error:             errors.New("invalid dosa annotation nxxx"),
		},
		{
			Tag:               "key=((ok)), name=jj",
			ExpectedIndexName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			InputIndexName: "SearchByKey",
			Error:          nil,
		},
		{
			Tag:               "key=((ok))",
			ExpectedIndexName: "",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			InputIndexName: "",
			Error:          errors.New("invalid name"),
		},
		{
			Tag:               "key=((ok))",
			ExpectedIndexName: "",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			InputIndexName: "searchByKey",
			Error:          errors.New("index name (searchByKey) must be exported, try (SearchByKey) instead"),
		},
		{
			Tag:               "name=jj, key=ok, columns=(ok)",
			ExpectedIndexName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			InputIndexName: "SearchByKey",
			Columns:        []string{"ok"},
			Error:          nil,
		},
		{
			Tag:               "name=jj, key=ok, columns=(ok, test, hi)",
			ExpectedIndexName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			InputIndexName: "SearchByKey",
			Columns:        []string{"ok", "test", "hi"},
			Error:          nil,
		},
		{
			Tag:               "name=jj, key=ok, columns=(ok, test, hi,)",
			ExpectedIndexName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			InputIndexName: "SearchByKey",
			Columns:        []string{"ok", "test", "hi"},
			Error:          errors.New("invalid columns tag"),
		},
		{
			Tag:               "name=jj, key=ok, columns=(ok, hi, hi)",
			ExpectedIndexName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			InputIndexName: "SearchByKey",
			Columns:        []string{"ok", "test", "hi"},
			Error:          errors.New("duplicate field hi in columns tag"),
		},
		{
			Tag:               "name=jj, key=ok, columns=(ok, test, (hi),)",
			ExpectedIndexName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			InputIndexName: "SearchByKey",
			Columns:        []string{"ok", "test", "hi"},
			Error:          errors.New("invalid columns tag"),
		},
		{
			Tag:               "name=jj, key=ok, columns=(ok, test, (hi))",
			ExpectedIndexName: "jj",
			PrimaryKey: &PrimaryKey{
				PartitionKeys:  []string{"ok"},
				ClusteringKeys: nil,
			},
			InputIndexName: "SearchByKey",
			Columns:        []string{"ok", "test", "hi"},
			Error:          errors.New("invalid columns tag"),
		},
	}

	for _, d := range data {
		name, primaryKey, columns, err := parseIndexTag(d.InputIndexName, d.Tag)
		if d.Error != nil {
			assert.Contains(t, err.Error(), d.Error.Error())
		} else {
			assert.Nil(t, err)
			assert.Equal(t, d.ExpectedIndexName, name)
			assert.Equal(t, d.PrimaryKey, primaryKey)
			assert.Equal(t, d.Columns, columns)
		}
	}
}
