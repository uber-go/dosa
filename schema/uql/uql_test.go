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

package uql_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/schema/uql"
)

func TestToUql(t *testing.T) {
	allColumnTypes := []*dosa.ColumnDefinition{
		{
			Name: "foo",
			Type: dosa.Int32,
		},
		{
			Name: "bar",
			Type: dosa.TUUID,
		},
		{
			Name: "qux",
			Type: dosa.Blob,
		},
		{
			Name: "fox",
			Type: dosa.String,
		},
		{
			Name: "dog",
			Type: dosa.Int64,
		},
		{
			Name: "cat",
			Type: dosa.Timestamp,
		},
		{
			Name: "tap",
			Type: dosa.Double,
		},
		{
			Name: "pop",
			Type: dosa.Bool,
		},
	}

	singleKeyEntity := &dosa.EntityDefinition{
		Name: "singlekey",
		Key: &dosa.PrimaryKey{
			PartitionKeys: []string{"foo"},
		},
		Columns: allColumnTypes,
	}

	compoundKeyEntity := &dosa.EntityDefinition{
		Name: "compoundkey",
		Key: &dosa.PrimaryKey{
			PartitionKeys: []string{"foo"},
			ClusteringKeys: []*dosa.ClusteringKey{
				{"qux", true},
			},
		},
		Columns: allColumnTypes,
	}

	twoParitionKeyEntity := &dosa.EntityDefinition{
		Name: "twopartitionkey",
		Key: &dosa.PrimaryKey{
			PartitionKeys: []string{"foo", "bar"},
		},
		Columns: allColumnTypes,
	}

	compositeKeyEntity := &dosa.EntityDefinition{
		Name: "compositekey",
		Key: &dosa.PrimaryKey{
			PartitionKeys: []string{"foo", "bar"},
			ClusteringKeys: []*dosa.ClusteringKey{
				{"qux", true},
				{"fox", false},
			},
		},
		Columns: allColumnTypes,
	}

	invalidEntity := &dosa.EntityDefinition{
		Name:    "twopartitionkey",
		Key:     nil,
		Columns: allColumnTypes,
	}

	expectedTmpl := `CREATE TABLE %s (
	  foo int32;
	  bar uuid;
	  qux blob;
	  fox string;
	  dog int64;
	  cat timestamp;
	  tap double;
	  pop bool;
	) PRIMARY KEY %s;
	`

	dataProvider := []struct {
		e         *dosa.EntityDefinition
		expected  string
		shouldErr bool
	}{
		{
			e:        singleKeyEntity,
			expected: fmt.Sprintf(expectedTmpl, singleKeyEntity.Name, "(foo)"),
		},
		{
			e:        compoundKeyEntity,
			expected: fmt.Sprintf(expectedTmpl, compoundKeyEntity.Name, "(foo, qux DESC)"),
		},
		{
			e:        twoParitionKeyEntity,
			expected: fmt.Sprintf(expectedTmpl, twoParitionKeyEntity.Name, "((foo, bar))"),
		},
		{
			e:        compositeKeyEntity,
			expected: fmt.Sprintf(expectedTmpl, compositeKeyEntity.Name, "((foo, bar), qux DESC, fox ASC)"),
		},
		{
			e:         nil,
			expected:  "",
			shouldErr: true,
		},
		{
			e:         invalidEntity,
			expected:  "",
			shouldErr: true,
		},
	}

	for _, testdata := range dataProvider {
		actual, err := uql.ToUQL(testdata.e)
		if testdata.shouldErr {
			assert.Error(t, err)
			continue
		}

		caseName := testdata.e.Name
		// compare line by line ignore leading and trailing whitespaces
		actualLines := strings.Split(actual, "\n")
		expectedLines := strings.Split(testdata.expected, "\n")
		assert.Equal(t, len(expectedLines), len(actualLines), caseName, "number of lines does not match expected")
		for i, actualLine := range actualLines {
			assert.Equal(t, strings.TrimSpace(expectedLines[i]), strings.TrimSpace(actualLine), caseName,
				fmt.Sprintf("output line %d does not match expected", i))
		}
	}
}
