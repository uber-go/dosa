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

package uql

import (
	"bytes"

	"text/template"

	"strings"

	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
)

// map from dosa type to uql type string
var uqlTypes = map[dosa.Type]string{
	dosa.String:    "string",
	dosa.Blob:      "blob",
	dosa.Bool:      "bool",
	dosa.Double:    "double",
	dosa.Int32:     "int32",
	dosa.Int64:     "int64",
	dosa.Timestamp: "timestamp",
	dosa.TUUID:     "uuid",
}

const separator = ", "

func formatPartitionKeys(s []string) string {
	return strings.Join(s, separator)
}

func formatWithClusteringKeys(clusteringKeys []*dosa.ClusteringKey) string {
	cks := make([]string, len(clusteringKeys))
	for i, clusteringKey := range clusteringKeys {
		if clusteringKey.Descending {
			cks[i] = clusteringKey.Name + " DESC"
		} else {
			cks[i] = clusteringKey.Name + " ASC"
		}
	}
	return strings.Join(cks, separator)
}

var funcMap = template.FuncMap{
	"toUqlType": func(t dosa.Type) string {
		return uqlTypes[t]
	},
	"formatKey": func(k dosa.PrimaryKey) string {
		partitionKeys := formatPartitionKeys(k.PartitionKeys)
		clusteringKeys := formatWithClusteringKeys(k.ClusteringKeys)
		switch {
		case len(k.PartitionKeys) == 1 && len(k.ClusteringKeys) == 0:
			return "(" + partitionKeys + ")"
		case len(k.PartitionKeys) == 1:
			return "(" + partitionKeys + separator + clusteringKeys + ")"
		case len(k.ClusteringKeys) == 0:
			return "((" + partitionKeys + "))"
		default:
			return "((" + partitionKeys + ")" + separator + clusteringKeys + ")"
		}
	},
}

const createStmt = "CREATE TABLE {{.Name}} (\n" +
	"{{range .Columns}}  {{.Name}} {{(toUqlType .Type)}};\n{{end}}" +
	") PRIMARY KEY {{(formatKey .Key)}};\n"

var tmpl = template.Must(template.New("uql").Funcs(funcMap).Parse(createStmt))

// ToUQL translates an entity defintion to UQL string of create table stmt.
func ToUQL(e *dosa.EntityDefinition) (string, error) {
	if err := e.EnsureValid(); err != nil {
		return "", errors.Wrap(err, "EntityDefinition is invalid")
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, e); err != nil {
		// shouldn't happen unless we have a bug in our code
		return "", errors.Wrap(err, "failed to execute UQL template; this is most likely a DOSA bug")
	}
	return buf.String(), nil
}
