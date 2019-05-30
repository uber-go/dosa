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

package cql

import (
	"bytes"
	"strings"
	"text/template"

	"github.com/uber-go/dosa"
)

func uniqueKey(e dosa.EntityDefinition, k *dosa.PrimaryKey) *dosa.PrimaryKey {
	return e.UniqueKey(k)
}

// typeMap returns the CQL type associated with the given dosa.Type,
// used in the template
func typeMap(t dosa.Type) string {
	switch t {
	case dosa.String:
		return "text"
	case dosa.Blob:
		return "blob"
	case dosa.Bool:
		return "boolean"
	case dosa.Double:
		return "double"
	case dosa.Int32:
		return "int"
	case dosa.Int64:
		return "bigint"
	case dosa.Timestamp:
		return "timestamp"
	case dosa.TUUID:
		return "uuid"
	}
	return "unknown"
}

func selectFieldsInCreatingView(columns []string) string {
	if len(columns) == 0 {
		return "*"
	}
	return `"` + strings.Join(columns, `", "`) + `"`
}

// precompile the template for create table
var cqlCreateTableTemplate = template.Must(template.
	New("cqlCreateTable").
	Funcs(map[string]interface{}{"typeMap": typeMap}).
	Funcs(map[string]interface{}{"uniqueKey": uniqueKey}).
	Funcs(map[string]interface{}{"selectFieldsInCreatingView": selectFieldsInCreatingView}).
	Parse(`create table "{{.Name}}" ({{range .Columns}}"{{- .Name -}}" {{ typeMap .Type -}}, {{end}}primary key {{ .Key }});
{{- range $name, $indexdef := .Indexes }}
create materialized view "{{- $name -}}" as
  select {{selectFieldsInCreatingView $indexdef.Columns}} from "{{- $.Name -}}"
  where{{range $keynum, $key := $indexdef.Key.PartitionKeys }}{{if $keynum}} AND {{end}} "{{ $key }}" is not null {{- end}}
  primary key {{ uniqueKey $ $indexdef.Key }};
{{- end -}}`))

// ToCQL generates CQL from an EntityDefinition
func ToCQL(e *dosa.EntityDefinition) string {
	var buf bytes.Buffer
	// errors are ignored here, they can only happen from an invalid template, which will get caught in tests
	err := cqlCreateTableTemplate.Execute(&buf, e)
	if err != nil {
		panic(err)
	}
	return buf.String()
}
