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
	"bytes"
	"text/template"
)

// typemap returns the CQL type associated with the given dosa.Type,
// used in the template
func typemap(t Type) string {
	switch t {
	case String:
		return "text"
	case Blob:
		return "blob"
	case Bool:
		return "boolean"
	case Double:
		return "double"
	case Int32:
		return "int"
	case Int64:
		return "bigint"
	case Timestamp:
		return "timestamp"
	case TUUID:
		return "uuid"
	}
	return "unknown"
}

// precompile the template for create table
var cqlCreateTableTemplate = template.Must(template.
	New("cqlCreateTable").
	Funcs(map[string]interface{}{"typemap": typemap}).
	Parse(`create table "{{.Name}}" ({{range .Columns}}"{{- .Name -}}" {{ typemap .Type -}}, {{end}}primary key {{ .Key }})`))

// CqlCreateTable generates CQL from an EntityDefinition
func (e *EntityDefinition) CqlCreateTable() string {
	var buf bytes.Buffer
	// errors are ignored here, they can only happen from an invalid template, which will get caught in tests
	_ = cqlCreateTableTemplate.Execute(&buf, e)
	return buf.String()
}
