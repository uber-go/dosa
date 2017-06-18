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

package cassandra

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"text/template"

	"github.com/uber-go/dosa"
)

const (
	keyspace       = "Keyspace"
	table          = "Table"
	values         = "Values"
	columns        = "Columns"
	ifNotExist     = "IfNotExist"
	limit          = "Limit"
	conditions     = "Conditions"
	insertTemplate = `INSERT INTO {{.Keyspace}}.{{.Table}} ({{ColumnFunc .Columns ", "}}) VALUES ({{QuestionMark .Values ", "}}){{ExistsFunc .IfNotExist}};`
	selectTemplate = `SELECT {{ColumnFunc .Columns ", "}} FROM {{.Keyspace}}.{{.Table}}{{WhereFunc .Conditions}}{{ConditionsFunc .Conditions " AND "}}{{LimitFunc .Limit}};`
	deleteTemplate = `DELETE FROM {{.Keyspace}}.{{.Table}} WHERE {{ConditionsFunc .Conditions " AND "}};`
)

var (
	funcMap = template.FuncMap{
		"ColumnFunc":     strings.Join,
		"QuestionMark":   questionMarkFunc,
		"ExistsFunc":     existsFunc,
		"LimitFunc":      limitFunc,
		"ConditionsFunc": conditionsFunc,
		"WhereFunc":      whereFunc,
	}
	insertTmpl = template.Must(template.New("insert").Funcs(funcMap).Parse(insertTemplate))
	selectTmpl = template.Must(template.New("select").Funcs(funcMap).Parse(selectTemplate))
	deleteTmpl = template.Must(template.New("delete").Funcs(funcMap).Parse(deleteTemplate))
)

func questionMarkFunc(qs []interface{}, sep string) string {
	questions := make([]string, len(qs))
	for i := range qs {
		questions[i] = "?"
	}
	return strings.Join(questions, sep)
}

func existsFunc(exists bool) string {
	if exists {
		return " IF NOT EXISTS"
	}
	return ""
}

func limitFunc(num int) string {
	if num > 0 {
		return fmt.Sprintf(" LIMIT %d", num)
	}
	return ""
}

func conditionsFunc(conds []*ColumnCondition, sep string) string {
	cstrs := make([]string, len(conds))
	for i, cond := range conds {
		sign := "="
		switch cond.Condition.Op {
		case dosa.Lt:
			sign = "<"
		case dosa.LtOrEq:
			sign = "<="
		case dosa.Gt:
			sign = ">"
		case dosa.GtOrEq:
			sign = ">="
		default:
			sign = "="
		}
		cstrs[i] = fmt.Sprintf("%s%s?", strconv.Quote(cond.Name), sign)
	}
	return strings.Join(cstrs, sep)
}

func whereFunc(conds []*ColumnCondition) string {
	if len(conds) > 0 {
		return " WHERE "
	}
	return ""
}

// Option to compose a cql statement
type Option map[string]interface{}

// OptFunc is the interface to set option
type OptFunc func(Option)

// Keyspace sets the `keyspace` to the cql statement
func Keyspace(v string) OptFunc {
	return func(opt Option) {
		opt[keyspace] = strconv.Quote(v)
	}
}

// Table sets the `table` to the cql statement
func Table(v string) OptFunc {
	return func(opt Option) {
		opt[table] = strconv.Quote(v)
	}
}

// Columns sets the `columns` clause to the cql statement
func Columns(v []string) OptFunc {
	return func(opt Option) {
		quoCs := make([]string, len(v))
		for i, c := range v {
			quoCs[i] = strconv.Quote(c)
		}
		opt[columns] = quoCs
	}
}

// Values sets the `values` clause to the cql statement
func Values(v interface{}) OptFunc {
	return func(opt Option) {
		opt[values] = v
	}
}

// IfNotExist sets the `if not exist` clause to the cql statement
func IfNotExist(v interface{}) OptFunc {
	return func(opt Option) {
		opt[ifNotExist] = v
	}
}

// Limit sets the `limit` to the cql statement
func Limit(v interface{}) OptFunc {
	return func(opt Option) {
		opt[limit] = v
	}
}

// Conditions set the `where` clause to the cql statement
func Conditions(v interface{}) OptFunc {
	return func(opt Option) {
		opt[conditions] = v
	}
}

// InsertStmt creates insert statement
func InsertStmt(opts ...OptFunc) (string, error) {
	var bb bytes.Buffer
	option := Option{
		ifNotExist: false,
	}
	for _, opt := range opts {
		opt(option)
	}
	err := insertTmpl.Execute(&bb, option)
	return bb.String(), err
}

// SelectStmt creates select statement
func SelectStmt(opts ...OptFunc) (string, error) {
	var bb bytes.Buffer
	option := Option{
		limit: 0,
	}
	for _, opt := range opts {
		opt(option)
	}
	err := selectTmpl.Execute(&bb, option)
	return bb.String(), err
}

// DeleteStmt creates delete statement
func DeleteStmt(opts ...OptFunc) (string, error) {
	var bb bytes.Buffer
	option := Option{}
	for _, opt := range opts {
		opt(option)
	}
	err := deleteTmpl.Execute(&bb, option)
	return bb.String(), err
}
