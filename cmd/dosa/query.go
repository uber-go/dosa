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

package main

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
)

type queryObj struct {
	fieldName string
	colName   string
	op        string
	valueStr  string
	value     dosa.FieldValue
}

func newQueryObj(fieldName, op, valueStr string) *queryObj {
	return &queryObj{
		fieldName: fieldName,
		op:        op,
		valueStr:  valueStr,
	}
}

// QueryOptions contains configuration for query command flags
type QueryOptions struct {
	Fields string `short:"f" long:"fields" description:"fields of results to return, separated by comma."`
}

// QueryCmd is a placeholder for all query commands
type QueryCmd struct {
	*QueryOptions
	Scope         scopeFlag `short:"s" long:"scope" description:"Storage scope for the given operation." required:"true"`
	NamePrefix    string    `short:"n" long:"namePrefix" description:"Name prefix for schema types."`
	Prefix        string    `short:"p" long:"prefix" description:"Name prefix for schema types." hidden:"true"`
	Path          string    `long:"path" description:"path to source." required:"true"`
	provideClient queryClientProvider
}

func (c *QueryCmd) getNamePrefix() (string, error) {
	if len(c.NamePrefix) > 0 {
		return c.NamePrefix, nil
	}
	if len(c.Prefix) > 0 {
		return c.Prefix, nil
	}
	return "", errors.New("required argument '--namePrefix' was not specified")
}

func (c *QueryCmd) doQueryOp(f func(ShellQueryClient, context.Context, []*queryObj, []string, int) ([]map[string]dosa.FieldValue, error), entityName string, queries []string, limit int) error {
	if options.ServiceName == "" {
		options.ServiceName = _defServiceName
	}

	prefix, err := c.getNamePrefix()
	if err != nil {
		return err
	}

	client, err := c.provideClient(options, c.Scope.String(), prefix, c.Path, entityName)
	if err != nil {
		return err
	}
	defer shutdownQueryClient(client)

	kvs, err := parseQuery(queries)
	if err != nil {
		return err
	}

	re, _ := client.GetRegistrar().Find(&dosa.Entity{})

	fvs, err := setQueryFieldValues(kvs, re)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), options.Timeout.Duration())
	defer cancel()

	var fields []string
	if c.Fields != "" {
		fields = strings.Split(c.Fields, ",")
	}

	results, err := f(client, ctx, fvs, fields, limit)
	if err != nil {
		return err
	}

	printResult(results)

	return nil
}

// QueryRead holds the options for 'query read'
type QueryRead struct {
	*QueryCmd
	Args struct {
		EntityName string   `positional-arg-name:"entity"`
		Queries    []string `positional-arg-name:"queries" description:"queries should be in the form field:op:value, supported operator: eq"`
	} `positional-args:"yes"`
}

func newQueryRead(provideClient queryClientProvider) *QueryRead {
	return &QueryRead{
		QueryCmd: &QueryCmd{
			provideClient: provideClient,
		},
	}
}

// Execute executes a read query command
func (c *QueryRead) Execute(args []string) error {
	return c.doQueryOp(ShellQueryClient.Read, c.Args.EntityName, c.Args.Queries, 1)
}

// QueryRange holds the options for 'query range'
type QueryRange struct {
	*QueryCmd
	Limit int `short:"l" long:"limit" default:"100" description:"max number of results to return, all results will be returned if not specified"`
	Args  struct {
		EntityName string   `positional-arg-name:"entity" description:"entity name"`
		Queries    []string `positional-arg-name:"queries" description:"queries should be in the form field:op:value, supported operators: eq,lt,le,gt,ge"`
	} `positional-args:"yes"`
}

func newQueryRange(provideClient queryClientProvider) *QueryRange {
	return &QueryRange{
		QueryCmd: &QueryCmd{
			provideClient: provideClient,
		},
	}
}

// Execute executes a range query command
func (c *QueryRange) Execute(args []string) error {
	return c.doQueryOp(ShellQueryClient.Range, c.Args.EntityName, c.Args.Queries, c.Limit)
}

// parseQuery parses the input query expressions
func parseQuery(exps []string) ([]*queryObj, error) {
	queries := make([]*queryObj, len(exps))
	for idx, exp := range exps {
		strs := strings.SplitN(exp, ":", 3)
		if len(strs) != 3 {
			return nil, errors.Errorf("query expression should be in the form field:op:value")
		}
		queries[idx] = newQueryObj(strs[0], strs[1], strs[2])
	}
	return queries, nil
}

// setQueryFieldValues sets the value field of queryObj
func setQueryFieldValues(queries []*queryObj, re *dosa.RegisteredEntity) ([]*queryObj, error) {
	cts := re.EntityDefinition().ColumnTypes()
	for _, query := range queries {
		// convert field name to column name, ColumnNames method will return error if field name not found
		cols, err := re.ColumnNames([]string{query.fieldName})
		if err != nil {
			return nil, err
		}
		query.colName = cols[0]
		if typ, ok := cts[cols[0]]; ok {
			fv, err := strToFieldValue(typ, query.valueStr)
			if err != nil {
				return nil, err
			}
			query.value = fv
		} else {
			return nil, errors.Errorf("cannot find the type of column %s", cols[0])
		}
	}
	return queries, nil
}

// strToFieldValue converts the 'value' of input query expression from string
// to dosa.FieldValue
func strToFieldValue(t dosa.Type, s string) (dosa.FieldValue, error) {
	switch t {
	case dosa.Int32:
		i, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, err
		}
		return dosa.FieldValue(int32(i)), nil
	case dosa.Int64:
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, err
		}
		return dosa.FieldValue(int64(i)), nil
	case dosa.Bool:
		b, err := strconv.ParseBool(s)
		if err != nil {
			return nil, err
		}
		return dosa.FieldValue(b), nil
	case dosa.String:
		return dosa.FieldValue(s), nil
	case dosa.Double:
		d, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, err
		}
		return dosa.FieldValue(d), nil
	case dosa.Timestamp:
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return nil, err
		}
		return dosa.FieldValue(t), nil
	case dosa.TUUID:
		u := dosa.UUID(s)
		return dosa.FieldValue(u), nil
	case dosa.Blob:
		// TODO: support query with binary arrays
		return nil, errors.Errorf("blob query not supported for now")
	default:
		return nil, errors.Errorf("unsupported type")
	}
}

func printResult(res []map[string]dosa.FieldValue) {
	fmt.Println("Results:")
	for _, fvs := range res {
		for k, v := range fvs {
			fmt.Printf("%s: %v\n", k, reflect.Indirect(reflect.ValueOf(v)))
		}
		fmt.Printf("\n")
	}
}
