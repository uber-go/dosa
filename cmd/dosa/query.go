// Copyright (c) 2020 Uber Technologies, Inc.
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
	"strings"

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
	Fields string `short:"f" long:"fields" description:"Fields of results to return, separated by comma."`
}

// QueryCmd is a placeholder for all query commands
type QueryCmd struct {
	*QueryOptions
	Scope         scopeFlag `short:"s" long:"scope" description:"Storage scope for the given operation." required:"true"`
	NamePrefix    string    `short:"n" long:"namePrefix" description:"Name prefix for schema types."`
	Prefix        string    `short:"p" long:"prefix" description:"Name prefix for schema types." hidden:"true"`
	Path          string    `long:"path" description:"Path to source." required:"true"`
	JarPath       string    `short:"j" long:"jarpath" description:"Path of the jar. This jar contains schema entities."`
	ClassNames    []string  `short:"c" long:"classnames" description:"Classes contain schema."`
	provideClient queryClientProvider
}

func (c *QueryCmd) doQueryOp(f func(ShellQueryClient, context.Context, []*queryObj, []string, int) ([]map[string]dosa.FieldValue, error), entityName string, queries []string, limit int) error {
	if c.JarPath != "" {
		fmt.Println("This operation has not been implemented in Java yet.")
	}

	if options.ServiceName == "" {
		options.ServiceName = _defServiceName
	}

	prefix, err := getNamePrefix(c.NamePrefix, c.Prefix)
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

	re, err := client.GetRegistrar().Find(&dosa.Entity{})
	// this error should never happen for CLI query cases
	if err != nil {
		return err
	}

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

	return printResults(results)
}

// QueryRead holds the options for 'query read'
type QueryRead struct {
	*QueryCmd
	Args struct {
		EntityName string   `positional-arg-name:"entity" description:"Entity name."`
		Queries    []string `positional-arg-name:"queries" description:"Queries should be in the form field:operator:value, supported operator: eq."`
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
	Limit int `short:"l" long:"limit" default:"100" description:"Max number of results to return."`
	Args  struct {
		EntityName string   `positional-arg-name:"entity" description:"Entity name."`
		Queries    []string `positional-arg-name:"queries" description:"Queries should be in the form field:operator:value, supported operators: eq,lt,le,gt,ge."`
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
