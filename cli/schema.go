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
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/devnull"
	"github.com/uber-go/dosa/schema/avro"
	"github.com/uber-go/dosa/schema/cql"
	"github.com/uber-go/dosa/schema/uql"
)

var (
	schemaDumpOutputTypes = map[string]bool{
		"cql":  true,
		"uql":  true,
		"avro": true,
	}
)

// SchemaOptions contains configuration for schema command flags.
type SchemaOptions struct {
	Excludes []string `short:"e" long:"exclude" description:"Exclude files matching pattern."`
	Verbose  bool     `short:"v" long:"verbose"`
}

// SchemaCmd is a placeholder for all schema commands
type SchemaCmd struct {
	*SchemaOptions
	Scope      string `short:"s" long:"scope" description:"Storage scope for the given operation."`
	NamePrefix string `long:"prefix" description:"Name prefix for schema types." required:"true"`
}

func (c *SchemaCmd) doSchemaOp(name string, f func(dosa.AdminClient, context.Context, string) (int32, error), args []string) error {
	if c.Verbose {
		fmt.Printf("executing %s with %v\n", name, args)
		fmt.Printf("options are %+v\n", *c)
		fmt.Printf("global options are %+v\n", options)
	}
	client, err := getAdminClient(options)
	if err != nil {
		return err
	}
	if len(args) != 0 {
		dirs, err := expandDirectories(args)
		if err != nil {
			return errors.Wrap(err, "could not expand directories")
		}
		client.Directories(dirs)
	}
	if len(c.Excludes) != 0 {
		client.Excludes(c.Excludes)
	}
	if c.Scope != "" {
		client.Scope(c.Scope)
	}

	ctx, cancel := context.WithTimeout(context.Background(), options.Timeout.Duration())
	defer cancel()

	if _, err := f(client, ctx, c.NamePrefix); err != nil {
		return err
	}

	if c.Verbose {
		fmt.Printf("%s successful\n", name)
	}
	return nil
}

// TODO make doSchema and doSchemaStatusOp better code
func (c *SchemaCmd) doSchemaStatusOp(name string, f func(dosa.AdminClient, context.Context, string) (*dosa.SchemaStatus, error), args []string) error {
	if c.Verbose {
		fmt.Printf("executing %s with %v\n", name, args)
		fmt.Printf("options are %+v\n", *c)
		fmt.Printf("global options are %+v\n", options)
	}
	client, err := getAdminClient(options)
	if err != nil {
		return err
	}
	if len(args) != 0 {
		dirs, err := expandDirectories(args)
		if err != nil {
			return errors.Wrap(err, "could not expand directories")
		}
		client.Directories(dirs)
	}
	if len(c.Excludes) != 0 {
		client.Excludes(c.Excludes)
	}
	if c.Scope != "" {
		client.Scope(c.Scope)
	}

	ctx, cancel := context.WithTimeout(context.Background(), options.Timeout.Duration())
	defer cancel()

	status, err := f(client, ctx, c.NamePrefix)
	if err != nil {
		return err
	}

	if c.Verbose {
		fmt.Printf("%s successful with status: %s\n", name, status.Status)
	}
	return nil
}

// SchemaCheck holds the options for 'schema check'
type SchemaCheck struct {
	*SchemaCmd
}

// Execute executes a schema check command
func (c *SchemaCheck) Execute(args []string) error {
	return c.doSchemaOp("schema check", dosa.AdminClient.CheckSchema, args)
}

// SchemaUpsert contains data for executing schema upsert command.
type SchemaUpsert struct {
	*SchemaCmd
}

// Execute executes a schema upsert command
func (c *SchemaUpsert) Execute(args []string) error {
	return c.doSchemaStatusOp("schema upsert", dosa.AdminClient.UpsertSchema, args)
}

// SchemaDump contains data for executing the schema dump command
type SchemaDump struct {
	*SchemaOptions
	Format string `long:"format" short:"f" description:"output format" choice:"cql" choice:"uql" choice:"avro" default:"cql"`
}

// Execute executes a schema dump command
func (c *SchemaDump) Execute(args []string) error {
	if c.Verbose {
		fmt.Printf("executing schema dump with %v\n", args)
		fmt.Printf("options are %+v\n", *c)
		fmt.Printf("global options are %+v\n", options)
	}

	// no connection necessary
	client := dosa.NewAdminClient(&devnull.Connector{})
	if len(args) != 0 {
		dirs, err := expandDirectories(args)
		if err != nil {
			return errors.Wrap(err, "could not expand directories")
		}
		client.Directories(dirs)
	}
	if len(c.Excludes) != 0 {
		client.Excludes(c.Excludes)
	}

	// try to parse entities in each directory
	defs, err := client.GetSchema()
	if err != nil {
		return err
	}

	// for each of those entities, format it in the specified way
	for _, d := range defs {
		switch c.Format {
		case "cql":
			fmt.Println(cql.ToCQL(d))
		case "uql":
			fmt.Println(uql.ToUQL(d))
		case "avro":
			fmt.Println(avro.ToAvro("TODO", d))
		}
	}

	return nil
}

// expandDirectory verifies that each argument is actually a directory or
// uses the special go suffix of /... to mean recursively walk from here
// example: ./... means the current directory and all subdirectories
func expandDirectories(dirs []string) ([]string, error) {
	const recursiveMarker = "/..."
	resultSet := make([]string, 0)
	for _, dir := range dirs {
		if strings.HasSuffix(dir, recursiveMarker) {
			err := filepath.Walk(strings.TrimSuffix(dir, recursiveMarker), func(path string, info os.FileInfo, err error) error {
				if info.IsDir() {
					resultSet = append(resultSet, path)
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
		} else {
			info, err := os.Stat(dir)
			if err != nil {
				return nil, err
			}
			if !info.IsDir() {
				return nil, fmt.Errorf("%q is not a directory", dir)
			}
			resultSet = append(resultSet, dir)
		}
	}
	if len(resultSet) == 0 {
		// treat an empty list as a search in the current directory (think "ls")
		return []string{"."}, nil
	}

	return resultSet, nil
}
