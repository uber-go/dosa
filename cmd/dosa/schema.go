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

type scopeFlag string

func (s *scopeFlag) setString(value string) {
	*s = scopeFlag(strings.Replace(value, ".", "_", -1))
}

// String implements the stringer interface
func (s *scopeFlag) String() string {
	return string(*s)
}

func (s *scopeFlag) UnmarshalFlag(value string) error {
	s.setString(value)
	return nil
}

// SchemaOptions contains configuration for schema command flags.
type SchemaOptions struct {
	Excludes []string `short:"e" long:"exclude" description:"Exclude files matching pattern."`
	Verbose  bool     `short:"v" long:"verbose"`
}

// SchemaCmd is a placeholder for all schema commands
type SchemaCmd struct {
	*SchemaOptions
	Scope         scopeFlag `short:"s" long:"scope" description:"Storage scope for the given operation." required:"true"`
	NamePrefix    string    `short:"n" long:"namePrefix" description:"Name prefix for schema types."`
	Prefix        string    `short:"p" long:"prefix" description:"Name prefix for schema types." hidden:"true"`
	provideClient adminClientProvider
}

func getNamePrefix(namePrefix, prefix string) (string, error) {
	if len(namePrefix) > 0 {
		return namePrefix, nil
	}
	if len(prefix) > 0 {
		return prefix, nil
	}
	return "", errors.New("required argument '--namePrefix' was not specified")
}

func (c *SchemaCmd) doSchemaOp(name string, f func(dosa.AdminClient, context.Context, string) (*dosa.SchemaStatus, error), args []string) error {
	if c.Verbose {
		fmt.Printf("executing %s with %v\n", name, args)
		fmt.Printf("options are %+v\n", *c)
		fmt.Printf("global options are %+v\n", options)
	}

	// TODO(eculver): use options/configurator pattern to apply defaults
	if options.ServiceName == "" {
		options.ServiceName = _defServiceName
	}

	client, err := c.provideClient(options)
	if err != nil {
		return err
	}
	defer shutdownAdminClient(client)

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
		client.Scope(c.Scope.String())
	}

	ctx, cancel := context.WithTimeout(context.Background(), options.Timeout.Duration())
	defer cancel()

	var prefix string
	if prefix, err = getNamePrefix(c.NamePrefix, c.Prefix); err != nil {
		return err
	}
	status, err := f(client, ctx, prefix)
	if err != nil {
		if c.Verbose {
			fmt.Printf("detail:%+v\n", err)
		}
		fmt.Println("Status: NOT OK")
		return err
	}
	fmt.Printf("Version: %d\n", status.Version)
	fmt.Printf("Status: %s\n", status.Status)
	return nil
}

// SchemaCheck holds the options for 'schema check'
type SchemaCheck struct {
	*SchemaCmd
	Args struct {
		Paths []string `positional-arg-name:"paths"`
	} `positional-args:"yes"`
}

func newSchemaCheck(provideClient adminClientProvider) *SchemaCheck {
	return &SchemaCheck{
		SchemaCmd: &SchemaCmd{
			provideClient: provideClient,
		},
	}
}

// Execute executes a schema check command
func (c *SchemaCheck) Execute(args []string) error {
	return c.doSchemaOp("schema check", dosa.AdminClient.CanUpsertSchema, c.Args.Paths)
}

// SchemaUpsert contains data for executing schema upsert command.
type SchemaUpsert struct {
	*SchemaCmd
	Args struct {
		Paths []string `positional-arg-name:"paths"`
	} `positional-args:"yes"`
}

func newSchemaUpsert(provideClient adminClientProvider) *SchemaUpsert {
	return &SchemaUpsert{
		SchemaCmd: &SchemaCmd{
			provideClient: provideClient,
		},
	}
}

// Execute executes a schema upsert command
func (c *SchemaUpsert) Execute(args []string) error {
	return c.doSchemaOp("schema upsert", dosa.AdminClient.UpsertSchema, c.Args.Paths)
}

// SchemaStatus contains data for executing schema status command
type SchemaStatus struct {
	*SchemaCmd
	Version int32 `long:"version" description:"Specify schema version."`
}

func newSchemaStatus(provideClient adminClientProvider) *SchemaStatus {
	return &SchemaStatus{
		SchemaCmd: &SchemaCmd{
			provideClient: provideClient,
		},
	}
}

// Execute executes a schema status command
func (c *SchemaStatus) Execute(args []string) error {
	if c.Verbose {
		fmt.Printf("executing schema status with %v\n", args)
		fmt.Printf("options are %+v\n", *c)
		fmt.Printf("global options are %+v\n", options)
	}

	// TODO(eculver): use options/configurator pattern to apply defaults
	if options.ServiceName == "" {
		options.ServiceName = _defServiceName
	}

	client, err := c.provideClient(options)
	if err != nil {
		return err
	}
	defer shutdownAdminClient(client)

	if c.Scope.String() != "" {
		client.Scope(c.Scope.String())
	}

	ctx, cancel := context.WithTimeout(context.Background(), options.Timeout.Duration())
	defer cancel()

	var prefix string
	if prefix, err = getNamePrefix(c.NamePrefix, c.Prefix); err != nil {
		return err
	}
	status, err := client.CheckSchemaStatus(ctx, prefix, c.Version)
	if err != nil {
		if c.Verbose {
			fmt.Printf("detail:%+v\n", err)
		}
		fmt.Println("Status: NOT OK")
		return err
	}
	fmt.Printf("Version: %d\n", status.Version)
	fmt.Printf("Status: %s\n", status.Status)
	return nil
}

// SchemaDump contains data for executing the schema dump command
type SchemaDump struct {
	*SchemaOptions
	Format string `long:"format" short:"f" description:"output format" choice:"cql" choice:"uql" choice:"avro" default:"cql"`
	Args   struct {
		Paths []string `positional-arg-name:"paths"`
	} `positional-args:"yes"`
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
	if len(c.Args.Paths) != 0 {
		dirs, err := expandDirectories(c.Args.Paths)
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
			s, err := avro.ToAvro("TODO", d)
			fmt.Println(string(s), err)
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
