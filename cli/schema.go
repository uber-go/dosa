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
)

var (
	schemaDumpOutputTypes = map[string]bool{
		"cql":  true,
		"uql":  true,
		"avro": true,
	}
)

// SchemaCmd is a placeholder for all schema commands
type SchemaCmd struct{}

// SchemaOptions contains configuration for schema command flags.
type SchemaOptions struct {
	NamePrefix string   `long:"prefix" description:"Name prefix for schema types."`
	Excludes   []string `short:"e" long:"exclude" description:"Exclude files matching pattern."`
	Scope      string   `short:"s" long:"scope" description:"Storage scope for the given operation."`
	Pedantic   bool     `long:"pedantic"`
	Verbose    bool     `short:"v" long:"verbose"`
}

// SchemaCheck holds the options for 'schema check'
type SchemaCheck struct {
	*SchemaOptions
}

// Execute executes a schema check command
func (c *SchemaCheck) Execute(args []string) error {
	if c.Verbose {
		fmt.Printf("executing schema check with %v\n", args)
		fmt.Printf("options is %+v\n", *c.SchemaOptions)
		fmt.Printf("global options is %v\n", options)
	}
	client, err := getAdminClient(options)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), options.Timeout.Duration())
	defer cancel()

	if len(args) != 0 {
		dirs, err := expandDirectories(args)
		if err != nil {
			return errors.Wrap(err, "could not expand directories")
		}
		client.Directories(dirs)
	}
	// default the scope to the current username
	if len(c.Scope) == 0 {
		c.Scope = "scope_" + os.Getenv("USER")
	}
	client.Scope(c.Scope)
	client.Excludes(c.Excludes)

	if _, err := client.CheckSchema(ctx, c.NamePrefix); err != nil {
		return err
	}

	if c.Verbose {
		fmt.Printf("%s\n", "schema check successful")
	}
	return nil
}

// SchemaUpsert contains data for executing schema upsert command.
type SchemaUpsert struct {
	*SchemaOptions
}

// Execute executes a schema upsert command
func (c *SchemaUpsert) Execute(args []string) error {
	if c.Verbose {
		fmt.Printf("executing schema upsert with %v\n", args)
		fmt.Printf("options is %+v\n", *c.SchemaOptions)
		fmt.Printf("global options is %v\n", options)
	}
	client, err := getAdminClient(options)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), options.Timeout.Duration())
	defer cancel()

	if len(args) != 0 {
		dirs, err := expandDirectories(args)
		if err != nil {
			return errors.Wrap(err, "could not expand directories")
		}
		client.Directories(dirs)
	}
	if len(c.Scope) == 0 {
		c.Scope = "scope_" + os.Getenv("USER")
	}
	client.Scope(c.Scope)
	client.Excludes(c.Excludes)

	if _, err := client.UpsertSchema(ctx, c.NamePrefix); err != nil {
		return err
	}

	if c.Verbose {
		fmt.Printf("%s\n", "schema upsert successful")
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

// SchemaDump contains data for executing the schema dump command
type SchemaDump struct {
	Format string `long:"format" short:"f" description:"output format" choice:"cql" choice:"uql" choice:"avro" default:"cql"`
}

// Execute executes a schema dump command
func (c *SchemaDump) Execute(args []string) error {
	fmt.Printf("dump schema in format %s TODO\n", c.Format)
	return nil
}

func init() {
	c, _ := OptionsParser.AddCommand("schema", "commands to manage schemas", "check or update schemas", &SchemaCmd{})
	_, _ = c.AddCommand("check", "Check schema", "check the schema", &SchemaCheck{})
	_, _ = c.AddCommand("upsert", "Upsert schema", "insert or update the schema", &SchemaUpsert{})
	_, _ = c.AddCommand("dump", "Dump schema", "display the schema in a given format", &SchemaDump{})
}
