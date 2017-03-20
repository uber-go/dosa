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

package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
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
	NamePrefix string   `long:"prefix" description:"Name prefix for schema types."`
	Excludes   []string `short:"e" long:"exclude" description:"Exclude files matching pattern."`
	Scope      string   `short:"s" long:"scope" description:"Storage scope for the given operation."`
	Pedantic   bool     `long:"pedantic"`
}

// SchemaCommands contains schema subcommand configuration.
type SchemaCommands struct {
	Check  *SchemaCheck  `command:"check"`
	Upsert *SchemaUpsert `command:"upsert"`
}

// SchemaCheck contains data for executing schema check command.
type SchemaCheck struct {
	*SchemaOptions
	timeout time.Duration
	client  dosa.AdminClient
}

// NewSchemaCheck returns a new schema check command.
func NewSchemaCheck(timeout time.Duration, client dosa.AdminClient) *SchemaCheck {
	return &SchemaCheck{timeout: timeout, client: client}
}

// Execute satisfies flags.Commander interface.
func (c *SchemaCheck) Execute(args []string) error {
	if c.client != nil {
		if len(args) != 0 {
			dirs, err := expandDirectories(args)
			if err != nil {
				return errors.Wrap(err, "could not expand directories")
			}
			c.client.Directories(dirs)
		}
		if c.Scope != "" {
			c.client.Scope(c.Scope)
		}
		if len(c.Excludes) != 0 {
			c.client.Excludes(c.Excludes)
		}
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()
		versions, err := c.client.CheckSchema(ctx, c.NamePrefix)
		if err != nil {
			return fmt.Errorf("check schema failed: %v", err)
		}
		fmt.Println("versions")
		for _, v := range versions {
			fmt.Println(v)
		}
	}
	return nil
}

// SchemaUpsert contains data for executing schema upsert command.
type SchemaUpsert struct {
	*SchemaOptions
	timeout time.Duration
	client  dosa.AdminClient
}

// NewSchemaUpsert returns a new schema upsert command.
func NewSchemaUpsert(timeout time.Duration, client dosa.AdminClient) *SchemaUpsert {
	return &SchemaUpsert{timeout: timeout, client: client}
}

// Execute satisfies flags.Commander interface.
func (c *SchemaUpsert) Execute(args []string) error {
	if c.client != nil {
		if len(args) != 0 {
			dirs, err := expandDirectories(args)
			if err != nil {
				return errors.Wrap(err, "could not expand directories")
			}
			c.client.Directories(dirs)
		}
		if c.Scope != "" {
			c.client.Scope(c.Scope)
		}
		if len(c.Excludes) != 0 {
			c.client.Excludes(c.Excludes)
		}
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()
		versions, err := c.client.UpsertSchema(ctx, c.NamePrefix)
		if err != nil {
			return fmt.Errorf("upsert schema failed: %v", err)
		}
		fmt.Println("versions")
		for _, v := range versions {
			fmt.Println(v)
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
