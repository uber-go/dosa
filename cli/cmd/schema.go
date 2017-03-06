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
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/uber-go/dosa"
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

// SchemaOptions are options specific to the schema command.
type SchemaOptions struct {
	Scope  string `short:"s" long:"scope" description:"Storage scope for the given operation."`
	Format string `long:"format" default:"cql" descrption:"The format for schema to be encoded as. Options: avro, uql, cql."`
}

// Schema is the entrypoint for the schema subcommand
func Schema(args []string, opts *SchemaOptions, client dosa.AdminClient) int {
	if len(args) == 0 {
		return schemaUsage()
	}

	// dispatch sub-command
	// TODO: refactor to use flags.NewNamedParser
	switch args[0] {
	case "check":
		return schemaCheck(args[1:], opts, client)
	case "dump":
		return schemaDump(args[1:], opts, client)
	default:
		return schemaUsage()
	}
}

// schemaCheck is the entrypoint for the `schema check` subcommand.
func schemaCheck(args []string, opts *SchemaOptions, client dosa.AdminClient) int {
	// stat search paths, return list of absolute paths to look for entities
	dirs, err := expandDirectories(args)
	if err != nil {
		fmt.Fprint(os.Stderr, err)
		return 1
	}

	for _, dir := range dirs {
		fmt.Println(dir)
	}

	// TODO: call connector.CheckSchema()
	fmt.Println("schema check")
	return 0
}

// schemaDump is the entrypoint for the `schema dump` subcommand. Given an slice
// of table definitions, write definitions to stdout in the given output format.
func schemaDump(args []string, opts *SchemaOptions, client dosa.AdminClient) int {
	// TODO: use opts
	exclude := "_test.go"
	outputFormat := "cql"
	pedantic := false

	dirs, err := expandDirectories(args)
	if err != nil {
		fmt.Fprint(os.Stderr, err)
		return 1
	}

	// validate output format
	if !validOutputFormat(outputFormat) {
		fmt.Fprintf(os.Stderr, "Unknown output format %q\n", outputFormat)
		return 1
	}

	// try to parse entities in each directory
	entities, err := findEntities(dirs, exclude, pedantic)
	if err != nil {
		fmt.Fprint(os.Stderr, err)
		return 1
	}

	// for each of those entities, format it in the specified way
	for _, entity := range entities {
		switch outputFormat {
		case "cql":
			fmt.Println(cql.ToCQL(&entity.EntityDefinition))
		case "uql":
			fmt.Println(uql.ToUQL(&entity.EntityDefinition))
		case "avro":
			fmt.Println(avro.ToAvro("TODO", &entity.EntityDefinition))
		}
	}
	return 0
}

// schemaUsage prints schema subcommand usage
func schemaUsage() int {
	fmt.Println("schema usage")
	return 0
}

func validOutputFormat(outputFormat string) bool {
	return schemaDumpOutputTypes[outputFormat]
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

// findEntities visits each directory to generate entity table definitions.
// An error is returned if no entities are found
func findEntities(dirs []string, exclude string, pedantic bool) ([]*dosa.Table, error) {
	entities := make([]*dosa.Table, 0)
	for _, dir := range dirs {
		ents, errs, err := dosa.FindEntities(dir, exclude)
		if err != nil {
			fmt.Fprint(os.Stderr, err)
			return nil, err
		}

		if errs != nil && len(errs) > 0 {
			for _, err := range errs {
				fmt.Fprintf(os.Stderr, "warning: %s\n", err)
			}
			if pedantic {
				fmt.Fprint(os.Stderr, "Failed (warnings are errors in pedantic mode)")
				os.Exit(1)
			}
		}
		entities = append(entities, ents...)
	}

	// if nothing was found, return an error
	if len(entities) == 0 {
		return nil, fmt.Errorf("no valid DOSA-annotated entities were found in the directories %s", dirs)
	}

	return entities, nil
}
