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
	"flag"
	"fmt"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/schema/avro"
	"github.com/uber-go/dosa/schema/cql"
	"github.com/uber-go/dosa/schema/uql"
	"os"
	"path/filepath"
	"strings"
)

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

var validOutputTypes = map[string]bool{"cql": true, "uql": true, "avro": true}

func validOutputFormat(outputFormat string) bool {
	return validOutputTypes[outputFormat]
}

func main() {
	// Override the default usage message
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s scans your sources and finds DOSA annotated objects\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Usage: %s [options] [directories]\nThe current directory is used if no directory is specified.\nPossible options are:\n", os.Args[0])
		flag.PrintDefaults()
	}
	// process the command line arguments
	flag.Parse()

	// validate output format
	if !validOutputFormat(outputFormat) {
		fmt.Fprintf(os.Stderr, "Unknown output format %q\n", outputFormat)
		os.Exit(1)
	}

	// compute the list of directories to search through
	expandedDirList, err := expandDirectories(flag.Args())
	if err != nil {
		fmt.Fprint(os.Stderr, err)
		os.Exit(1)
	}
	// find all the entities in the directories
	entities := make([]*dosa.Table, 0)
	for _, dir := range expandedDirList {
		ents, errs, err := dosa.FindEntities(dir, exclusionList)
		if err != nil {
			fmt.Fprint(os.Stderr, err)
			os.Exit(1)
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

	// if nothing at all was found, that's an error
	if len(entities) == 0 {
		fmt.Fprintf(os.Stderr, "No valid DOSA-annotated entities were found in the directories %s\n", expandedDirList)
		os.Exit(1)
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
}

var outputFormat string

func init() {
	const (
		defaultFormat = "cql"
		usage         = "the output format (cql, uql, avro, etc)"
	)
	flag.StringVar(&outputFormat, "format", defaultFormat, usage)
}

var exclusionList string

func init() {
	const (
		defaultExclusionList = "*_test.go"
		usage                = "pattern of files to exclude"
	)
	flag.StringVar(&exclusionList, "exclude", defaultExclusionList, usage)
}

var pedantic bool

func init() {
	const (
		defaultPedantic = false
		usage           = "treat warnings as errors"
	)
	flag.BoolVar(&pedantic, "pedantic", defaultPedantic, usage)
}
