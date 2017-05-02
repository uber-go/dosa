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
	"fmt"
	"os"

	flags "github.com/jessevdk/go-flags"
	_ "github.com/uber-go/dosa/connectors/yarpc"
)

// for testing, we make exit an overridable routine
type exiter func(int)

var exit = os.Exit

// these are overridden at build-time w/ the -ldflags -X option
var (
	version   = "0.0.0"
	githash   = "master"
	timestamp = "now"
)

// BuildInfo reports information about the binary build environment
type BuildInfo struct {
	Version   string
	Githash   string
	Timestamp string
}

func (b BuildInfo) String() string {
	return fmt.Sprintf("Version:\t%s\nGit Commit:\t%s\nUTC Build Time:\t%s", version, githash, timestamp)
}

func (b BuildInfo) Execute(args []string) error {
	fmt.Printf("%s\n", b)
	return nil
}

// GlobalOptions are options for all subcommands
type GlobalOptions struct {
	Host        string   `long:"host" default:"127.0.0.1" description:"The hostname or IP for the gateway."`
	Port        string   `short:"p" long:"port" default:"21300" description:"The hostname or IP for the gateway."`
	Transport   string   `long:"transport" default:"tchannel" description:"TCP Transport to use. Options: http, tchannel."`
	ServiceName string   `long:"service" description:"The TChannel service name for the gateway."`
	CallerName  string   `long:"caller" default:"dosacli-$USER" description:"Caller will override the default caller name (which is dosacli-$USER)."`
	Timeout     timeFlag `long:"timeout" default:"60s" description:"The timeout for gateway requests. E.g., 100ms, 0.5s, 1s. If no unit is specified, milliseconds are assumed."`
	Connector   string   `hidden:"true" long:"connector" default:"yarpc" description:"Name of connector to use"`
	Version     bool     `long:"version" description:"Display version info"`
}

var (
	options   GlobalOptions
	buildInfo BuildInfo
)

// OptionsParser holds the global parser

func main() {
	buildInfo := &BuildInfo{}
	OptionsParser := flags.NewParser(&options, flags.PassAfterNonOption|flags.HelpFlag)
	OptionsParser.ShortDescription = "DOSA CLI - The command-line tool for your DOSA client"
	OptionsParser.LongDescription = `
dosa manages your schema both in production and development scopes`
	c, _ := OptionsParser.AddCommand("version", "display build info", "display build info", &BuildInfo{})

	c, _ = OptionsParser.AddCommand("scope", "commands to manage scope", "create, drop, or truncate development scopes", &ScopeOptions{})
	_, _ = c.AddCommand("create", "Create scope", "creates a new scope", &ScopeCreate{})
	_, _ = c.AddCommand("drop", "Drop scope", "drops a scope", &ScopeDrop{})
	_, _ = c.AddCommand("truncate", "Truncate scope", "truncates a scope", &ScopeTruncate{})

	c, _ = OptionsParser.AddCommand("schema", "commands to manage schemas", "check or update schemas", &SchemaOptions{})
	_, _ = c.AddCommand("check", "Check schema", "check the schema", &SchemaCheck{})
	_, _ = c.AddCommand("upsert", "Upsert schema", "insert or update the schema", &SchemaUpsert{})
	_, _ = c.AddCommand("dump", "Dump schema", "display the schema in a given format", &SchemaDump{})
	_, _ = c.AddCommand("status", "Check schema status", "Check application status of schema", &SchemaStatus{})

	_, err := OptionsParser.Parse()

	if options.Version {
		fmt.Fprintf(os.Stdout, "%s\n", buildInfo.String())
		options.Version = false // for tests, we leak state between runs
		exit(0)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		exit(1)
		return
	}

	exit(0)
}
