// Copyright (c) 2019 Uber Technologies, Inc.
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
	"bytes"
	"fmt"
	"os"
	"os/exec"

	"github.com/jessevdk/go-flags"
	"github.com/uber-go/dosa"
)

// for testing, we make exit an overridable routine
type exiter func(int)

var exit = os.Exit

type adminClientProvider func(opts GlobalOptions) (dosa.AdminClient, error)
type mdClientProvider func(opts GlobalOptions) (dosa.Client, error)
type queryClientProvider func(opts GlobalOptions, scope, prefix, path, structName string) (ShellQueryClient, error)

// these are overridden at build-time w/ the -ldflags -X option
var (
	version           = "0.0.0"
	githash           = "master"
	timestamp         = "now"
	javaclientVersion = "1.1.0-beta"
	javaclient        = os.Getenv("HOME") + "/.m2/target/dependency/java-client-" + javaclientVersion + ".jar"
)

// BuildInfo reports information about the binary build environment
type BuildInfo struct {
	Version   string
	Githash   string
	Timestamp string
}

// String satisfies Stringer interface
func (b BuildInfo) String() string {
	return fmt.Sprintf("Version:\t%s\nGit Commit:\t%s\nUTC Build Time:\t%s", version, githash, timestamp)
}

// Execute is ran for the version subcommand
func (b BuildInfo) Execute(args []string) error {
	fmt.Printf("%s\n", b)
	return nil
}

// GlobalOptions are options for all subcommands
type GlobalOptions struct {
	Host        string     `long:"host" default:"127.0.0.1" description:"The hostname or IP for the gateway."`
	Port        string     `short:"p" long:"port" default:"21300" description:"The hostname or IP for the gateway."`
	ServiceName string     `short:"s" long:"service" default:"dosa-gateway" description:"The TChannel service name for the gateway."`
	CallerName  callerFlag `long:"caller" default:"dosacli-$USER" description:"The RPC Caller name."`
	Timeout     timeFlag   `long:"timeout" default:"60s" description:"The timeout for gateway requests. E.g., 100ms, 0.5s, 1s. If no unit is specified, milliseconds are assumed."`
	Version     bool       `long:"version" description:"Display version info"`
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
	_, _ = c.AddCommand("create", "Create scope", "creates a new scope", newScopeCreate(provideAdminClient))
	_, _ = c.AddCommand("drop", "Drop scope", "drops a scope", newScopeDrop(provideAdminClient))
	_, _ = c.AddCommand("truncate", "Truncate scope", "truncates a scope", newScopeTruncate(provideAdminClient))

	_, _ = c.AddCommand("list", "List scopes", "lists scopes", newScopeList(provideMDClient))
	_, _ = c.AddCommand("show", "Show scope MD", "show scope metadata", newScopeShow(provideMDClient))

	c, _ = OptionsParser.AddCommand("schema", "commands to manage schemas", "check or update schemas", &SchemaOptions{})
	_, _ = c.AddCommand("upsertable", "Check upsertability of schema", "schema upsertability", newSchemaUpsertable(provideAdminClient))
	_, _ = c.AddCommand("upsert", "Upsert schema", "insert or update the schema", newSchemaUpsert(provideAdminClient))
	_, _ = c.AddCommand("dump", "Dump schema", "display the schema in a given format", &SchemaDump{})
	_, _ = c.AddCommand("status", "Check schema status", "Check application status of schema", newSchemaStatus(provideAdminClient))

	c, _ = OptionsParser.AddCommand("query", "commands to do query", "fetch one or multiple rows", &QueryOptions{})
	_, _ = c.AddCommand("read", "Read query", "read a row by primary keys", newQueryRead(provideShellQueryClient))
	_, _ = c.AddCommand("range", "Range query", "read rows with range of primary keys and indexes", newQueryRange(provideShellQueryClient))

	// TODO: implement admin subcommand
	// c, _ = OptionsParser.AddCommand("admin", "commands to administrate", "", &AdminOptions{})

	_, err := OptionsParser.Parse()

	if options.Version {
		_, _ = fmt.Fprintf(os.Stdout, "%s\n", buildInfo.String())
		options.Version = false // for tests, we leak state between runs
		exit(0)
	}

	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		exit(1)
		return
	}

	exit(0)
}

func downloadJar() {
	fmt.Println("Downloading required dependencies... This may take some time...")
	cmd := exec.Command("mvn", "org.apache.maven.plugins:maven-dependency-plugin:RELEASE:copy",
		"-Dartifact=com.uber.dosa:java-client:"+javaclientVersion, "-Dproject.basedir="+os.Getenv("HOME")+"/.m2/")
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		fmt.Println(fmt.Sprint(err) + ": " + stderr.String())
		return
	}
}
