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
	"errors"
	"fmt"
	"os"

	flags "github.com/jessevdk/go-flags"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/cli/cmd"
	"github.com/uber-go/dosa/connectors/yarpc"
)

var errExit = errors.New("sentinel error used to exit cleanly")

func main() {
	opts := Options{}
	parser := flags.NewParser(&opts, flags.PassDoubleDash)
	// parser.Usage = "[scope | schema] [OPTIONS]"
	parser.ShortDescription = "DOSA CLI - The command-line tool for your DOSA client"
	parser.LongDescription = `
dosa is the command-line tool for common tasks related to storing data with the DOSA client.`

	// ignore resulting args, need to populate base options first
	_, err := parser.Parse()
	if err != nil {
		fmt.Printf("Failed to parse options: %v\n\n", err)
		parser.WriteHelp(os.Stdout)
		os.Exit(1)
	}

	// defaults
	if opts.ServiceName == "" {
		opts.ServiceName = "dosa-gateway"
	}
	if opts.CallerName == "" || opts.CallerName == "dosacli-$USER" {
		opts.CallerName = fmt.Sprintf("dosacli-%s", os.Getenv("USER"))
	}

	// create YARPC connector
	conn, err := yarpc.NewConnector(&yarpc.Config{
		Transport:   opts.Transport,
		Host:        opts.Host,
		Port:        opts.Port,
		CallerName:  opts.CallerName,
		ServiceName: opts.ServiceName,
	})
	if err != nil {
		fmt.Printf("Failed to create connector: %v\n\n", err)
		os.Exit(1)
	}

	ctx, _ := context.WithTimeout(context.Background(), opts.Timeout.Duration())
	client := dosa.NewAdminClient(conn)
	cmds := &Commands{
		Scope: &cmd.ScopeCommands{
			Create:   cmd.NewScopeCreate(ctx, client),
			Drop:     cmd.NewScopeDrop(ctx, client),
			Truncate: cmd.NewScopeTruncate(ctx, client),
		},
		Schema: &cmd.SchemaCommands{
			Check:  cmd.NewSchemaCheck(ctx, client),
			Upsert: cmd.NewSchemaUpsert(ctx, client),
		},
	}

	// populate subcommand options and args
	subparser := flags.NewParser(cmds, flags.PassDoubleDash)

	// try to execute subcommand
	_, err = subparser.Parse()
	if err != nil {
		fmt.Printf("Failed to parse options: %v\n\n", err)
		subparser.WriteHelp(os.Stdout)
		os.Exit(1)
	}

	// subcommand executed successfully
	os.Exit(0)
}
