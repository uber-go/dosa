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
	"errors"
	"fmt"
	"os"

	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/cli/cmd"
	"github.com/uber-go/dosa/connectors/yarpc"
)

var errExit = errors.New("sentinel error used to exit cleanly")

func main() {
	opts, err := getOptions(os.Args[1:], os.Stdout)
	if err != nil {
		if err == errExit {
			return
		}
		fmt.Printf("Failed to parse options: %v", err)
		os.Exit(1)
	}

	client, err := getClient(&opts.Base)
	if err != nil {
		fmt.Printf("Failed to create new client with options: %v\n", err)
		os.Exit(1)
	}

	// getOptions should guarantee that there is always a subcommand
	// however, there still may not be any arguments, but subcommands should
	// implement that behavior, presumably by using flags.NewNamedParser
	subargs := os.Args[1:]
	switch opts.Base.subcmd {
	case "scope":
		os.Exit(cmd.Scope(subargs[1:], &opts.Scope, client))
	case "schema":
		os.Exit(cmd.Schema(subargs[1:], &opts.Schema, client))
	case "help":
		os.Exit(0)
	default:
		os.Exit(1)
	}
}

func getClient(opts *BaseOptions) (dosa.AdminClient, error) {
	conn, err := yarpc.NewConnector(&yarpc.Config{
		Transport:   opts.Transport,
		Host:        opts.Host,
		Port:        opts.Port,
		CallerName:  opts.CallerName,
		ServiceName: opts.ServiceName,
	})
	if err != nil {
		return nil, err
	}

	client := dosa.NewAdminClient(conn)

	return client, nil
}
