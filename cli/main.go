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

// GlobalOptions are options for all subcommands
type GlobalOptions struct {
	Host        string   `short:"h" long:"host" default:"127.0.0.1" description:"The hostname or IP for the gateway."`
	Port        string   `short:"p" long:"port" default:"5437" description:"The hostname or IP for the gateway."`
	Transport   string   `long:"transport" default:"tchannel" description:"TCP Transport to use. Options: http, tchannel."`
	ServiceName string   `long:"service" default:"dosa-gateway" description:"The TChannel service name for the gateway."`
	CallerName  string   `long:"caller" default:"dosacli-$USER" description:"Caller will override the default caller name (which is dosacli-$USER)."`
	Timeout     timeFlag `long:"timeout" default:"60s" description:"The timeout for gateway requests. E.g., 100ms, 0.5s, 1s. If no unit is specified, milliseconds are assumed."`
	Connector   string   `long:"connector" default:"yarpc" description:"Name of connector to use"`
}

var options GlobalOptions

// OptionsParser holds the global parser
var OptionsParser = flags.NewParser(&options, flags.IgnoreUnknown)

func main() {
	OptionsParser.ShortDescription = "DOSA CLI - The command-line tool for your DOSA client"
	OptionsParser.LongDescription = `
dosa manages your schema both in production and development scopes`

	_, err := OptionsParser.Parse()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		OptionsParser.WriteHelp(os.Stderr)
		exit(1)
		return
	}

	exit(0)
}
