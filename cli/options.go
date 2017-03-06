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
	"strconv"
	"time"

	"github.com/jessevdk/go-flags"
	"github.com/uber-go/dosa/cli/cmd"
)

// Options contains definitions for all options that can be provided.
type Options struct {
	Base   BaseOptions       `group:"base"`
	Scope  cmd.ScopeOptions  `group:"scope"`
	Schema cmd.SchemaOptions `group:"schema"`
}

// BaseOptions are options for all subcommands
type BaseOptions struct {
	subcmd      string
	Transport   string   `long:"transport" default:"http" description:"TCP Transport to use. Options: http, tchannel."`
	Host        string   `short:"h" long:"host" default:"127.0.0.1" description:"The hostname or IP for the gateway."`
	Port        string   `short:"p" long:"port" default:"6708" description:"The hostname or IP for the gateway."`
	ServiceName string   `long:"service" default:"dosa-gateway" description:"The TChannel service name for the gateway."`
	CallerName  string   `long:"caller" default:"dosacli-$USER" description:"Caller will override the default caller name (which is dosacli-$USER)."`
	Timeout     timeFlag `long:"timeout" default:"1s" description:"The timeout for gateway requests. E.g., 100ms, 0.5s, 1s. If no unit is specified, milliseconds are assumed."`
	Pedantic    bool     `long:"pedantic"`
}

// getOptions returns Options instance from any slice of string arguments. Any
// output is written to the writer provided (eg. os.Stdout, os.Stderr)
func getOptions(args []string, out *os.File) (*Options, error) {
	opts := Options{}
	parser := flags.NewParser(&opts, flags.PassDoubleDash)
	parser.Usage = "[scope | schema | gen] [OPTIONS]"
	parser.ShortDescription = "DOSA CLI - The command-line tool for your DOSA client"
	parser.LongDescription = `
dosa is the command-line tool for common tasks related to storing data with the DOSA client.`

	remaining, err := parser.ParseArgs(args)

	// If there are no arguments specified, write the help, but do it after
	// Parse so that output shows defaults
	if len(remaining) == 0 {
		parser.WriteHelp(out)
		return &opts, errExit
	}

	if err != nil {
		if ferr, ok := err.(*flags.Error); ok {
			if ferr.Type == flags.ErrHelp {
				parser.WriteHelp(out)
				return &opts, errExit
			}
		}
		return &opts, err
	}

	if opts.Base.CallerName == "" || opts.Base.CallerName == "dosacli-$USER" {
		opts.Base.CallerName = fmt.Sprintf("dosacli-%s", os.Getenv("USER"))
	}

	// sub-command always required
	if ok := fromPositional(remaining, 0, &opts.Base.subcmd); !ok {
		parser.WriteHelp(out)
		return &opts, errExit
	}

	// special case for "help" subcommand
	if opts.Base.subcmd == "help" {
		parser.WriteHelp(out)
	}

	return &opts, nil
}

func fromPositional(args []string, index int, s *string) bool {
	if len(args) <= index {
		return false
	}

	if args[index] != "" {
		*s = args[index]
	}

	return true
}

type timeFlag time.Duration

func (t *timeFlag) setDuration(d time.Duration) {
	*t = timeFlag(d)
}

func (t timeFlag) Duration() time.Duration {
	return time.Duration(t)
}

func (t *timeFlag) UnmarshalFlag(value string) error {
	valueInt, err := strconv.Atoi(value)
	if err == nil {
		// We received a number without a unit, assume milliseconds.
		t.setDuration(time.Duration(valueInt) * time.Millisecond)
		return nil
	}

	d, err := time.ParseDuration(value)
	if err != nil {
		return err
	}

	t.setDuration(d)
	return nil
}
