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
	"strconv"
	"time"

	"github.com/uber-go/dosa/cli/cmd"
)

// Options are options for all subcommands
type Options struct {
	Host        string   `short:"h" long:"host" default:"127.0.0.1" description:"The hostname or IP for the gateway."`
	Port        string   `short:"p" long:"port" default:"6707" description:"The hostname or IP for the gateway."`
	Transport   string   `long:"transport" default:"tchannel" description:"TCP Transport to use. Options: http, tchannel."`
	ServiceName string   `long:"service" default:"dosa-gateway" description:"The TChannel service name for the gateway."`
	CallerName  string   `long:"caller" default:"dosacli-$USER" description:"Caller will override the default caller name (which is dosacli-$USER)."`
	Timeout     timeFlag `long:"timeout" default:"1s" description:"The timeout for gateway requests. E.g., 100ms, 0.5s, 1s. If no unit is specified, milliseconds are assumed."`
}

// Commands contains subcommand configuration
type Commands struct {
	*Options
	Scope  *cmd.ScopeCommands  `command:"scope"`
	Schema *cmd.SchemaCommands `command:"schema"`
}

type timeFlag time.Duration

func (t *timeFlag) setDuration(d time.Duration) {
	*t = timeFlag(d)
}

// Duration returns the flag value as a time.Duration
func (t timeFlag) Duration() time.Duration {
	return time.Duration(t)
}

// UmarshalFlag satisfies the flag interface
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
