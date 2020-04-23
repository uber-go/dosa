// Copyright (c) 2020 Uber Technologies, Inc.
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
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/yarpc"
)

const _defServiceName = "dosa-gateway"

// from YARPC: "must begin with a letter and consist only of dash-delimited
// lower-case ASCII alphanumeric words" -- we do this here because YARPC
// will panic if caller name is invalid.
var validNameRegex = regexp.MustCompile("^[a-z]+([a-z0-9]|[^-]-)*[^-]$")

// Error message for malformed service names.
const errmsg = "callerName %s must begin with a letter and consist only of dash-separated lower-case ASCII alphanumeric words"

type callerFlag string

func (s *callerFlag) setString(value string) {
	*s = callerFlag(strings.Replace(value, ".", "-", -1))
}

// String implements the stringer interface
func (s *callerFlag) String() string {
	return string(*s)
}

func (s *callerFlag) UnmarshalFlag(value string) error {
	if value == "" || value == "dosacli-$USER" {
		value = fmt.Sprintf("dosacli-%s", strings.ToLower(os.Getenv("USER")))
	}
	s.setString(value)
	return nil
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

func provideAdminClient(opts GlobalOptions) (dosa.AdminClient, error) {
	if !validNameRegex.MatchString(string(opts.CallerName)) {
		return nil, fmt.Errorf(errmsg, opts.CallerName)
	}

	ycfg := yarpc.Config{
		Host:         opts.Host,
		Port:         opts.Port,
		CallerName:   opts.CallerName.String(),
		ServiceName:  opts.ServiceName,
		ExtraHeaders: getAuthHeaders(),
	}

	conn, err := yarpc.NewConnector(ycfg)
	if err != nil {
		return nil, err
	}

	return dosa.NewAdminClient(conn), nil
}

func shutdownAdminClient(client dosa.AdminClient) {
	if client.Shutdown() != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to properly shutdown client")
	}
}

func provideMDClient(opts GlobalOptions) (c dosa.Client, err error) {
	if !validNameRegex.MatchString(string(opts.CallerName)) {
		return nil, fmt.Errorf(errmsg, opts.CallerName)
	}

	cfg := yarpc.ClientConfig{
		Scope:      "production",
		NamePrefix: "dosa_scopes_metadata",
		Yarpc: yarpc.Config{
			Host:         opts.Host,
			Port:         opts.Port,
			CallerName:   opts.CallerName.String(),
			ServiceName:  opts.ServiceName,
			ExtraHeaders: getAuthHeaders(),
		},
	}

	if c, err = cfg.NewClient((*dosa.ScopeMetadata)(nil)); err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if err = c.Initialize(ctx); err != nil {
		c = nil
	}
	return

}

func shutdownMDClient(client dosa.Client) {
	if client.Shutdown() != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to properly shutdown client")
	}
}

func provideShellQueryClient(opts GlobalOptions, scope, prefix, path, structName string) (ShellQueryClient, error) {
	// from YARPC: "must begin with a letter and consist only of dash-delimited
	// lower-case ASCII alphanumeric words" -- we do this here because YARPC
	// will panic if caller name is invalid.
	if !validNameRegex.MatchString(string(opts.CallerName)) {
		return nil, fmt.Errorf("invalid caller name: %s, must begin with a letter and consist only of dash-delimited lower-case ASCII alphanumeric words", opts.CallerName)
	}

	ycfg := yarpc.Config{
		Host:         opts.Host,
		Port:         opts.Port,
		CallerName:   opts.CallerName.String(),
		ServiceName:  opts.ServiceName,
		Transport:    opts.Transport,
		ExtraHeaders: getAuthHeaders(),
	}

	conn, err := yarpc.NewConnector(ycfg)
	if err != nil {
		return nil, err
	}

	// search the directory and get the entity with input name
	table, err := dosa.FindEntityByName(path, structName)
	if err != nil {
		return nil, err
	}

	reg, err := newSimpleRegistrar(scope, prefix, table)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), options.Timeout.Duration())
	defer cancel()

	client := newShellQueryClient(reg, conn)
	if err := client.Initialize(ctx); err != nil {
		return nil, err
	}

	return client, nil
}

func shutdownQueryClient(client ShellQueryClient) {
	if client.Shutdown() != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to properly shutdown client")
	}
}
