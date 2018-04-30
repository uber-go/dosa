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
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/yarpc"
	"github.com/uber/dosa-idl/.gen/dosa/dosaclient"
	rpc "go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/tchannel"
)

const _defServiceName = "dosa-gateway"

var validNameRegex = regexp.MustCompile("^[a-z]+([a-z0-9]|[^-]-)*[^-]$")

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
		value = fmt.Sprintf("dosacli-%s", os.Getenv("USER"))
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

func provideYarpcClient(opts GlobalOptions) (dosa.AdminClient, clientFinisher, error) {
	// from YARPC: "must begin with a letter and consist only of dash-delimited
	// lower-case ASCII alphanumeric words" -- we do this here because YARPC
	// will panic if caller name is invalid.
	if !validNameRegex.MatchString(string(opts.CallerName)) {
		return nil, func() {}, fmt.Errorf("invalid caller name: %s, must begin with a letter and consist only of dash-delimited lower-case ASCII alphanumeric words", opts.CallerName)
	}

	ycfg := rpc.Config{Name: opts.CallerName.String()}
	hostPort := fmt.Sprintf("%s:%s", opts.Host, opts.Port)
	// this looks wrong, BUT since it's a uni-directional tchannel
	// connection, we have to pass CallerName as the tchannel "ServiceName"
	// for source/destination to be reported correctly by RPC layer.
	ts, err := tchannel.NewChannelTransport(tchannel.ServiceName(opts.CallerName.String()))
	if err != nil {
		return nil, func() {}, err
	}
	ycfg.Outbounds = rpc.Outbounds{
		opts.ServiceName: {
			Unary: ts.NewSingleOutbound(hostPort),
		},
	}

	// important to note that this will panic if config contains invalid
	// values such as service name containing invalid characters
	dispatcher := rpc.NewDispatcher(ycfg)
	if err := dispatcher.Start(); err != nil {
		return nil, func() {}, err
	}

	conn := yarpc.NewConnector(dosaclient.New(dispatcher.ClientConfig(opts.ServiceName)))

	client := dosa.NewAdminClient(conn)

	finisher := func() {
		if err := dispatcher.Stop(); err != nil {
			fmt.Fprintln(os.Stderr, "Failed to stop yarpc dispatcher")
		}
	}

	return client, finisher, nil
}

func shutdownAdminClient(client dosa.AdminClient) {
	if client.Shutdown() != nil {
		fmt.Fprintf(os.Stderr, "Failed to properly shutdown client")
	}
}
