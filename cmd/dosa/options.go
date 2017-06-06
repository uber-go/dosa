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
	"time"

	"github.com/uber-go/dosa"
)

const (
	_defServiceName  = "dosa-dev-gateway"
	_prodServiceName = "dosa-gateway"
	_prodScope       = "production"
)

var validNameRegex = regexp.MustCompile("^[a-z]+([a-z0-9]|[^-]-)*[^-]$")

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

func getAdminClient(opts GlobalOptions) (dosa.AdminClient, error) {
	// fix up the callername
	if opts.CallerName == "" || opts.CallerName == "dosacli-$USER" {
		opts.CallerName = fmt.Sprintf("dosacli-%s", os.Getenv("USER"))
	}

	// from YARPC: "must begin with a letter and consist only of dash-delimited
	// lower-case ASCII alphanumeric words" -- we do this here because YARPC
	// will panic if caller name is invalid.
	if !validNameRegex.MatchString(opts.CallerName) {
		return nil, fmt.Errorf("invalid caller name: %s, must begin with a letter and consist only of dash-delimited lower-case ASCII alphanumeric words", opts.CallerName)
	}

	// create connector
	conn, err := dosa.GetConnector(opts.Connector, map[string]interface{}{
		"transport":   opts.Transport,
		"host":        opts.Host,
		"port":        opts.Port,
		"callername":  opts.CallerName,
		"servicename": opts.ServiceName,
	})
	if err != nil {
		return nil, err
	}
	client := dosa.NewAdminClient(conn)

	return client, nil
}
