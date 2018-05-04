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
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
)

// ScopeOptions contains configuration for scope command flags
type ScopeOptions struct{}

// ScopeCmd are options for all scope commands
type ScopeCmd struct{}

func (c *ScopeCmd) doScopeOp(name string, f func(dosa.AdminClient, context.Context, string) error, scopes []string) error {
	// TODO(eculver): use options/configurator pattern to apply defaults
	if options.ServiceName == "" {
		options.ServiceName = _defServiceName
	}

	client, err := getAdminClient(options)
	if err != nil {
		return err
	}
	defer shutdownAdminClient(client)

	for _, s := range scopes {
		ctx, cancel := context.WithTimeout(context.Background(), options.Timeout.Duration())
		defer cancel()
		if err := f(client, ctx, s); err != nil {
			return errors.Wrapf(err, "%s scope on %q", name, s)
		}
		fmt.Printf("%s scope %q: OK\n", name, s)
	}
	return nil
}

// ScopeCreate contains data for executing scope create command.
type ScopeCreate struct {
	*ScopeCmd
	Owner    string `short:"o" long:"owner" description:"The owning group (ublame name)"`
	Type     string `short:"t" long:"type" description:"Scope type (default: 'development')"`
	Cluster  string `short:"c" long:"cluster" description:"Hosting cluster for a production scope"`
	ReadRPS  int32  `short:"r" long:"read-rate" description:"Max read rate for a production scope"`
	WriteRPS int32  `short:"w" long:"write-rate" description:"Max write rate for a production scope"`
	Args     struct {
		Scopes []string `positional-arg-name:"scopes" required:"1"`
	} `positional-args:"yes" required:"1"`
}

// Execute executes a scope create command
func (c *ScopeCreate) Execute(args []string) error {
	typ, err := parseType(c.Type)
	if err != nil {
		return err
	}
	if len(c.Owner) == 0 {
		return errors.New("the owning ublame-group must be specified")
	}
	return c.doScopeOp("create",
		func(client dosa.AdminClient, ctx context.Context, scope string) error {
			return dosa.AdminClient.CreateScope(client, ctx, &dosa.ScopeMetadata{
				Name:        scope,
				Owner:       c.Owner,
				Type:        int32(typ),
				Creator:     *dosa.GetUsername(),
				Cluster:     c.Cluster,
				ReadMaxRPS:  c.ReadRPS,
				WriteMaxRPS: c.WriteRPS,
			})
		}, c.Args.Scopes)
}

// ScopeDrop contains data for executing scope drop command.
type ScopeDrop struct {
	*ScopeCmd
	Args struct {
		Scopes []string `positional-arg-name:"scopes" required:"1"`
	} `positional-args:"yes" required:"1"`
}

// Execute executes a scope drop command
func (c *ScopeDrop) Execute(args []string) error {
	return c.doScopeOp("drop", dosa.AdminClient.DropScope, c.Args.Scopes)
}

// ScopeTruncate contains data for executing scope truncate command.
type ScopeTruncate struct {
	*ScopeCmd
	Args struct {
		Scopes []string `positional-arg-name:"scopes" required:"1"`
	} `positional-args:"yes" required:"1"`
}

// Execute executes a scope truncate command
func (c *ScopeTruncate) Execute(args []string) error {
	return c.doScopeOp("truncate", dosa.AdminClient.TruncateScope, c.Args.Scopes)
}

func parseType(t string) (dosa.ScopeType, error) {
	if len(t) == 0 {
		return dosa.Development, nil
	}
	lt := strings.ToLower(t)
	if strings.HasPrefix("production", lt) {
		return dosa.Production, nil
	}
	if strings.HasPrefix("staging", lt) {
		return dosa.Staging, nil
	}
	if strings.HasPrefix("development", lt) {
		return dosa.Development, nil
	}
	return dosa.Development, fmt.Errorf("unknown scope type %q", t)
}
