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

	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
)

// ScopeOptions contains configuration for scope command flags
type ScopeOptions struct{}

// ScopeCmd is a placeholder for all scope commands
type ScopeCmd struct{}

func (c *ScopeCmd) doScopeOp(name string, f func(dosa.AdminClient, context.Context, string) error, scopes []string) error {
	// set default service name if one isn't provided, this is done here instead
	// of in the struct tags because schema and scope commands differ slightly
	// in how the service name should be inferred.
	if options.ServiceName == "" {
		options.ServiceName = _defServiceName // defined in options.go
	}

	client, err := getAdminClient(options)
	if err != nil {
		return err
	}
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
	Args struct {
		Scopes []string `positional-arg-name:"scopes" required:"1"`
	} `positional-args:"yes" required:"1"`
}

// Execute executes a scope create command
func (c *ScopeCreate) Execute(args []string) error {
	return c.doScopeOp("create", dosa.AdminClient.CreateScope, c.Args.Scopes)
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
