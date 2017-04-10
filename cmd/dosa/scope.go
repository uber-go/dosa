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
	"os"

	"github.com/pkg/errors"
)

// ScopeOptions contains configuration for scope command flags
type ScopeOptions struct{}

// ScopeArgs specifies the required positional args for scope commands
type ScopeArgs struct {
	Scopes []string `positional-arg-name:"scopes" required:"1"`
}

// ScopeCreate contains data for executing scope create command.
type ScopeCreate struct {
	*ScopeArgs `positional-args:"yes" required:"1"`
}

// Execute executes a scope create command
func (c *ScopeCreate) Execute(args []string) error {
	client, err := getAdminClient(options)
	if err != nil {
		return err
	}

	for _, s := range args {
		ctx, cancel := context.WithTimeout(context.Background(), options.Timeout.Duration())
		defer cancel()
		if err := client.CreateScope(ctx, s); err != nil {
			return errors.Wrapf(err, "create scope on %q", s)
		}
		fmt.Printf("created scope %q\n", s)
	}
	return nil
}

// ScopeDrop contains data for executing scope drop command.
type ScopeDrop struct {
	*ScopeArgs `positional-args:"yes" required:"1"`
}

// Execute executes a scope drop command
func (c *ScopeDrop) Execute(args []string) error {
	client, err := getAdminClient(options)
	if err != nil {
		return err
	}

	for _, s := range args {
		ctx, cancel := context.WithTimeout(context.Background(), options.Timeout.Duration())
		defer cancel()
		if err := client.DropScope(ctx, s); err != nil {
			return errors.Wrapf(err, "drop scope on %q", s)
		}
		fmt.Printf("dropped scope %q\n", s)
	}

	return nil
}

// ScopeTruncate contains data for executing scope truncate command.
type ScopeTruncate struct {
	*ScopeArgs `positional-args:"yes" required:"1"`
}

// Execute executes a scope truncate command
func (c *ScopeTruncate) Execute(args []string) error {
	client, err := getAdminClient(options)
	if err != nil {
		return err
	}

	for _, s := range args {
		ctx, cancel := context.WithTimeout(context.Background(), options.Timeout.Duration())
		defer cancel()
		if err := client.TruncateScope(ctx, s); err != nil {
			return errors.Wrapf(err, "truncate scope on %q", s)
		}
		fmt.Fprintf(os.Stdout, "truncated scope %q\n", s)
	}
	return nil
}
