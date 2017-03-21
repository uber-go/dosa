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
)

// ScopeCmd holds options for scope commands (there aren't any)
type ScopeCmd struct{}

// ScopeCreate contains data for executing scope create command.
type ScopeCreate struct{}

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
			return fmt.Errorf("create scope failed: %v", err)
		}
		fmt.Printf("created scope %q\n", s)
	}
	return nil
}

// ScopeDrop contains data for executing scope drop command.
type ScopeDrop struct {
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
			return fmt.Errorf("drop scope failed: %v", err)
		}
		fmt.Printf("dropped scope %q\n", s)
	}

	return nil
}

// ScopeTruncate contains data for executing scope truncate command.
type ScopeTruncate struct {
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
			return fmt.Errorf("truncate scope failed: %v", err)
		}
		fmt.Fprintf(os.Stdout, "truncated scope: %v\n", s)
	}
	return nil
}

func init() {
	c, _ := OptionsParser.AddCommand("scope", "commands to manage scope", "create, drop, or truncate development scopes", &ScopeCmd{})
	_, _ = c.AddCommand("create", "Create scope", "creates a new scope", &ScopeCreate{})
	_, _ = c.AddCommand("drop", "Drop scope", "drops a scope", &ScopeDrop{})
	_, _ = c.AddCommand("truncate", "Truncate scope", "truncates a scope", &ScopeTruncate{})
}
