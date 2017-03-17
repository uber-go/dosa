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

package cmd

import (
	"context"
	"fmt"

	"github.com/uber-go/dosa"
)

// ScopeCommands contains scope subcommand configuration.
type ScopeCommands struct {
	Create   *ScopeCreate   `command:"create"`
	Drop     *ScopeDrop     `command:"drop"`
	Truncate *ScopeTruncate `command:"truncate"`
}

// ScopeCreate contains data for executing scope create command.
type ScopeCreate struct {
	context context.Context
	client  dosa.AdminClient
}

// NewScopeCreate returns a new scope create command.
func NewScopeCreate(ctx context.Context, client dosa.AdminClient) *ScopeCreate {
	return &ScopeCreate{context: ctx, client: client}
}

// Execute satisfies flags.Commander interface.
func (c *ScopeCreate) Execute(args []string) error {
	if c.client != nil {
		fmt.Printf("creating scope(s): %v\n", args)
		for _, s := range args {
			if err := c.client.CreateScope(context.Background(), s); err != nil {
				return fmt.Errorf("create scope failed: %v", err)
			}
		}
	}
	return nil
}

// ScopeDrop contains data for executing scope drop command.
type ScopeDrop struct {
	context context.Context
	client  dosa.AdminClient
}

// NewScopeDrop returns a new scope drop command.
func NewScopeDrop(ctx context.Context, client dosa.AdminClient) *ScopeDrop {
	return &ScopeDrop{context: ctx, client: client}
}

// Execute satisfies flags.Commander interface.
func (c *ScopeDrop) Execute(args []string) error {
	if c.client != nil {
		for _, s := range args {
			if err := c.client.DropScope(context.Background(), s); err != nil {
				return fmt.Errorf("drop scope failed: %v", err)
			}
		}
	}
	return nil
}

// ScopeTruncate contains data for executing scope truncate command.
type ScopeTruncate struct {
	context context.Context
	client  dosa.AdminClient
}

// NewScopeTruncate returns a new scope create command.
func NewScopeTruncate(ctx context.Context, client dosa.AdminClient) *ScopeTruncate {
	return &ScopeTruncate{context: ctx, client: client}
}

// Execute satisfies flags.Commander interface.
func (c *ScopeTruncate) Execute(args []string) error {
	if c.client != nil {
		for _, s := range args {
			fmt.Printf("truncating scope %s\n", s)
			/*
				if err := c.client.TruncateScope(context.Background(), s); err != nil {
					return fmt.Errorf("truncate scope failed: %v\n", err)
				}
			*/
		}
	}
	return nil
}
