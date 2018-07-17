// Copyright (c) 2018 Uber Technologies, Inc.
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

// ScopeList contains data for executing scope truncate command.
type ScopeList struct {
	*ScopeCmd
}

func newScopeList(provideClient clientProvider) *ScopeLst {
	return &ScopeList{
		ScopeCmd: &ScopeCmd{
			provideClient: provideClient,
		},
	}
}

// Execute executes a scope list command
func (c *ScopeList) Execute(args []string) error {
	return c.doScopeOp("list", dosa.AdminClient.ListScope, c.Args.Scopes)
}

// ScopeShow contains data for executing scope truncate command.
type ScopeShow struct {
	*ScopeCmd
	Args struct {
		Scopes []string `positional-arg-name:"scopes" required:"1"`
	} `positional-args:"yes" required:"1"`
}

func newScopeShow(provideClient clientProvider) *ScopeShow {
	return &ScopeShow{
		ScopeCmd: &ScopeCmd{
			provideClient: provideClient,
		},
	}
}

// Execute executes a scope truncate command
func (c *ScopeShow) Execute(args []string) error {
	return c.doScopeOp("show",
		func() error {
			md := &dosa.ScopeMetadata{Name: c.Args.Name}
		}, c.Args.Scopes)
}
