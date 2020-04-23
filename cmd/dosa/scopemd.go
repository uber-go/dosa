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

	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
)

// ScopeList contains data for executing scope truncate command.
type ScopeList struct {
	*ScopeCmd
}

func newScopeList(provideClient mdClientProvider) *ScopeList {
	return &ScopeList{
		ScopeCmd: &ScopeCmd{
			provideMDClient: provideClient,
		},
	}
}

// Execute executes a scope list command
func (c *ScopeList) Execute(args []string) error {
	client, err := c.makeClient()
	if err != nil {
		return errors.Wrap(err, "could not make client")
	}
	defer shutdownMDClient(client)

	var scopes []string
	if scopes, err = c.getScopes(client); err == nil {
		return err
	}
	for _, sp := range scopes {
		fmt.Println(sp)
	}
	return nil
}

func (c *ScopeList) getScopes(client dosa.Client) ([]string, error) {
	scopeList := []string{}

	var md dosa.ScopeMetadata
	scanOp := dosa.NewScanOp(&md).Limit(100).Fields([]string{"Name"})
	for {
		ctx, cancel := context.WithTimeout(context.Background(), options.Timeout.Duration())
		defer cancel()

		scopes, token, err := client.ScanEverything(ctx, scanOp)
		if err != nil {
			if dosa.ErrorIsNotFound(err) {
				break
			}
			fmt.Printf("MD table scan failed (token=%q): %v\n", token, err)
			continue
		}
		scanOp.Offset(token)

		for _, e := range scopes {
			md := e.(*dosa.ScopeMetadata)
			scopeList = append(scopeList, md.Name)
		}
	}
	return scopeList, nil
}

// ScopeShow displays metadata for the specified scopes.
type ScopeShow struct {
	*ScopeCmd
	Args struct {
		Scopes []string `positional-arg-name:"scopes" required:"1"`
	} `positional-args:"yes" required:"1"`
}

func newScopeShow(provideClient mdClientProvider) *ScopeShow {
	return &ScopeShow{
		ScopeCmd: &ScopeCmd{
			provideMDClient: provideClient,
		},
	}
}

// Execute shows MD for the scope(s)
func (c *ScopeShow) Execute(args []string) error {
	client, err := c.makeClient()
	if err != nil {
		return errors.Wrap(err, "could not make client")
	}
	defer shutdownMDClient(client)

	for _, scope := range args {
		if md, err := c.getMetadata(client, scope); err != nil {
			fmt.Printf("Could not read scope metadata for %q: %v\n", scope, err)
		} else {
			fmt.Printf("%+v\n", md)
		}
	}
	return nil
}

// getMetadata returns the MD for a scope. Currently prefixes (for prod) are not handled; the intent
// is that a scope may be qualified by a name-prefix, as in "production.vsoffers".
func (c *ScopeShow) getMetadata(client dosa.Client, scope string) (*dosa.ScopeMetadata, error) {
	ctx, cancel := context.WithTimeout(context.Background(), options.Timeout.Duration())
	defer cancel()

	md := &dosa.ScopeMetadata{Name: scope}
	if err := client.Read(ctx, dosa.All(), md); err != nil {
		return nil, err
	}
	return md, nil
}

func (c *ScopeCmd) makeClient() (dosa.Client, error) {
	if options.ServiceName == "" {
		options.ServiceName = _defServiceName
	}
	return c.provideMDClient(options)
}
