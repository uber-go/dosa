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
	"fmt"

	"github.com/uber-go/dosa"
)

// ScopeOptions are options specific to the scope subcommand.
type ScopeOptions struct {
	// empty for now
}

// Scope is the entrypoint for the scope command
func Scope(args []string, opts *ScopeOptions, client dosa.AdminClient) int {
	if len(args) == 0 {
		return scopeUsage()
	}

	// dispatch sub-command
	// TODO: refactor to use flags.NewNamedParser
	switch args[0] {
	case "create":
		return scopeCreate(args[1], client)
	case "list":
		return scopeList(client)
	default:
		return scopeUsage()
	}
}

// TODO: not implemented
func scopeCreate(scopeName string, client dosa.AdminClient) int {
	fmt.Printf("create scope %s\n", scopeName)
	return 0
}

// TODO: not implemented
func scopeList(client dosa.AdminClient) int {
	fmt.Println("scope list")
	return 0
}

func scopeUsage() int {
	fmt.Println("scope usage")
	return 0
}
