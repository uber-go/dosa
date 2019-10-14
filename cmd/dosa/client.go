// Copyright (c) 2019 Uber Technologies, Inc.
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

	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
)

// ShellQueryClient defines methods to be use by CLI tools
type ShellQueryClient interface {
	// Range fetches entities within a range
	Range(ctx context.Context, ops []*queryObj, fields []string, limit int) ([]map[string]dosa.FieldValue, error)
	// Read fetches a row by primary key
	Read(ctx context.Context, ops []*queryObj, fields []string, limit int) ([]map[string]dosa.FieldValue, error)
	// GetRegistrar returns the registrar
	GetRegistrar() dosa.Registrar
	// Shutdown gracefully shuts down the shell query client
	Shutdown() error
	// Initialize is called when initialize the shell query client
	Initialize(ctx context.Context) error
}

type shellQueryClient struct {
	registrar dosa.Registrar
	connector dosa.Connector
}

// newShellClient returns a new DOSA shell query client for the registrar and connector provided.
func newShellQueryClient(reg dosa.Registrar, conn dosa.Connector) ShellQueryClient {
	return &shellQueryClient{
		registrar: reg,
		connector: conn,
	}
}

// GetRegistrar returns the registrar that is registered in the client
func (c *shellQueryClient) GetRegistrar() dosa.Registrar {
	return c.registrar
}

func (c *shellQueryClient) Initialize(ctx context.Context) error {
	registar := c.GetRegistrar()
	reg, err := registar.Find(&dosa.Entity{})
	// this error should never happen for CLI query cases
	if err != nil {
		return errors.New("Error finding dosa entities")
	}

	version, err := c.connector.CheckSchema(ctx, registar.Scope(), registar.NamePrefix(), []*dosa.EntityDefinition{reg.EntityDefinition()})
	if err != nil {
		return errors.New("schema on the server is incompatible with the code")
	}
	reg.SetVersion(version)
	return nil
}

// Shutdown gracefully shuts down the client
func (c *shellQueryClient) Shutdown() error {
	return c.connector.Shutdown()
}
