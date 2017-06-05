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

package dosaclient

import (
	"context"

	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/config"
	"github.com/uber-go/dosa/registry"
)

// New creates a new DOSA client from the configuration provided. Depending
// on the configuration, this method will create the appropriate connector
// and will try to find DOSA entities to register before returning an
// initialized client or an error. See the config package for defaults.
func New(cfg *config.Config) (dosa.Client, error) {
	// create registry from config, by default this will search ./entities/dosa
	// for types that implement dosa.DomainObject and have valid primary key.
	reg, err := registry.NewRegistrar(cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "could not register DOSA entities in %s", cfg.EntityPaths)
	}

	if cfg.Connector == nil {
		return nil, errors.Errorf("Connector configuration is nil")
	}

	args := dosa.CreationArgs(cfg.Connector)
	connName, ok := args["name"].(string)
	if !ok {
		return nil, errors.Errorf("Connector config must contain a string 'name' value (%v)", args)
	}

	// create connector with args
	conn, err := dosa.GetConnector(connName, args)
	if err != nil {
		return nil, errors.Wrapf(err, "GetConnector failed for connector with name: %v", connName)
	}

	// client init
	client := dosa.NewClient(reg, conn)
	ctx, cancelFn := context.WithTimeout(context.Background(), cfg.Timeout.Initialize)
	defer cancelFn()
	if err := client.Initialize(ctx); err != nil {
		return nil, errors.Wrap(err, "could not initialize DOSA client")
	}
	return client, nil
}
