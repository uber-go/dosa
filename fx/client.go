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

package dosafx

import (
	"context"

	"go.uber.org/fx/service"
	"go.uber.org/yarpc"

	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/config"
	dosarpc "github.com/uber-go/dosa/connectors/yarpc"
	"github.com/uber-go/dosa/registry"
)

// New creates a new DOSA client that can be registered as an FX module.
func New(d *yarpc.Dispatcher, h service.Host) (dosa.Client, error) {
	cfg, err := config.NewConfigForService(h)
	if err != nil {
		return nil, errors.Wrap(err, "could not populate DOSA configuration")
	}

	reg, err := registry.NewRegistrar(cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "could not register DOSA entities in %s", cfg.EntityPaths)
	}

	// TODO: we should have a better way to inject/override YARPC
	if connName, ok := cfg.Connector["name"].(string); ok {
		if connName != "yarpc" {
			conn, err := dosa.GetConnector(connName, nil)
			if err != nil {
				return nil, errors.Wrapf(err, "GetConnector failed for connector with name: %v", connName)
			}
			// must be a test connector
			return dosa.NewClient(reg, conn), nil
		}
	}

	// before the connection can be used, it must be started
	cc := d.ClientConfig(cfg.Service())
	if err := cc.GetUnaryOutbound().Start(); err != nil {
		return nil, errors.Wrap(err, "could not start outbound connection")
	}

	// client init
	client := dosa.NewClient(reg, dosarpc.NewConnectorWithTransport(cc))
	ctx, cancelFn := context.WithTimeout(context.Background(), cfg.Timeout.Initialize)
	defer cancelFn()
	if err := client.Initialize(ctx); err != nil {
		return nil, errors.Wrap(err, "could not initialize DOSA client")
	}
	return client, nil
}
