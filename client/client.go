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
	"fmt"

	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/config"
	"github.com/uber-go/dosa/connectors/devnull"
	"github.com/uber-go/dosa/connectors/memory"
	"github.com/uber-go/dosa/connectors/random"
	"github.com/uber-go/dosa/connectors/yarpc"
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
	// create connection from config
	var conn dosa.Connector
	var connErr error
	switch cfg.Connector {
	case devnull.Name():
		conn, connErr = dosa.GetConnector(devnull.Name(), nil)
	case memory.Name():
		conn, connErr = dosa.GetConnector(memory.Name(), nil)
	case random.Name():
		conn, connErr = dosa.GetConnector(random.Name(), nil)
	case yarpc.Name():
		conn, connErr = dosa.GetConnector(yarpc.Name(), map[string]interface{}{
			"transport":   cfg.Yarpc.Transport,
			"host":        cfg.Yarpc.Host,
			"port":        cfg.Yarpc.Port,
			"callername":  cfg.Yarpc.CallerName,
			"servicename": cfg.Yarpc.ServiceName,
		})
	default:
		return nil, fmt.Errorf("unknown connector type: %s - must be one of: devnull, memory, random or yarpc", cfg.Connector)
	}
	if connErr != nil {
		return nil, errors.Wrapf(err, "could not create connector for type %s", cfg.Connector)
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
