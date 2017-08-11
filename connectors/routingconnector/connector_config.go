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

package routingconnector

import (
	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
)

// ConnectorConfig wraps the connector and configuration for routing together
type ConnectorConfig struct {
	Connector dosa.Connector
	Config    *RoutingConfig
}

// NewConnectorConfigs initializes a list of ConfiguredConnector
func NewConnectorConfigs(cfg Config, connectorMap map[string]dosa.Connector) ([]*ConnectorConfig, error) {
	cConnectors := make([]*ConnectorConfig, 0)

	if cfg.Routers == nil || len(cfg.Routers) == 0 {
		return nil, errors.New("no connectors set in the config yaml file")
	}

	if connectorMap == nil || len(connectorMap) == 0 {
		return nil, errors.New("connector map should not be empty")
	}

	for _, cConfigMap := range cfg.Routers {
		for scope, cConfig := range cConfigMap {
			if cConfig == nil {
				return nil, errors.New("connector config under scope should be defined in yaml config file")
			}
			for namePrefix, connectorName := range cConfig {
				cConnector, err := NewConnectorConfig(connectorName, scope, namePrefix, connectorMap)
				if err != nil {
					return nil, errors.Wrapf(
						err, "could not initialize ConfiguredConnector whose scope is %v", scope)
				}
				cConnectors = append(cConnectors, cConnector)
			}
		}
	}

	return cConnectors, nil
}

// NewConnectorConfig initializes ConfiguredConnector
func NewConnectorConfig(connName string, scope string, namePrefix string, connectorMap map[string]dosa.Connector) (*ConnectorConfig, error) {
	if connName == "" {
		return nil, errors.New("connector name should not be empty. " +
			"connectorName should be defined in the yaml config file")
	}
	conn, err := fetchConnector(connName, connectorMap)
	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch connector")
	}

	rConfig, err := NewRoutingConfig(scope, namePrefix)
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize scopedConfig")
	}
	return &ConnectorConfig{
		Connector: conn,
		Config:    rConfig,
	}, nil
}

// fetchConnector fetches the connector based on the connectorName defined in yaml config file
func fetchConnector(connName string, connectorMap map[string]dosa.Connector) (dosa.Connector, error) {
	conn, ok := connectorMap[connName]
	if !ok {
		return nil, errors.Errorf("connector name %v does not exist in connector map", connName)
	}
	return conn, nil
}
