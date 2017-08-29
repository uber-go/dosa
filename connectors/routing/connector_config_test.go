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

package routing

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewConnectorConfigs(t *testing.T) {
	connectorMap := getConnectorMap()
	cConfigMap, err := NewConnectorConfigs(cfg, connectorMap)
	assert.NoError(t, err)
	assert.Len(t, cConfigMap, 9)
}

func TestNewConnectorConfigsEmptyConnectorConfig(t *testing.T) {
	connectorMap := getConnectorMap()
	dummyCfg := Config{
		Routers: []map[string]map[string]string{
			{
				"production": nil,
			},
		},
	}

	cConfigMap, err := NewConnectorConfigs(dummyCfg, connectorMap)
	assert.Contains(t, err.Error(), "connector config under scope should be defined in yaml config file")
	assert.Nil(t, cConfigMap)
}

func TestNewConnectorConfigsNoDefaultNamePrefix(t *testing.T) {
	connectorMap := getConnectorMap()
	dummyCfg := Config{
		Routers: []map[string]map[string]string{
			{
				"production": {
					"map": "memory",
				},
			},
		},
	}

	_, err := NewConnectorConfigs(dummyCfg, nil)
	assert.Contains(t, err.Error(), "connector map should not be empty")

	dummyCfg.Routers[0]["production"]["map"] = ""
	_, err = NewConnectorConfigs(dummyCfg, connectorMap)
	assert.Contains(t, err.Error(), "could not initialize ConnectorConfig whose scope is")
}

func TestFetchConnectorWithError(t *testing.T) {
	connectorMap := getConnectorMap()
	_, err := fetchConnector("doNotExist", connectorMap)
	assert.Contains(t, err.Error(), "does not exist in connector map")
}

func TestNewConnectorConfigError(t *testing.T) {
	connectorMap := getConnectorMap()
	cConn, err := NewConnectorConfig("", "", "", connectorMap)
	assert.Nil(t, cConn)
	assert.Contains(t, err.Error(), "connector name should not be empty")

	cConn, err = NewConnectorConfig("memory", "", "", connectorMap)
	assert.Nil(t, cConn)
	assert.Contains(t, err.Error(), "failed to initialize scopedConfig")

	cConn, err = NewConnectorConfig("dummyTest", "", "", connectorMap)
	assert.Nil(t, cConn)
	assert.Contains(t, err.Error(), "failed to fetch connector")
}
