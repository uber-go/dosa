package routingconnector

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
	assert.Contains(t, err.Error(), "could not initialize ConfiguredConnector whose scope is")
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
