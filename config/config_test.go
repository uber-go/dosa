package config_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/config"
	"github.com/uber-go/dosa/connectors/yarpc"
)

type SinglePartitionKey struct {
	dosa.Entity     `dosa:"primaryKey=PrimaryKey"`
	PrimaryKey int64
	data       string
}

func TestConfig_NewClient(t *testing.T) {
	cases := []struct {
		cfg     config.Config
		isErr   bool
	}{
		{
			// success
			cfg:    config.Config{
				Scope: "test",
				NamePrefix: "namePrefix",
				Yarpc: yarpc.Config{
					Transport:   "http",
					Host:        "localhost",
					Port:        "8080",
					CallerName:  "dosa-test",
					ServiceName: "dosa-gateway",
				},
			},
			isErr: false,
		},
		{
			// registrar fail
			cfg:    config.Config{
				Scope: "test",
				NamePrefix: "name*(Prefix",
				Yarpc: yarpc.Config{
					Transport:   "http",
					Host:        "localhost",
					Port:        "8080",
					CallerName:  "dosa-test",
					ServiceName: "dosa-gateway",
				},
			},
			isErr: true,
		},
		{
			// yarpc fail
			cfg:    config.Config{
				Scope: "test",
				NamePrefix: "name*(Prefix",
				Yarpc: yarpc.Config{
					Transport:   "http",
					Host:        "localhost",
					Port:        "8080",
				},
			},
			isErr: true,
		},
	}

	for _, c := range cases {
		_, err := c.cfg.NewClient(&SinglePartitionKey{})
		if c.isErr {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}
}
