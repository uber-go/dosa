package config

import (
	"github.com/uber-go/dosa/connectors/yarpc"
	"github.com/uber-go/dosa"
)

// Config represents the settings for the dosa client
type Config struct {
	Scope      string `yaml:"scope"`
	NamePrefix string `yaml:"namePrefix"`
	Yarpc      yarpc.Config `yaml:"yarpc"`
}

// NewClient creates a DOSA client based on the configuration
func (c Config) NewClient(entities ...dosa.DomainObject) (dosa.Client, error) {
	reg, err := dosa.NewRegistrar(c.Scope, c.NamePrefix, entities...)
	if err != nil {
		return nil, err
	}

	conn, err := yarpc.NewConnector(&c.Yarpc)
	if err != nil {
		return nil, err
	}

	return dosa.NewClient(reg, conn)
}
