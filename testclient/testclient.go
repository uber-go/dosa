package testclient

import (
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/memory"
)

// NewTestClient creates a DOSA client useful for testing.
func NewTestClient(scope, prefix string, entities ...dosa.DomainObject) (dosa.Client, error) {
	reg, err := dosa.NewRegistrar(scope, prefix, entities...)
	if err != nil {
		return nil, err
	}
	connector := memory.NewConnector()
	return dosa.NewClient(reg, connector), nil
}
