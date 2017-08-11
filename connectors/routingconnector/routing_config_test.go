package routingconnector

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRoutingConfig(t *testing.T) {
	rConfig, err := NewRoutingConfig("production", "test")
	assert.Nil(t, err)
	assert.Equal(t, rConfig.NamePrefix, "test")
}

func TestNewRoutingConfigWithStarPrefix(t *testing.T) {
	rConfig, err := NewRoutingConfig("production", "*.v1")
	assert.Nil(t, rConfig)
	assert.Contains(t, err.Error(), "is not supported")
}

func TestTestNewRoutingConfigError(t *testing.T) {
	rConfig, err := NewRoutingConfig("production", "")
	assert.Nil(t, rConfig)
	assert.Contains(t, err.Error(), "could not be empty")

	rConfig, err = NewRoutingConfig("", "test")
	assert.Nil(t, rConfig)
	assert.Contains(t, err.Error(), "scope could not be empty")
}
