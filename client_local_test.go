package dosa

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

// just for 100% coverage. This belongs in client_test but can't since
// it requires calling a private method
func TestIsDomainObject(t *testing.T) {
	e := &Entity{}
	assert.True(t, e.isDomainObject())
}