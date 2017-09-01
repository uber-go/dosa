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
	"gopkg.in/yaml.v2"
)

// TestBasicConfig test the basic yaml file conversion
func TestBasicConfig(t *testing.T) {
	yamlFile := `
routers:
# routers structure looks like:
# - [scope]
#    [namePrefix_1]: connectorName
#    [namePrefix_2]: connectorName
- production:
    default: cassandra
    serviceA: cassandra
- development:
    default: cassandra
    serviceB: cassandra
- eats:
    '*': eats
    bazaar.*: eats
    default: eats
    eats-store: eats
- default:
    default: dosa
`
	testCfg := &Config{}
	err := yaml.Unmarshal([]byte(yamlFile), testCfg)
	assert.NoError(t, err)
	assert.Len(t, testCfg.Routers, 9)
	rs := Routers{
		buildRouter("production", "serviceA", "cassandra"),
		buildRouter("production", "default", "cassandra"),
		buildRouter("eats", "eats-store", "eats"),
		buildRouter("eats", "default", "eats"),
		buildRouter("eats", "bazaar.*", "eats"),
		buildRouter("eats", "*", "eats"),
		buildRouter("development", "serviceB", "cassandra"),
		buildRouter("development", "default", "cassandra"),
		buildRouter("default", "default", "dosa"),
	}
	assert.Equal(t, testCfg.Routers, rs)
	err = yaml.Unmarshal([]byte(`bad yaml file`), testCfg)
	assert.Error(t, err)
}

func buildRouter(scope, namePrefix, connector string) *RoutingConfig {
	rc, _ := NewRoutingConfig(scope, namePrefix, connector)
	return rc
}


func TestRouter(t *testing.T) {
	yamlFile := `
routers:
# routers structure looks like:
# - [scope]
#    [namePrefix_1]: connectorName
#    [namePrefix_2]: connectorName
- production:
    default: cassandra
    serviceA: cassandra
- development:
    default: cassandra
    serviceB: cassandra
- eats:
    '*': eats
    bazaar.*: eats
    default: eats
    eats-store: eats
- default:
    default: dosa
`
	testCfg := &Config{}
	err := yaml.Unmarshal([]byte(yamlFile), testCfg)
	assert.NoError(t, err)

	cfg, err := testCfg.FindDefaultRouter()
	assert.NoError(t, err)
	assert.Equal(t, cfg.Scope, "default")

	cfg, err = testCfg.FindRouter("production", "serviceA")
	assert.NoError(t, err)
	assert.Equal(t, cfg, buildRouter("production", "serviceA", "cassandra"))

	cfg, err = testCfg.FindRouter("eats", "bazaar.k")
	assert.NoError(t, err)
	assert.Equal(t, cfg, buildRouter("eats", "bazaar.*", "eats"))

	cfg, err = testCfg.FindRouter("eats", "d.k")
	assert.NoError(t, err)
	assert.Equal(t, cfg, buildRouter("eats", "*", "eats"))
}