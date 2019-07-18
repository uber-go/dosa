// Copyright (c) 2019 Uber Technologies, Inc.
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

var yamlFile = `
routers:
- production:
    "*": cassandra1
    serviceA: cassandra2
- development:
    default: cassandra3
    serviceB: cassandra4
- ebook:
    app: ebook0
    apple.*: ebook2
    '*': ebook1
    ebook-store: ebook4
- ebook*:
    foo: ebook-foo
    foo*: ebook-foo-2
    '*': ebook0
- default:
    foo_*: dosa1
    "*": dosa2
`

// TestBasicConfig test the basic yaml file conversion
func TestBasicConfig(t *testing.T) {
	testCfg := &Config{}
	err := yaml.Unmarshal([]byte(yamlFile), testCfg)
	assert.NoError(t, err)
	assert.Len(t, testCfg.Routers, 9)
	rs := Routers{
		buildRouter("development", "serviceB", "cassandra4"),
		buildRouter("development", "default", "cassandra3"),
		buildRouter("ebook", "app", "ebook0"),
		buildRouter("ebook", "apple.*", "ebook2"),
		buildRouter("ebook", "ebook-store", "ebook4"),
		buildRouter("ebook", "*", "ebook1"),
		buildRouter("ebook*", "foo", "ebook-foo"),
		buildRouter("ebook*", "foo*", "ebook-foo-2"),
		buildRouter("ebook*", "*", "ebook0"),
		buildRouter("production", "serviceA", "cassandra2"),
		buildRouter("production", "*", "cassandra1"),
		buildRouter("default", "foo_*", "dosa"),
		buildRouter("default", "*", "dosa2"),
	}
	assert.Equal(t, testCfg.Routers, rs)
	err = yaml.Unmarshal([]byte(`bad yaml file`), testCfg)
	assert.Error(t, err)
}

func buildRouter(scope, namePrefix, connector string) *Rule {
	rc, _ := NewRule(scope, namePrefix, connector)
	return rc
}

func TestRouter(t *testing.T) {
	testCfg := &Config{}
	err := yaml.Unmarshal([]byte(yamlFile), testCfg)
	assert.NoError(t, err)

	cfg := testCfg.findDefaultRouter()
	assert.Equal(t, cfg.scope, "default")

	cfg = testCfg.FindRouter("production", "serviceA")
	assert.Equal(t, cfg, buildRouter("production", "serviceA", "cassandra2"))

	cfg = testCfg.FindRouter("development", "serviceA")
	assert.Equal(t, cfg, buildRouter("development", "default", "cassandra3"))

	cfg = testCfg.FindRouter("ebook", "apple.k")
	assert.Equal(t, cfg, buildRouter("ebook", "apple.*", "ebook2"))

	cfg = testCfg.FindRouter("ebook", "d.k")
	assert.Equal(t, cfg, buildRouter("ebook", "*", "ebook1"))

	cfg = testCfg.FindRouter("ebook2", "bar")
	assert.Equal(t, cfg, buildRouter("ebook*", "*", "ebook0"))

	cfg = testCfg.FindRouter("ebook2", "foo_bar")
	assert.Equal(t, cfg, buildRouter("ebook*", "foo_bar", "ebook-foo-2"))

	cfg = testCfg.FindRouter("dev_user2", "foo_bar")
	assert.Equal(t, cfg, buildRouter("default", "foo_*", "dosa1"))

	cfg = testCfg.FindRouter("a", "d.k")
	assert.Equal(t, cfg, buildRouter("default", "*", "dosa2"))
}

var prodConfig = `
routers:
  - production:
      "dosa3test*": dosa_prod_a
      "eternal2a": dosa_prod_a
      "another_client*": dosa_prod_a
      "*": dosa
  - service:
      "*": cl_service
  - service_tier1:
      "*": cl_service_tier1
  - dosa_test:
      "*": dosa_staging
  - default:
      "*": dosa_dev
`

func TestProdConfig(t *testing.T) {
	// Make sure the production config works as expected.

	prodCfg := &Config{}
	err := yaml.Unmarshal([]byte(prodConfig), prodCfg)
	assert.NoError(t, err)

	rs := Routers{
		buildRouter("dosa_test", "*", "dosa_staging"),
		buildRouter("service", "*", "cl_service"),
		buildRouter("service_tier1", "*", "cl_service_tier1"),
		buildRouter("production", "another_client", "dosa_prod_a"),
		buildRouter("production", "dosa3test*", "dosa_prod_a"),
		buildRouter("production", "eternal2a", "dosa_prod_a"),
		buildRouter("production", "*", "dosa"),
		buildRouter("default", "*", "dosa_dev"),
	}
	assert.Equal(t, prodCfg.Routers, rs)
}
