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
	"strings"
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
    ebook_store: ebook4
- ebook*:
    foo: ebook-foo
    foo*: ebook-foo-2
    '*': ebook.42
- default:
    foo_*: dosa1
    default: dosa2
`

// TestBasicConfig test the basic yaml file conversion
func TestBasicConfig(t *testing.T) {
	testCfg := &Config{}
	err := yaml.Unmarshal([]byte(yamlFile), testCfg)
	assert.NoError(t, err)
	rs := Routers{
		buildRule("development", "serviceB", "cassandra4"),
		buildRule("development", "default", "cassandra3"),
		buildRule("ebook", "app", "ebook0"),
		buildRule("ebook", "apple.*", "ebook2"),
		buildRule("ebook", "ebook_store", "ebook4"),
		buildRule("ebook", "*", "ebook1"),
		buildRule("ebook*", "foo", "ebook-foo"),
		buildRule("ebook*", "foo*", "ebook-foo-2"),
		buildRule("ebook*", "*", "ebook.42"),
		buildRule("production", "serviceA", "cassandra2"),
		buildRule("production", "*", "cassandra1"),
		buildRule("default", "foo_*", "dosa1"),
		buildRule("default", "default", "dosa2"),
	}
	assert.Equal(t, testCfg.Routers, rs)

	err = yaml.Unmarshal([]byte(`bad yaml file`), testCfg)
	assert.Error(t, err)

	s := []string{
		"{development.serviceB -> cassandra4}",
		"{development.* -> cassandra3}",
		"{ebook.app -> ebook0}",
		"{ebook.apple.* -> ebook2}",
		"{ebook.ebook_store -> ebook4}",
		"{ebook.* -> ebook1}",
		"{ebook*.foo -> ebook-foo}",
		"{ebook*.foo* -> ebook-foo-2}",
		"{ebook*.* -> ebook.42}",
		"{production.serviceA -> cassandra2}",
		"{production.* -> cassandra1}",
		"{*.foo_* -> dosa1}",
		"{*.* -> dosa2}",
	}

	assert.Equal(t, "["+strings.Join(s, ",")+"]", rs.String())
}

func buildRule(scope, namePrefix, connector string) *Rule {
	rc, _ := NewRule(scope, namePrefix, connector)
	return rc
}

func TestRouting(t *testing.T) {
	testCfg := &Config{}
	err := yaml.Unmarshal([]byte(yamlFile), testCfg)
	assert.NoError(t, err)

	rule := testCfg.findDefaultRule()
	assert.Equal(t, rule.Scope(), "*")

	var eng string

	eng = testCfg.getEngineName("production", "serviceA")
	assert.Equal(t, eng, "cassandra2")

	eng = testCfg.getEngineName("production", "something_else")
	assert.Equal(t, eng, "cassandra1")

	eng = testCfg.getEngineName("development", "serviceA")
	assert.Equal(t, eng, "cassandra3")

	eng = testCfg.getEngineName("ebook", "app")
	assert.Equal(t, eng, "ebook0")

	eng = testCfg.getEngineName("ebook", "apple.k")
	assert.Equal(t, eng, "ebook2")

	eng = testCfg.getEngineName("ebook", "apple2")
	assert.Equal(t, eng, "ebook1")

	eng = testCfg.getEngineName("ebook", "d.k")
	assert.Equal(t, eng, "ebook1")

	eng = testCfg.getEngineName("ebook", "foo")
	assert.Equal(t, eng, "ebook1")

	eng = testCfg.getEngineName("ebook2", "foo")
	assert.Equal(t, eng, "ebook-foo")

	eng = testCfg.getEngineName("ebook2", "app")
	assert.Equal(t, eng, "ebook.42")

	eng = testCfg.getEngineName("ebook2", "foo_bar")
	assert.Equal(t, eng, "ebook-foo-2")

	eng = testCfg.getEngineName("ebook_bar", "baz")
	assert.Equal(t, eng, "ebook.42")

	eng = testCfg.getEngineName("dev_user2", "foo_bar")
	assert.Equal(t, eng, "dosa1")

	eng = testCfg.getEngineName("a", "d.k")
	assert.Equal(t, eng, "dosa2")
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
		buildRule("dosa_test", "*", "dosa_staging"),
		buildRule("production", "another_client*", "dosa_prod_a"),
		buildRule("production", "dosa3test*", "dosa_prod_a"),
		buildRule("production", "eternal2a", "dosa_prod_a"),
		buildRule("production", "*", "dosa"),
		buildRule("service", "*", "cl_service"),
		buildRule("service_tier1", "*", "cl_service_tier1"),
		buildRule("default", "*", "dosa_dev"),
	}
	assert.Equal(t, prodCfg.Routers, rs)
}
