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
	"fmt"
	"sort"

	"github.com/pkg/errors"
)

// DefaultScope represents the default scope
const DefaultScope = "default"

// Routers represents a list of routing config
type Routers []*Rule

func (r Routers) Len() int {
	return len(r)
}
func (r Routers) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}
func (r Routers) Less(i, j int) bool {
	if r[i].Scope == r[j].Scope {
		return r[i].NamePrefix > r[j].NamePrefix
	}

	return r[i].Scope > r[j].Scope
}

// UnmarshalYAML unmarshals the config into gocql cluster config
func (r *Routers) UnmarshalYAML(unmarshal func(interface{}) error) error {
	routers := make(Routers, 0)
	scopes := make([]map[string]interface{}, 0)
	if err := unmarshal(&scopes); err != nil {
		return err
	}
	defaultRouterExist := false
	for _, scopeMap := range scopes {
		for scope, namePrefixes := range scopeMap {
			namePrefixesMap, ok := namePrefixes.(map[interface{}]interface{})
			if !ok {
				return fmt.Errorf("failed to parse the config: %v", namePrefixes)
			}

			for namePrefix, connector := range namePrefixesMap {
				namePrefixStr := namePrefix.(string)
				connectorName, ok := connector.(string)
				if !ok {
					return fmt.Errorf("failed to parse the config: %v", namePrefixesMap)
				}
				router, err := NewRule(scope, namePrefixStr, connectorName)
				if err != nil {
					return errors.Wrap(err, "failed to parse routing config")
				}
				routers = append(routers, router)
				if scope == DefaultScope {
					defaultRouterExist = true
				}
			}
		}
	}

	if !defaultRouterExist {
		return errors.New("there should be a default scope defined in routing config yaml file")
	}

	sort.Sort(routers)
	*r = routers
	return nil
}

// Config is a struct contains fields from yaml
// scope should be an exact string in any case,
// namePrefix could be in 3 different format:
// 1. exact string like "service" that matches namePrefix that is exactly "service"
// 2. partial glob match like "service.*" that matches all namePrefix that has a prefix of "service."
// 3. full glob match like "*" that matches all namePrefix
type Config struct {
	Routers Routers `yaml:"routers"`
}

// FindRouter finds the router information based on scope and namePrefix.
func (c *Config) FindRouter(scope, namePrefix string) *Rule {
	for _, router := range c.Routers {
		if router.RouteTo(scope, namePrefix) {
			return router
		}
	}

	return c.findDefaultRouter()
}

// findDefaultRouter finds the default router information.
func (c *Config) findDefaultRouter() *Rule {
	for _, router := range c.Routers {
		if router.Scope == DefaultScope {
			return router
		}
	}
	return nil
}
