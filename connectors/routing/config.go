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
	"github.com/pkg/errors"
	"sort"
)

type Routers []*RoutingConfig

func (rc Routers) Len() int {
	return len(rc)
}
func (rc Routers) Swap(i, j int) {
	rc[i], rc[j] = rc[j], rc[i]
}
func (rc Routers) Less(i, j int) bool {
	if rc[i].Scope == rc[j].Scope {
		return rc[i].NamePrefix > rc[j].NamePrefix
	}

	return rc[i].Scope > rc[j].Scope
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

// UnmarshalYAML unmarshals the config into gocql cluster config
func (r *Routers) UnmarshalYAML(unmarshal func(interface{}) error) error {
	routers := make(Routers, 0)
	scopes := make([]map[string]interface{}, 0)
	if err := unmarshal(&scopes); err != nil {
		return err
	}

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
				router, err := NewRoutingConfig(scope, namePrefixStr, connectorName)
				if err != nil {
					return errors.Wrap(err, "failed to parse routing config")
				}
				routers = append(routers, router)
			}
		}
	}
	sort.Sort(routers)
	*r = routers
	return nil
}

// FindRouter finds the router information based on scope and namePrefix.
func (c *Config) FindRouter(scope, namePrefix string) (*RoutingConfig, error) {
	for _, router := range c.Routers {
		if router.RouteTo(scope, namePrefix) {
			return router, nil
		}
	}

	return nil, fmt.Errorf("can't find any router info for scope %s, namePrefix %s", scope, namePrefix)
}


// FindDefaultRouter finds the default router information.
func (c *Config) FindDefaultRouter() (*RoutingConfig, error) {
	for _, router := range c.Routers {
		if router.Scope == DefaultScope {
			return router, nil
		}
	}
	return nil, errors.New("there should be a default scope defined in routing config yaml file")
}
