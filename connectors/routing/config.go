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
	"fmt"
	"sort"
	"strings"

	"github.com/pkg/errors"
)

// DefaultName is an alias for the glob "*" (regexp .*)
const DefaultName = "default"

////////////////////////////////////////////////////////////////////////////////////////////////////
//                            NOTE: "Router" is a synonym for "Rule".
////////////////////////////////////////////////////////////////////////////////////////////////////

// Routers represents a list of routing rules.
type Routers []*Rule

func (r Routers) Len() int {
	return len(r)
}
func (r Routers) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}
func (r Routers) Less(i, j int) bool {
	if r[i].canonScope == r[j].canonScope {
		return r[i].canonPfx < r[j].canonPfx
	}

	return r[i].canonScope < r[j].canonScope
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
				router, err := NewRule(scope, namePrefixStr, connectorName)
				if err != nil {
					return errors.Wrap(err, "failed to parse routing config")
				}
				routers = append(routers, router)
			}
		}
	}
	sort.Sort(routers)
	lastRule := routers[len(routers)-1]
	if lastRule.Scope() != "*" || lastRule.NamePrefix() != "*" {
		return errors.New("no default rule defined in the 'routers' config")
	}

	*r = routers
	return nil
}

// Config for the routing connector is a "case statement" of scope names, and each entry is a list
// of assigments "pattern" -> engine-name.
//
// Example:
//
// routers:
// - "*":
//     sless_*: schemaless
//     "*": dosa_dev
// - production:
//     serviceA: cassandra
//     serviceX: schemaless
//     *: dosa
// - development:
//     *: dosa_dev
//     serviceB: cassandra
//     serviceX: schemaless
// - ebook:
//     '*': ebook
//     apple.*: ebook
//     ebook_store: ebook
//
// A pattern is not a regular expression: only prefixes may be specified (i.e. trailing "*").
// The string "default" is a synonym for "*".
// Literal strings (no "*") sort before any pattern, i.e. "foo" < "foo*"
//
type Config struct {
	Routers Routers `yaml:"routers"`
}

// getEngineName returns the name of the engine to use for a given (scope, name-prefix). The "Routers" list
// MUST be sorted in priority order.
func (c *Config) getEngineName(scope, namePrefix string) string {
	var rule *Rule
	for _, rule = range c.Routers {
		if rule.canHandle(scope, namePrefix) {
			return rule.Destination()
		}
	}

	// The last rule in the list is the default rule, which always exists; we should never
	// reach this point.
	return "an unknown error has occurred"
}

// findDefaultRouter finds the default rule.
func (c *Config) findDefaultRule() *Rule {
	// The rules are sorted such that the default rule (i.e. *.*) is at the end.
	rule := c.Routers[len(c.Routers)-1]
	if rule.Scope() == "*" && rule.NamePrefix() == "*" {
		return rule
	}
	return nil
}

func (r *Routers) String() string {
	s := []string{}
	for _, rule := range *r {
		s = append(s, rule.String())
	}
	return "[" + strings.Join(s, ",") + "]"
}

func (c *Config) String() string {
	return c.Routers.String()
}
