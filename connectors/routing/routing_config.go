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
	"strings"

	"github.com/gobwas/glob"
	"github.com/pkg/errors"
)

// RoutingConfig implements routing method
// to decide which connector the RoutingConnector talks to
type RoutingConfig struct {
	Scope      string
	NamePrefix string
	Connector string
	GlobMatch  glob.Glob
}

// NewRoutingConfig initializes RoutingConfig
func NewRoutingConfig(scope, namePrefix, connector string) (*RoutingConfig, error) {
	if namePrefix == "" {
		return nil, errors.New("namePrefix could not be empty, should be defined in yaml file")
	}

	if scope == "" {
		return nil, errors.New("scope could not be empty, should be defined in yaml file")
	}

	if strings.HasPrefix(namePrefix, "*") && len(namePrefix) != 1 {
		// we don't support name like '*.service.v1'
		return nil, errors.Errorf(
			"namePrefix like %v is not supported, cannot put * at the beginning of namePrefix.", namePrefix)
	}

	globMatch := glob.MustCompile(namePrefix)

	return &RoutingConfig{NamePrefix: namePrefix, Scope: scope, Connector: connector, GlobMatch: globMatch}, nil
}

// RouteTo implements the method to choose the matched connector
func (rc *RoutingConfig) RouteTo(scope string, namePrefix string) bool {
	// scope should be an exact match
	if rc.Scope != scope {
		return false
	}

	// namePrefix could be glob match, but first check if there's an exact match
	if rc.NamePrefix == namePrefix {
		return true
	}

	if rc.GlobMatch.Match(namePrefix) {
		return true
	}

	return false
}
