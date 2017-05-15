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

// TODO(eculver): consider supporting variadic options args to `dosa.New`

package config

import (
	"github.com/uber-go/dosa/connectors/yarpc"
)

// Option ...
type Option func(cfg *Config) error

// WithScope ...
func WithScope(scopeName string) Option {
	return func(cfg *Config) error {
		cfg.Scope = scopeName
		return nil
	}
}

// WithNamePrefix ...
func WithNamePrefix(namePrefix string) Option {
	return func(cfg *Config) error {
		cfg.NamePrefix = namePrefix
		return nil
	}
}

// WithEntityPath ...
func WithEntityPath(pathName string) Option {
	return func(cfg *Config) error {
		cfg.EntityPaths = append(cfg.EntityPaths, pathName)
		return nil
	}
}

// ExcludingEntities ...
func ExcludingEntities(exclude string) Option {
	return func(cfg *Config) error {
		cfg.Excludes = append(cfg.Excludes, exclude)
		return nil
	}
}

// WithYARPC ...
func WithYARPC(ycfg yarpc.Config) Option {
	return func(cfg *Config) error {
		cfg.Yarpc = ycfg
		return nil
	}
}

// WithTimeouts ...
func WithTimeouts(tcfg *TimeoutConfig) Option {
	return func(cfg *Config) error {
		cfg.Timeout = tcfg
		return nil
	}
}
