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

package config

import (
	"time"

	"go.uber.org/fx/service"

	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/yarpc"
)

const (
	// configuration path in dotted notation, primarily used for UberFx
	cfgPath = "storage.dosa"

	// use dev gateway for any non-production scope
	// TODO(eculver): this switching should be done on server-side
	prodScope   = "production"
	prodService = "dosa-gateway"
	devService  = "dosa-dev-gateway"

	_defConn = "yarpc"

	// default search location and what to exclude
	_defEntityPath = "./entities/dosa"
	_defExclude    = "_test.go"

	// default timeouts in milliseconds
	_defCreateIfNotExistsTimeout = time.Duration(10 * time.Second)
	_defInitializeTimeout        = time.Duration(10 * time.Second)
	_defRangeTimeout             = time.Duration(10 * time.Second)
	_defReadTimeout              = time.Duration(2 * time.Second)
	_defRemoveTimeout            = time.Duration(2 * time.Second)
	_defScanEverythingTimeout    = time.Duration(10 * time.Second)
	_defSearchTimeout            = time.Duration(10 * time.Second)
	_defUpsertTimeout            = time.Duration(2 * time.Second)
)

// TimeoutConfig holds timeout values for all DOSA operations
type TimeoutConfig struct {
	CreateIfNotExists time.Duration `yaml:"createIfNotExists"`
	Initialize        time.Duration `yaml:"initialize"`
	Range             time.Duration `yaml:"range"`
	Read              time.Duration `yaml:"read"`
	Remove            time.Duration `yaml:"remove"`
	ScanEverything    time.Duration `yaml:"scanEverything"`
	Search            time.Duration `yaml:"search"`
	Upsert            time.Duration `yaml:"upsert"`
}

// Config represents the settings for the dosa client
type Config struct {
	Scope       string            `yaml:"scope"`
	NamePrefix  string            `yaml:"namePrefix"`
	EntityPaths []string          `yaml:"entityPaths"`
	Excludes    []string          `yaml:"excludes"`
	Connector   dosa.CreationArgs `yaml:"connector"`
	Yarpc       yarpc.Config      `yaml:"yarpc"`
	Timeout     *TimeoutConfig    `yaml:"timeout"`
}

// NewClient creates a DOSA client based on the configuration
// TODO: this should be deprecated so that we can decouple from YARPC, should
// use dosaclient.New instead
func (c Config) NewClient(entities ...dosa.DomainObject) (dosa.Client, error) {
	reg, err := dosa.NewRegistrar(c.Scope, c.NamePrefix, entities...)
	if err != nil {
		return nil, err
	}

	conn, err := yarpc.NewConnector(&c.Yarpc)
	if err != nil {
		return nil, err
	}

	return dosa.NewClient(reg, conn), nil
}

// NewDefaultConfig returns a configuration instance with all default values
func NewDefaultConfig() Config {
	return Config{
		EntityPaths: []string{_defEntityPath},
		Excludes:    []string{_defExclude},
		Connector:   map[string]interface{}{"name": _defConn},
		Timeout: &TimeoutConfig{
			CreateIfNotExists: _defCreateIfNotExistsTimeout,
			Initialize:        _defInitializeTimeout,
			Range:             _defRangeTimeout,
			Read:              _defReadTimeout,
			Remove:            _defRemoveTimeout,
			ScanEverything:    _defScanEverythingTimeout,
			Search:            _defSearchTimeout,
			Upsert:            _defUpsertTimeout,
		},
	}
}

// NewConfigForService returns a new configuration instance from an UberFx
// service's config. The configuration must live at the path "storage.dosa".
func NewConfigForService(h service.Host) (*Config, error) {
	cfg := NewDefaultConfig()
	if err := h.Config().Get(cfgPath).Populate(&cfg); err != nil {
		return nil, errors.Wrap(err, "could not populate DOSA configuration")
	}
	return &cfg, nil
}

// Service returns the YARPC service name based on the scope set. If scope is
// anything other than "production", the service name will be "dosa-dev-gateway"
func (c *Config) Service() string {
	if c.Scope == prodScope {
		return prodService
	}
	return devService
}
