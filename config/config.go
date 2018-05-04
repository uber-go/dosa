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

	"github.com/pkg/errors"
	"github.com/uber-go/dosa"

	// import to invoke all connector init methods
	_ "github.com/uber-go/dosa/connectors/devnull"
	_ "github.com/uber-go/dosa/connectors/memory"
	_ "github.com/uber-go/dosa/connectors/random"
	"github.com/uber-go/dosa/connectors/yarpc"
)

const (
	_defConn = "yarpc"

	// default timeouts in milliseconds
	_defCreateIfNotExistsTimeout = time.Duration(10 * time.Second)
	_defInitializeTimeout        = time.Duration(10 * time.Second)
	_defRangeTimeout             = time.Duration(10 * time.Second)
	_defReadTimeout              = time.Duration(2 * time.Second)
	_defRemoveTimeout            = time.Duration(2 * time.Second)
	_defScanEverythingTimeout    = time.Duration(10 * time.Second)
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
	Upsert            time.Duration `yaml:"upsert"`
}

// Config represents the settings for the dosa client
type Config struct {
	Scope      string            `yaml:"scope"`
	NamePrefix string            `yaml:"namePrefix"`
	Connector  dosa.CreationArgs `yaml:"connector"`
	Yarpc      *yarpc.Config     `yaml:"yarpc"`
	Timeout    *TimeoutConfig    `yaml:"timeout"`
}

// NewClient creates a DOSA client based on the configuration
func (c Config) NewClient(entities ...dosa.DomainObject) (dosa.Client, error) {
	reg, err := dosa.NewRegistrar(c.Scope, c.NamePrefix, entities...)
	if err != nil {
		return nil, err
	}

	// for backwards compatibility
	if c.Yarpc != nil {
		c.Connector = dosa.CreationArgs{
			"name":        "yarpc",
			"host":        c.Yarpc.Host,
			"port":        c.Yarpc.Port,
			"callername":  c.Yarpc.CallerName,
			"servicename": c.Yarpc.ServiceName,
		}
	}

	if c.Connector == nil {
		return nil, errors.Errorf("Connector configuration is nil")
	}

	args := dosa.CreationArgs(c.Connector)
	connName, ok := args["name"].(string)
	if !ok {
		return nil, errors.Errorf("Connector config must contain a string 'name' value (%v)", args)
	}

	// create connector with args
	conn, err := dosa.GetConnector(connName, args)
	if err != nil {
		return nil, errors.Wrapf(err, "GetConnector failed for connector with name: %v", connName)
	}

	return dosa.NewClient(reg, conn), nil
}

// NewDefaultConfig returns a configuration instance with all default values
func NewDefaultConfig() Config {
	return Config{
		Connector: map[string]interface{}{"name": _defConn},
		Timeout: &TimeoutConfig{
			CreateIfNotExists: _defCreateIfNotExistsTimeout,
			Initialize:        _defInitializeTimeout,
			Range:             _defRangeTimeout,
			Read:              _defReadTimeout,
			Remove:            _defRemoveTimeout,
			ScanEverything:    _defScanEverythingTimeout,
			Upsert:            _defUpsertTimeout,
		},
	}
}
