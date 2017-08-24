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

package config_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/config"
	"github.com/uber-go/dosa/connectors/yarpc"
)

const (
	testScope      = "testscope"
	testPrefix     = "testprefix"
	testEntityPath = "../testentity"
)

// SinglePartitionKey is used to test NewClient
type SinglePartitionKey struct {
	dosa.Entity `dosa:"primaryKey=PrimaryKey"`
	PrimaryKey  int64
	data        string
}

func TestConfig_NewClient(t *testing.T) {
	cases := []struct {
		cfg   config.Config
		isErr bool
	}{
		{
			// success: yarpc
			cfg: config.Config{
				Scope:      "test",
				NamePrefix: "namePrefix",
				Yarpc: &yarpc.Config{
					Transport:   "http",
					Host:        "localhost",
					Port:        "8080",
					CallerName:  "dosa-test",
					ServiceName: "dosa-gateway",
				},
			},
			isErr: false,
		},
		{
			// success: memory
			cfg: config.Config{
				Scope:      "test",
				NamePrefix: "namePrefix",
				Connector: dosa.CreationArgs{
					"name": "memory",
				},
			},
			isErr: false,
		},
		{
			// no connector
			cfg: config.Config{
				Scope:      "test",
				NamePrefix: "prefix",
			},
			isErr: true,
		},
		{
			// unknown connector
			cfg: config.Config{
				Scope:      "test",
				NamePrefix: "prefix",
				Connector: dosa.CreationArgs{
					"name": "foo",
				},
			},
			isErr: true,
		},
		{
			// invalid connector args
			cfg: config.Config{
				Scope:      "test",
				NamePrefix: "prefix",
				Connector: dosa.CreationArgs{
					"foo": "bar",
				},
			},
			isErr: true,
		},
		{
			// registrar fail
			cfg: config.Config{
				Scope:      "test",
				NamePrefix: "name*(Prefix",
				Yarpc: &yarpc.Config{
					Transport:   "http",
					Host:        "localhost",
					Port:        "8080",
					CallerName:  "dosa-test",
					ServiceName: "dosa-gateway",
				},
			},
			isErr: true,
		},
		{
			// yarpc fail
			cfg: config.Config{
				Scope:      "test",
				NamePrefix: "name*(Prefix",
				Yarpc: &yarpc.Config{
					Transport: "http",
					Host:      "localhost",
					Port:      "8080",
				},
			},
			isErr: true,
		},
	}

	for _, c := range cases {
		_, err := c.cfg.NewClient(&SinglePartitionKey{})
		if c.isErr {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}
}

func TestConfig_NewDefaultConfig(t *testing.T) {
	c := config.NewDefaultConfig()
	assert.NotNil(t, c.Connector)
	assert.NotNil(t, c.Timeout)
	assert.NotNil(t, c.Timeout.CreateIfNotExists)
	assert.NotNil(t, c.Timeout.Initialize)
	assert.NotNil(t, c.Timeout.Range)
	assert.NotNil(t, c.Timeout.Read)
	assert.NotNil(t, c.Timeout.Remove)
	assert.NotNil(t, c.Timeout.ScanEverything)
	assert.NotNil(t, c.Timeout.Search)
	assert.NotNil(t, c.Timeout.Upsert)
}
