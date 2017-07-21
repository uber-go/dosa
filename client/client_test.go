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

package dosaclient_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa/client"
	"github.com/uber-go/dosa/config"
	_ "github.com/uber-go/dosa/connectors/devnull"
	_ "github.com/uber-go/dosa/connectors/memory"
	_ "github.com/uber-go/dosa/connectors/random"
	"github.com/uber-go/dosa/testentity"
)

func TestNew(t *testing.T) {
	regErrCfg := config.NewDefaultConfig()
	regErrCfg.NamePrefix = "1invalid"
	c, err := dosaclient.New(&regErrCfg)
	assert.Nil(t, c)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not register")

	nilCfg := config.NewDefaultConfig()
	nilCfg.Connector = nil
	c, err = dosaclient.New(&nilCfg, &testentity.TestEntity{})
	assert.Nil(t, c)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "configuration is nil")

	invalidCfg := config.NewDefaultConfig()
	invalidCfg.Connector = make(map[string]interface{})
	c, err = dosaclient.New(&invalidCfg, &testentity.TestEntity{})
	assert.Nil(t, c)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "must contain a string 'name' value")

	unknownCfg := config.NewDefaultConfig()
	unknownCfg.Connector["name"] = "foo"
	c, err = dosaclient.New(&unknownCfg, &testentity.TestEntity{})
	assert.Nil(t, c)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "GetConnector failed for connector with name: foo")

	devnullCfg := config.NewDefaultConfig()
	devnullCfg.Connector["name"] = "devnull"
	c, err = dosaclient.New(&devnullCfg, &testentity.TestEntity{})
	assert.NotNil(t, c)
	assert.NoError(t, err)

	memoryCfg := config.NewDefaultConfig()
	memoryCfg.Connector["name"] = "memory"
	c, err = dosaclient.New(&memoryCfg, &testentity.TestEntity{})
	assert.NotNil(t, c)
	assert.NoError(t, err)

	randomCfg := config.NewDefaultConfig()
	randomCfg.Connector["name"] = "random"
	c, err = dosaclient.New(&randomCfg, &testentity.TestEntity{})
	assert.NotNil(t, c)
	assert.NoError(t, err)
}
