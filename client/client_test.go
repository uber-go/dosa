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
)

func TestNew(t *testing.T) {
	entityPathsValid := []string{"../testentity"}
	entityPathsInvalid := []string{"/does/not/exist"}

	parseErrCfg := config.NewDefaultConfig()
	parseErrCfg.EntityPaths = entityPathsInvalid
	c, err := dosaclient.New(&parseErrCfg)
	assert.Nil(t, c)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not register")

	nilCfg := config.NewDefaultConfig()
	nilCfg.Connector = nil
	nilCfg.EntityPaths = entityPathsValid
	c, err = dosaclient.New(&nilCfg)
	assert.Nil(t, c)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "configuration is nil")

	invalidCfg := config.NewDefaultConfig()
	invalidCfg.Connector = make(map[string]interface{})
	invalidCfg.EntityPaths = entityPathsValid
	c, err = dosaclient.New(&invalidCfg)
	assert.Nil(t, c)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "must contain a string 'name' value")

	unknownCfg := config.NewDefaultConfig()
	unknownCfg.Connector["name"] = "foo"
	unknownCfg.EntityPaths = entityPathsValid
	c, err = dosaclient.New(&unknownCfg)
	assert.Nil(t, c)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "GetConnector failed for connector with name: foo")

	devnullCfg := config.NewDefaultConfig()
	devnullCfg.Connector["name"] = "devnull"
	devnullCfg.EntityPaths = entityPathsValid
	c, err = dosaclient.New(&devnullCfg)
	assert.NotNil(t, c)
	assert.NoError(t, err)

	memoryCfg := config.NewDefaultConfig()
	memoryCfg.Connector["name"] = "memory"
	memoryCfg.EntityPaths = entityPathsValid
	c, err = dosaclient.New(&memoryCfg)
	assert.NotNil(t, c)
	assert.NoError(t, err)

	randomCfg := config.NewDefaultConfig()
	randomCfg.Connector["name"] = "random"
	randomCfg.EntityPaths = entityPathsValid
	c, err = dosaclient.New(&randomCfg)
	assert.NotNil(t, c)
	assert.NoError(t, err)
}
