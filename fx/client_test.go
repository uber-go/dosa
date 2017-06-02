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

package dosafx_test

import (
	"testing"

	fxconfig "go.uber.org/fx/config"
	"go.uber.org/fx/service"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/tchannel"
	"go.uber.org/yarpc/yarpctest/recorder"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/config"
	_ "github.com/uber-go/dosa/connectors/devnull"
	"github.com/uber-go/dosa/fx"
)

type InvalidEntity struct {
	dosa.Entity `dosa:"primaryKey="`
	PrimaryKey  int64
	data        string
}

type ValidEntity struct {
	dosa.Entity `dosa:"primaryKey=PrimaryKey"`
	PrimaryKey  int64
	data        string
}

func getTestDispatcher(t *testing.T) *yarpc.Dispatcher {
	ts, _ := tchannel.NewChannelTransport(tchannel.ServiceName("dosatest"))
	ycfg := yarpc.Config{
		Name: "service",
		Outbounds: yarpc.Outbounds{
			"dosa-dev-gateway": {
				Unary: ts.NewSingleOutbound("127.0.0.1:21300"),
			},
		},
		OutboundMiddleware: yarpc.OutboundMiddleware{
			Unary: recorder.NewRecorder(t),
		},
	}
	return yarpc.NewDispatcher(ycfg)
}

func TestFx(t *testing.T) {
	scope := "test"
	prefix := "dosa.test"
	connector := "devnull"

	dosaCfg := config.NewDefaultConfig()
	dosaCfg.Scope = scope
	dosaCfg.NamePrefix = prefix
	dosaCfg.Connector["name"] = connector

	testCfgErr := map[string]interface{}{
		"storage": map[string]interface{}{
			"dosa": dosaCfg,
		},
	}
	testCfgErrProvider := fxconfig.NewStaticProvider(testCfgErr)

	dosaCfg.EntityPaths = []string{"../testentity"}

	testCfg := map[string]interface{}{
		"storage": map[string]interface{}{
			"dosa": config.Config(dosaCfg),
		},
	}
	testCfgProvider := fxconfig.NewStaticProvider(testCfg)
	tcs := []struct {
		dispatcher *yarpc.Dispatcher
		host       service.Host
		err        string
	}{
		{
			dispatcher: getTestDispatcher(t),
			host:       service.NopHostWithConfig(testCfgErrProvider),
			err:        "could not register",
		},
		{
			dispatcher: getTestDispatcher(t),
			host:       service.NopHostWithConfig(testCfgProvider),
		},
	}

	for _, tc := range tcs {
		c, err := dosafx.New(tc.dispatcher, tc.host)
		if tc.err != "" {
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.err)
			assert.Nil(t, c)
			continue
		}
		assert.NoError(t, err)
		assert.NotNil(t, c)
	}
}
