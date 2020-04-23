// Copyright (c) 2020 Uber Technologies, Inc.
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

package main

import (
	"os"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/mocks"
)

func TestScope_ServiceDefault(t *testing.T) {
	tcs := []struct {
		serviceName string
		expected    string
	}{
		//  service = "" -> default
		{
			expected: _defServiceName,
		},
		//  service = "foo" -> foo
		{
			serviceName: "foo",
			expected:    "foo",
		},
	}
	for _, tc := range tcs {
		for _, cmd := range []string{"create", "drop", "truncate"} {
			os.Args = []string{
				"dosa",
			}
			if len(tc.serviceName) > 0 {
				os.Args = append(os.Args, "--service", tc.serviceName)
			}
			os.Args = append(os.Args, "scope", cmd, "scope")
			main()
			assert.Equal(t, tc.expected, options.ServiceName)
		}
	}
}

// There are 3 tests to perform on each scope operator:
// 1 - success case, should call connector once for each provided scope, displaying scope name and operation
// 2 - failure case, connector's API call generated error, provides name of scope that failed
// 3 - failure case, connector could not be initialized

func TestScope_Create(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mc := mocks.NewMockConnector(ctrl)
	mc.EXPECT().CreateScope(gomock.Any(), gomock.Any()).Times(4).Return(nil)
	mc.EXPECT().Shutdown().Return(nil)
	provideClient := func(opts GlobalOptions) (dosa.AdminClient, error) {
		return dosa.NewAdminClient(mc), nil
	}

	scopeCreate := ScopeCreate{
		ScopeCmd: &ScopeCmd{
			provideClient: provideClient,
		},
		Owner: "foo",
	}
	scopeCreate.Args.Scopes = []string{"one_scope", "two_scope", "three_scope", "four"}
	err := scopeCreate.Execute([]string{})
	assert.NoError(t, err)

	mc = mocks.NewMockConnector(ctrl)
	mc.EXPECT().CreateScope(gomock.Any(), gomock.Any()).Times(1).Return(nil)
	mc.EXPECT().Shutdown().Return(nil)

	provideClient = func(opts GlobalOptions) (dosa.AdminClient, error) {
		return dosa.NewAdminClient(mc), nil
	}
	scopeCreate = ScopeCreate{
		ScopeCmd: &ScopeCmd{
			provideClient: provideClient,
		},
		Owner: "fred",
	}
	scopeCreate.Args.Scopes = []string{"fred_dev"}

	err = scopeCreate.Execute([]string{})
	assert.NoError(t, err)

	mc = mocks.NewMockConnector(ctrl)
	mc.EXPECT().CreateScope(gomock.Any(), gomock.Any()).Times(1).Return(errors.New("oops"))
	mc.EXPECT().Shutdown().Return(nil)

	provideClient = func(opts GlobalOptions) (dosa.AdminClient, error) {
		return dosa.NewAdminClient(mc), nil
	}
	scopeCreate = ScopeCreate{
		ScopeCmd: &ScopeCmd{
			provideClient: provideClient,
		},
		Owner: "fred",
	}
	scopeCreate.Args.Scopes = []string{"fred_dev"}

	err = scopeCreate.Execute([]string{})
	assert.Error(t, err)

	scopeCreate = ScopeCreate{
		ScopeCmd: &ScopeCmd{
			provideClient: provideClient,
		},
	}
	scopeCreate.Args.Scopes = []string{"no_owner"}

	err = scopeCreate.Execute([]string{})
	assert.Contains(t, err.Error(), "--owner")
}

func TestScopeDrop_Execute(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mc := mocks.NewMockConnector(ctrl)
	mc.EXPECT().DropScope(gomock.Any(), gomock.Any()).Times(4).Return(nil)
	mc.EXPECT().Shutdown().Return(nil)
	provideClient := func(opts GlobalOptions) (dosa.AdminClient, error) {
		return dosa.NewAdminClient(mc), nil
	}
	scopeDrop := ScopeDrop{
		ScopeCmd: &ScopeCmd{
			provideClient: provideClient,
		},
	}
	scopeDrop.Args.Scopes = []string{"one_fish", "two_fish", "three_fish", "four"}
	err := scopeDrop.Execute([]string{})
	assert.NoError(t, err)
}

func TestScopeTruncate_Execute(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// success case
	mc := mocks.NewMockConnector(ctrl)
	mc.EXPECT().TruncateScope(gomock.Any(), gomock.Any()).Times(4).Return(nil)
	mc.EXPECT().Shutdown().Return(nil)
	provideClient := func(opts GlobalOptions) (dosa.AdminClient, error) {
		return dosa.NewAdminClient(mc), nil
	}

	scopeTruncate := ScopeTruncate{
		ScopeCmd: &ScopeCmd{
			provideClient: provideClient,
		},
	}
	scopeTruncate.Args.Scopes = []string{"one_fish", "two_fish", "three_fish", "four"}
	err := scopeTruncate.Execute([]string{})
	assert.NoError(t, err)

	// failure case
	mc = mocks.NewMockConnector(ctrl)
	mc.EXPECT().TruncateScope(gomock.Any(), gomock.Any()).Times(1).Return(&dosa.ErrNotFound{})
	mc.EXPECT().Shutdown().Return(nil)

	provideClient = func(opts GlobalOptions) (dosa.AdminClient, error) {
		return dosa.NewAdminClient(mc), nil
	}
	scopeTruncate = ScopeTruncate{
		ScopeCmd: &ScopeCmd{
			provideClient: provideClient,
		},
	}
	scopeTruncate.Args.Scopes = []string{"one_fish", "two_fish", "three_fish", "four"}
	err = scopeTruncate.Execute([]string{})
	assert.Error(t, err)
}

func TestParseType(t *testing.T) {
	// Prefixes are OK
	var typ dosa.ScopeType
	var err error

	typ, err = parseType("dev")
	assert.Nil(t, err)
	assert.Equal(t, dosa.Development, typ)

	typ, err = parseType("dEv")
	assert.Nil(t, err)
	assert.Equal(t, dosa.Development, typ)

	typ, err = parseType("development")
	assert.Nil(t, err)
	assert.Equal(t, dosa.Development, typ)

	typ, err = parseType("prod")
	assert.Nil(t, err)
	assert.Equal(t, dosa.Production, typ)

	typ, err = parseType("Prod")
	assert.Nil(t, err)
	assert.Equal(t, dosa.Production, typ)

	typ, err = parseType("PROD")
	assert.Nil(t, err)
	assert.Equal(t, dosa.Production, typ)

	typ, err = parseType("production")
	assert.Nil(t, err)
	assert.Equal(t, dosa.Production, typ)

	typ, err = parseType("int")
	assert.NotNil(t, err)

	// Typos
	typ, err = parseType("probuction")
	assert.NotNil(t, err)

	typ, err = parseType("deve1opment")
	assert.NotNil(t, err)
}
