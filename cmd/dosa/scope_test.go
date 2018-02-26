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
				"--service", tc.serviceName,
				"--connector", "devnull",
				"scope",
				cmd,
				"scope",
			}
			main()
			assert.Equal(t, options.ServiceName, tc.expected)
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

	exit = func(r int) {
		assert.Equal(t, 0, r)
	}
	dosa.RegisterConnector("mock", func(dosa.CreationArgs) (dosa.Connector, error) {
		mc := mocks.NewMockConnector(ctrl)
		mc.EXPECT().CreateScope(gomock.Any(), gomock.Any(), gomock.Any()).Times(4).Return(nil)
		return mc, nil
	})
	os.Args = []string{"dosa", "--connector", "mock", "scope", "create", "one_scope", "two_scope", "three_scope", "four"}
	c := StartCapture()
	main()
	assert.Contains(t, c.stop(false), "\"three_scope\"")

	dosa.RegisterConnector("mock", func(dosa.CreationArgs) (dosa.Connector, error) {
		mc := mocks.NewMockConnector(ctrl)
		mc.EXPECT().CreateScope(gomock.Any(), "fred_dev", gomock.Any()).Times(1).Return(nil)
		return mc, nil
	})
	os.Args = []string{"dosa", "--connector", "mock", "scope", "create", "--owner", "fred", "fred_dev"}
	c = StartCapture()
	main()
	assert.Contains(t, c.stop(false), "\"fred_dev\"")

	dosa.RegisterConnector("mock", func(dosa.CreationArgs) (dosa.Connector, error) {
		mc := mocks.NewMockConnector(ctrl)
		mc.EXPECT().CreateScope(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(errors.New("oops"))
		return mc, nil
	})
	c = StartCapture()
	exit = func(r int) {
		assert.Equal(t, 1, r)
	}
	os.Args = []string{"dosa", "--connector", "oops", "scope", "drop", "five_fish", "six_fish", "seven_fish", "eight"}
	main()
	assert.Contains(t, c.stop(true), "oops")
}

func TestScopeDrop_Execute(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	exit = func(r int) {
		assert.Equal(t, 0, r)
	}
	c := StartCapture()
	dosa.RegisterConnector("mock", func(dosa.CreationArgs) (dosa.Connector, error) {
		mc := mocks.NewMockConnector(ctrl)
		mc.EXPECT().DropScope(gomock.Any(), gomock.Any()).Times(4).Return(nil)
		return mc, nil
	})
	os.Args = []string{"dosa", "--connector", "mock", "scope", "drop", "one_fish", "two_fish", "three_fish", "four"}
	main()
	assert.Contains(t, c.stop(false), "\"three_fish\"")

	c = StartCapture()
	exit = func(r int) {
		assert.Equal(t, 1, r)
	}
	os.Args = []string{"dosa", "--connector", "oops", "scope", "drop", "five_fish", "six_fish", "seven_fish", "eight"}
	main()
	assert.Contains(t, c.stop(true), "oops")
}

func TestScopeTruncate_Execute(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// success case
	c := StartCapture()
	exit = func(r int) {
		assert.Equal(t, 0, r)
	}
	dosa.RegisterConnector("mock", func(dosa.CreationArgs) (dosa.Connector, error) {
		mc := mocks.NewMockConnector(ctrl)
		mc.EXPECT().TruncateScope(gomock.Any(), gomock.Any()).Times(4).Return(nil)
		return mc, nil
	})
	os.Args = []string{"dosa", "--connector", "mock", "scope", "truncate", "one_fish", "two_fish", "three_fish", "four"}
	main()
	assert.Contains(t, c.stop(false), "three_fish")

	// failure case
	c = StartCapture()
	exit = func(r int) {
		assert.Equal(t, 1, r)
	}
	dosa.RegisterConnector("mock", func(dosa.CreationArgs) (dosa.Connector, error) {
		mc := mocks.NewMockConnector(ctrl)
		mc.EXPECT().TruncateScope(gomock.Any(), gomock.Any()).Times(1).Return(&dosa.ErrNotFound{})
		return mc, nil
	})
	os.Args = []string{"dosa", "--connector", "mock", "scope", "truncate", "one_fish", "two_fish", "three_fish", "four"}
	main()
	assert.Contains(t, c.stop(true), "\"one_fish\"")
}
