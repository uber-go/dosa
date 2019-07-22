// Copyright (c) 2019 Uber Technologies, Inc.
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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRoutingConfig(t *testing.T) {
	rConfig, err := NewRule("production", "test", "memory")
	assert.Nil(t, err)
	assert.Equal(t, rConfig.namePrefix, "test")
}

func TestNewRoutingConfigWithStarPrefix(t *testing.T) {
	rConfig, err := NewRule("production", "*.v1", "memory")
	assert.Nil(t, rConfig)
	assert.Contains(t, err.Error(), "invalid")
}

func TestTestNewRoutingConfigError(t *testing.T) {
	rConfig, err := NewRule("production", "", "memory")
	assert.Nil(t, rConfig)
	assert.Contains(t, err.Error(), "cannot be empty")

	rConfig, err = NewRule("", "test", "memory")
	assert.Nil(t, rConfig)
	assert.Contains(t, err.Error(), "scope cannot be empty")
}

func TestString(t *testing.T) {
	r, err := NewRule("production", "test", "memory")
	assert.Nil(t, err)
	assert.Equal(t, "{production.test -> memory}", r.String())
}

func TestCanonicalize(t *testing.T) {
	cases := []struct {
		pat     string
		cpat    string
		isScope bool
		err     bool
	}{
		{
			pat:  "foobar",
			cpat: "foobar",
		},
		{
			pat:  "fooBar",
			cpat: "foobar",
		},
		{
			pat:     "foobar",
			cpat:    "foobar",
			isScope: true,
		},
		{
			pat:     "fooBar",
			cpat:    "foobar",
			isScope: true,
		},
		{
			pat: "9foobar",
			err: true,
		},
		{
			pat: "9fooBar",
			err: true,
		},
		{
			pat:     "9foobar",
			isScope: true,
			err:     true,
		},
		{
			pat:     "9fooBar",
			isScope: true,
			err:     true,
		},
		{
			pat:  "foo.Bar",
			cpat: "foo.bar",
		},
		{
			pat:     "foo.bar",
			isScope: true,
			err:     true,
		},
		{
			pat: "foo!bar",
			err: true,
		},
		{
			pat:     "foo!bar",
			isScope: true,
			err:     true,
		},
	}
	for _, tc := range cases {
		// literal string
		cpLit, e1 := canonicalize(tc.pat, false, tc.isScope)
		if tc.err {
			assert.Error(t, e1)
		} else {
			assert.NoError(t, e1)
		}
		// * at the end
		cpGlob, e2 := canonicalize(tc.pat, true, tc.isScope)
		if tc.err {
			assert.Error(t, e2)
		} else {
			assert.NoError(t, e2)
			assert.Equal(t, tc.cpat, cpLit)
			// Check that globs sort after literals and are prefixes
			assert.True(t, cpLit < cpGlob)
			assert.True(t, strings.HasPrefix(cpGlob, cpLit))
		}
	}
}
