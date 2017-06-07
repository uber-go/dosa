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

package registry_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/config"
	"github.com/uber-go/dosa/registry"
	// "github.com/uber-go/dosa/testentity"
)

var (
	testScope  = "testscope"
	testPrefix = "testprefix"
)

type TestEntity1 struct {
	dosa.Entity `dosa:"primaryKey=(UUID)"`
	UUID        dosa.UUID
}

type TestEntity2 struct {
	dosa.Entity `dosa:"primaryKey=(UUID, Name)"`
	UUID        int64
	Name        string
	Email       string
}

func TestRegistrar_Scope(t *testing.T) {
	cfg := &config.Config{
		Scope: testScope,
	}
	reg, err := registry.NewRegistrar(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, reg)
	assert.Equal(t, reg.Scope(), testScope)
}

func TestRegistrar_NamePrefix(t *testing.T) {
	cfg := &config.Config{
		NamePrefix: testPrefix,
	}
	reg, err := registry.NewRegistrar(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, reg)
	assert.Equal(t, reg.NamePrefix(), testPrefix)
}

func TestRegistrar_Find(t *testing.T) {
	cfg := &config.Config{
		Scope:      testScope,
		NamePrefix: testPrefix,
	}
	reg, err := registry.NewRegistrar(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, reg)

	// not found
	re, err := reg.Find(&TestEntity1{})
	assert.Error(t, err)

	// found
	cfg.EntityPaths = []string{"."}
	reg, err = registry.NewRegistrar(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, reg)
	re, err = reg.Find(&TestEntity1{})
	assert.NoError(t, err)
	assert.NotNil(t, re)
}

func TestRegistrar_FindAll(t *testing.T) {
	cfg := &config.Config{
		Scope:      testScope,
		NamePrefix: testPrefix,
	}
	reg, err := registry.NewRegistrar(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, reg)

	// not found
	res, err := reg.FindAll()
	assert.Error(t, err)

	// found 1
	cfg.EntityPaths = []string{"../testentity"}
	reg, err = registry.NewRegistrar(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, reg)
	res, err = reg.FindAll()
	assert.NoError(t, err)
	assert.Equal(t, len(res), 1)

	// found many
	cfg.EntityPaths = []string{".", "../testentity"}
	reg, err = registry.NewRegistrar(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, reg)
	res, err = reg.FindAll()
	assert.NoError(t, err)
	assert.Equal(t, len(res), 3)
}

func TestNewRegistrar(t *testing.T) {
	cases := []struct {
		scope  string
		prefix string
		paths  []string
		err    string
	}{
		// invalid prefix
		{
			prefix: "--",
			err:    "failed to construct Registrar",
		},
		// directory not found
		{
			prefix: testPrefix,
			paths:  []string{"/foo/bar/baz"},
			err:    "no such file or directory",
		},
		// parse errors
		{
			prefix: testPrefix,
			paths:  []string{".."},
			err:    "entities had warnings/errors",
		},
		// ok
		{
			scope:  testScope,
			prefix: testPrefix,
			paths:  []string{"../testentity"},
		},
	}
	for _, c := range cases {
		cfg := &config.Config{
			Scope:       c.scope,
			NamePrefix:  c.prefix,
			EntityPaths: c.paths,
		}
		reg, err := registry.NewRegistrar(cfg)
		if c.err != "" {
			assert.Contains(t, err.Error(), c.err)
			continue
		}
		assert.NoError(t, err)
		assert.NotNil(t, reg)
	}

}
