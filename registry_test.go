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

package dosa_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/uber-go/dosa"
)

type RegistryTestValid struct {
	dosa.Entity `dosa:"primaryKey=(ID)"`
	ID          int64
	Name        string
	Email       string
}

type RegistryTestInvalid struct {
	dosa.Entity `dosa:"primaryKey=()"`
	PrimaryKey  int64
	data        string
}

func TestNewRegistrar(t *testing.T) {
	entities := []dosa.DomainObject{&RegistryTestValid{}}

	_, err := dosa.NewRegistrar("test", "invalid-prefix", entities...)
	assert.Error(t, err)

	_, err = dosa.NewRegistrar("test", "valid.prefix", entities...)
	assert.NoError(t, err)
}

func TestRegistrar(t *testing.T) {
	validEntities := []dosa.DomainObject{&RegistryTestValid{}}
	invalidEntities := []dosa.DomainObject{&RegistryTestInvalid{}}

	r, err := dosa.NewRegistrar("test", "team.service", validEntities...)
	assert.NoError(t, err)
	_, err = dosa.NewRegistrar("test", "team.service", invalidEntities...)
	assert.Error(t, err)

	for _, e := range validEntities {
		expectedEntityName := strings.ToLower(reflect.TypeOf(e).Elem().Name())
		expectedVersion := int32(1)

		re, err := r.Find(e)
		assert.NoError(t, err)
		re.SetVersion(expectedVersion)

		info := re.EntityInfo()
		assert.Equal(t, info.Ref.Scope, "test")
		assert.Equal(t, info.Ref.NamePrefix, "team.service")
		assert.Equal(t, info.Ref.EntityName, expectedEntityName)
		assert.Equal(t, info.Ref.Version, expectedVersion)
	}

	_, err = r.Find(invalidEntities[0])
	assert.Error(t, err)

	var registered []*dosa.RegisteredEntity
	registered, err = r.FindAll()
	assert.NoError(t, err)
	assert.Equal(t, len(registered), len(validEntities))
}
