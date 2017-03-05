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
	dosa.Entity `dosa:"primaryKey=(ID, Name)"`
	ID          int64
	Name        string
	Email       string
}

type RegistryTestInvalid struct {
	dosa.Entity `dosa:"primaryKey=()"`
	PrimaryKey  int64
	data        string
}

func TestNewRegisteredEntity(t *testing.T) {
	table, _ := dosa.TableFromInstance(&RegistryTestValid{})
	scope := "test"
	namePrefix := "team.service"
	entityName := "registrytestvalid"
	version := int32(12)

	re := dosa.NewRegisteredEntity(scope, namePrefix, table)
	assert.NotNil(t, re)

	info := re.EntityInfo()
	assert.NotNil(t, info)

	ref := re.SchemaRef()
	assert.NotNil(t, ref)

	def := re.EntityDefinition()
	assert.NotNil(t, def)

	re.SetVersion(version)
	assert.Equal(t, ref.Scope, scope)
	assert.Equal(t, ref.NamePrefix, namePrefix)
	assert.Equal(t, ref.EntityName, entityName)
	assert.Equal(t, ref.Version, version)
	assert.Equal(t, def.Name, entityName)
}

func TestRegisteredEntity_KeyFieldValues(t *testing.T) {
	entity := &RegistryTestValid{
		ID:    int64(1),
		Name:  "foo",
		Email: "foo@email.com",
	}
	scope := "test"
	namePrefix := "team.service"
	table, _ := dosa.TableFromInstance(entity)
	re := dosa.NewRegisteredEntity(scope, namePrefix, table)

	// invalid primary key
	assert.Panics(t, func() {
		re.KeyFieldValues(&RegistryTestInvalid{PrimaryKey: 1})
	})

	// valid
	fieldValues := re.KeyFieldValues(entity)
	expected := map[string]dosa.FieldValue{
		"id":   int64(1),
		"name": "foo",
	}
	assert.Equal(t, fieldValues, expected)
}

func TestRegisteredEntity_ColumnNames(t *testing.T) {
	entity := &RegistryTestValid{
		ID:    int64(1),
		Name:  "foo",
		Email: "foo@email.com",
	}
	scope := "test"
	namePrefix := "team.service"
	table, _ := dosa.TableFromInstance(entity)
	re := dosa.NewRegisteredEntity(scope, namePrefix, table)

	// empty, just return an empty set
	columnNames, err := re.ColumnNames([]string{})
	assert.Empty(t, err)

	// invalid
	columnNames, err = re.ColumnNames([]string{"ID", "foo"})
	assert.Error(t, err)

	// valid
	columnNames, err = re.ColumnNames([]string{"ID", "Name"})
	assert.NoError(t, err)
	assert.Equal(t, columnNames, []string{"id", "name"})
}

func TestRegisteredEntity_SetFieldValues(t *testing.T) {
	entity := &RegistryTestValid{
		ID:    int64(1),
		Name:  "foo",
		Email: "foo@email.com",
	}
	scope := "test"
	namePrefix := "team.service"
	table, _ := dosa.TableFromInstance(entity)
	re := dosa.NewRegisteredEntity(scope, namePrefix, table)
	validFieldValues := map[string]dosa.FieldValue{
		"id":    int64(2),
		"name":  "bar",
		"email": "bar@email.com",
	}
	invalidFieldValues := map[string]dosa.FieldValue{
		"id":      int64(2),
		"name":    "bar",
		"invalid": "invalid",
	}

	// invalid entity
	assert.Panics(t, func() {
		re.SetFieldValues(&RegistryTestInvalid{PrimaryKey: 1}, validFieldValues)
	})

	// invalid values
	assert.Error(t, re.SetFieldValues(entity, invalidFieldValues))

	// valid
	assert.NoError(t, re.SetFieldValues(entity, validFieldValues))
	assert.Equal(t, entity.ID, validFieldValues["id"])
	assert.Equal(t, entity.Name, validFieldValues["name"])
	assert.Equal(t, entity.Email, validFieldValues["email"])
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
		entityName := strings.ToLower(reflect.TypeOf(e).Elem().Name())
		version := int32(1)

		re, err := r.Find(e)
		assert.NoError(t, err)
		re.SetVersion(version)

		info := re.EntityInfo()
		assert.Equal(t, info.Ref.Scope, r.Scope())
		assert.Equal(t, info.Ref.NamePrefix, r.NamePrefix())
		assert.Equal(t, info.Ref.EntityName, entityName)
		assert.Equal(t, info.Ref.Version, version)
	}

	_, err = r.Find(invalidEntities[0])
	assert.Error(t, err)

	var registered []*dosa.RegisteredEntity
	registered, err = r.FindAll()
	assert.NoError(t, err)
	assert.Equal(t, len(registered), len(validEntities))
}
