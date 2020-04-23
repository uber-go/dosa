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
	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
)

// simpleRegistrar is a simplified registrar only used by shell query client
type simpleRegistrar struct {
	scope      string
	namePrefix string
	entity     *dosa.RegisteredEntity
}

// newSimpleRegistrar creates a new simpleRegistrar
func newSimpleRegistrar(scope, namePrefix string, table *dosa.Table) (dosa.Registrar, error) {
	if err := dosa.IsValidNamePrefix(namePrefix); err != nil {
		return nil, errors.Wrap(err, "failed to construct Registrar")
	}

	re := dosa.NewRegisteredEntity(scope, namePrefix, table)

	return &simpleRegistrar{
		scope:      scope,
		namePrefix: namePrefix,
		entity:     re,
	}, nil
}

// Scope returns the registrar's scope
func (r *simpleRegistrar) Scope() string {
	return r.scope
}

// NamePrefix returns the registrar's prefix
func (r *simpleRegistrar) NamePrefix() string {
	return r.namePrefix
}

// Find returns the embedded entity
func (r *simpleRegistrar) Find(entity dosa.DomainObject) (*dosa.RegisteredEntity, error) {
	if r.entity == nil {
		return nil, errors.New("no entity found in registrar")
	}

	return r.entity, nil
}

// FindAll returns the embedded entity in a slice
func (r *simpleRegistrar) FindAll() []*dosa.RegisteredEntity {
	return []*dosa.RegisteredEntity{r.entity}
}
