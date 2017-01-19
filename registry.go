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

package dosa

import (
	"reflect"

	"github.com/pkg/errors"
)

// Registrar is the interface to register DOSA entities.
type Registrar interface {
	Register(...DomainObject) error
	lookupByFQN(FQN) (*Table, error)
	lookupByType(DomainObject) (*Table, FQN, error)
}

// prefixedRegistrar puts every entity under a prefix.
// This registrar is not threadsafe. However, Register step is done in bootstrap phase (usually with a single thread),
// and after bootstrap multiple goroutines can safely read from this registrar.
type prefixedRegistrar struct {
	prefix    FQN
	fqnIndex  map[FQN]*Table
	typeIndex map[reflect.Type]FQN
}

// NewRegistrar creates a Registrar with desired FQN prefix.
// All registered entities would be put under this FQN prefix.
func NewRegistrar(fqnPrefix string) (Registrar, error) {
	prefix, err := ToFQN(fqnPrefix)
	if err != nil {
		return nil, errors.Wrap(err, "failed to construct Registrar")
	}
	return &prefixedRegistrar{
		prefix:    prefix,
		fqnIndex:  make(map[FQN]*Table),
		typeIndex: make(map[reflect.Type]FQN),
	}, nil
}

func (r *prefixedRegistrar) Register(entities ...DomainObject) error {
	for _, e := range entities {
		table, err := TableFromInstance(e)
		if err != nil {
			return errors.Wrapf(err, "failed to register entity")
		}
		fqn, err := r.prefix.Child(table.Name)
		if err != nil {
			// shouldn't happen if TableFromInstance behave correctly
			return errors.Wrap(err, "failed to register entity, this is most likely a bug in DOSA")
		}
		r.fqnIndex[fqn] = table
		r.typeIndex[reflect.TypeOf(e).Elem()] = fqn
	}
	return nil
}

func (r *prefixedRegistrar) lookupByFQN(fqn FQN) (*Table, error) {
	table, ok := r.fqnIndex[fqn]
	if !ok {
		return nil, errors.Errorf("failed to find entity definition for FQN: %s", fqn)
	}
	return table, nil
}

func (r *prefixedRegistrar) lookupByType(entity DomainObject) (*Table, FQN, error) {
	t := reflect.TypeOf(entity).Elem()
	fqn, ok := r.typeIndex[t]
	if !ok {
		return nil, "", errors.Errorf("failed to find entity definition for entity: %v", t)
	}
	table := r.fqnIndex[fqn]
	return table, fqn, nil
}
