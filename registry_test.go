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
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRegistrar(t *testing.T) {
	_, err := NewRegistrar("invalid-prefix")
	assert.Error(t, err)

	_, err = NewRegistrar("valid.prefix")
	assert.NoError(t, err)
}

func TestRegistrar(t *testing.T) {
	r, err := NewRegistrar("test.registrar")
	assert.NoError(t, err)
	err = r.Register(&SinglePrimaryKeyNoParen{}, &SinglePrimaryKey{}, &MultiComponentPrimaryKey{})
	assert.NoError(t, err)
	err = r.Register(&EmptyPrimaryKey{})
	assert.Error(t, err)

	for _, e := range []DomainObject{&SinglePrimaryKeyNoParen{}, &SinglePrimaryKey{}, &MultiComponentPrimaryKey{}} {
		expectedFQN := FQN(fmt.Sprintf("test.registrar.%s", strings.ToLower(reflect.TypeOf(e).Elem().Name())))

		table, fqn, err := r.LookupByType(e)
		assert.NoError(t, err)
		assert.NotNil(t, table)
		assert.Equal(t, expectedFQN, fqn)

		table, err = r.LookupByFQN(expectedFQN)
		assert.NoError(t, err)
		assert.NotNil(t, table)
	}

	_, err = r.LookupByFQN("test.registrar.nil")
	assert.Error(t, err)
	_, _, err = r.LookupByType(&SinglePartitionKey{})
	assert.Error(t, err)
}
