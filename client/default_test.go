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

package dosaclient_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/client"
	"github.com/uber-go/dosa/connectors/noop"
)

type ExampleEntity struct {
	dosa.Entity `dosa:"primaryKey=(ID)"`
	ID          int64
	Name        string
	Email       string
}

var (
	testEntity   = &ExampleEntity{ID: 1, Name: "foo", Email: "foo@uber.com"}
	testEntities = []dosa.DomainObject{testEntity}
)

func ExampleNewDefault() {
	// initialize registrar
	reg, _ := dosa.NewRegistrar("myteam.service")
	if err := reg.Register(testEntities...); err != nil {
		// registration will most likely fail as a result of programmer error
		panic("registrar.Register returned an error")
	}

	// use a noop connector for example purposes
	conn := &noop.Connector{}

	// initialize a pseudo-connected client
	c, err := dosaclient.NewDefault(reg, conn)
	if err != nil {
		// panic is probably not what you want to do in practice, but for the
		// sake of an example, this is the behavior we want
		panic("dosaclient.NewDefault returned an error")
	}

	err = c.Initialize(context.Background())
	if err != nil {
		// same as above, probably want to surface the error in some way, but
		// an error here may indicate something that is retriable, for example
		// a timeout may be recoverable whereas a schema validation error is not
		panic("dosaclient.Initialize returned an error")
	}
}

func TestNewDefault(t *testing.T) {
	// initialize registrar
	reg, err := dosa.NewRegistrar("myteam.service")
	assert.NoError(t, err)
	assert.NotNil(t, reg)

	err = reg.Register(testEntities...)
	assert.NoError(t, err)

	// use a noop connector for test test test purposes
	conn := &noop.Connector{}

	// initialize a pseudo-connected client
	c, err := dosaclient.NewDefault(reg, conn)
	assert.NoError(t, err)
	err = c.Initialize(context.TODO())
	assert.NoError(t, err)
}

func TestRead(t *testing.T) {
	// initialize registrar
	reg, err := dosa.NewRegistrar("myteam.service")
	assert.NoError(t, err)
	assert.NotNil(t, reg)

	err = reg.Register(testEntities...)
	assert.NoError(t, err)

	// use a noop connector for test purposes
	conn := &noop.Connector{}

	// initialize a pseudo-connected client
	c, err := dosaclient.NewDefault(reg, conn)
	assert.NoError(t, err)
	err = c.Initialize(context.TODO())
	assert.NoError(t, err)

	err = c.Read(context.TODO(), []string{"ID", "Name", "Email"}, testEntity)
	assert.NoError(t, err)
	assert.Equal(t, testEntity.ID, int64(1))
	assert.Equal(t, testEntity.Name, "test")
	assert.Equal(t, testEntity.Email, "email")
}
