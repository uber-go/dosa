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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors"
)

type ClientTestEntity struct {
	dosa.Entity `dosa:"primaryKey=(ID)"`
	ID          int64
	Name        string
	Email       string
}

var (
	clientTestEntity   = &ClientTestEntity{ID: 1, Name: "foo", Email: "foo@uber.com"}
	clientTestEntities = []dosa.DomainObject{clientTestEntity}
)

func ExampleNewClient() {
	// initialize registrar
	reg, err := dosa.NewRegistrar("test", "myteam.myservice", clientTestEntities...)
	if err != nil {
		// registration will most likely fail as a result of programmer error
		panic("dosa.NewRegister returned an error")
	}

	// use a noop connector for example purposes
	conn := &connectors.Noop{}

	// initialize a pseudo-connected client
	client, err := dosa.NewClient(reg, conn)
	if err != nil {
		// panic is probably not what you want to do in practice, but for the
		// sake of an example, this is the behavior we want
		panic("dosa.NewClient returned an error")
	}

	err = client.Initialize(context.Background())
	if err != nil {
		// same as above, probably want to surface the error in some way, but
		// an error here may indicate something that is retriable, for example
		// a timeout may be recoverable whereas a schema validation error is not
		panic("client.Initialize returned an error")
	}
}

func TestNewClient(t *testing.T) {
	// initialize registrar
	reg, err := dosa.NewRegistrar("test", "myteam.myservice", clientTestEntities...)
	assert.NoError(t, err)
	assert.NotNil(t, reg)

	// use a noop connector for test test test purposes
	conn := &connectors.Noop{}

	// initialize a pseudo-connected client
	client, err := dosa.NewClient(reg, conn)
	assert.NoError(t, err)
	err = client.Initialize(context.TODO())
	assert.NoError(t, err)
}

func TestClient_Read(t *testing.T) {
	// initialize registrar
	reg, err := dosa.NewRegistrar("test", "myteam.myservice", clientTestEntities...)
	assert.NoError(t, err)
	assert.NotNil(t, reg)

	// use a noop connector for test purposes
	conn := &connectors.Noop{}

	// initialize a pseudo-connected client
	client, err := dosa.NewClient(reg, conn)
	assert.NoError(t, err)
	err = client.Initialize(context.TODO())
	assert.NoError(t, err)

	err = client.Read(context.TODO(), []string{"ID", "Name", "Email"}, clientTestEntity)
	assert.NoError(t, err)
	assert.Equal(t, clientTestEntity.ID, int64(1))
	assert.Equal(t, clientTestEntity.Name, "updated name")
	assert.Equal(t, clientTestEntity.Email, "updated@email.com")
}
