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

package testingexamples

import (
	"context"
	"time"

	"github.com/uber-go/dosa"
)

// User is a user record
type User struct {
	dosa.Entity `dosa:"primaryKey=UUID"`
	UUID        dosa.UUID
	Name        string
	Email       string
	CreatedOn   time.Time
}

// Datastore implements methods to operate on entities.
type Datastore struct {
	client dosa.Client
}

// NewDatastore returns a new instance of Datastore or an error
func NewDatastore(c dosa.Client) (*Datastore, error) {
	ctx, cancelFn := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFn()
	if err := c.Initialize(ctx); err != nil {
		return nil, err
	}
	return &Datastore{client: c}, nil
}

// GetUser fetches a user from the datastore by uuid
func (d *Datastore) GetUser(ctx context.Context, uuid dosa.UUID) (*User, error) {
	user := &User{UUID: uuid}
	readCtx, readCancelFn := context.WithTimeout(ctx, 1*time.Second)
	defer readCancelFn()

	// we want to read all values, so we pass `nil` for fields to read
	if err := d.client.Read(readCtx, nil, user); err != nil {
		return nil, err
	}
	return user, nil
}
