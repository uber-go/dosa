// Copyright (c) 2018 Uber Technologies, Inc.
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

package testclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/testentity"
)

func TestTestClient(t *testing.T) {
	client, err := NewTestClient("testscope", "testprefix", &testentity.TestEntity{})
	assert.NoError(t, err)

	err = client.Initialize(context.Background())
	assert.NoError(t, err)

	uuid := dosa.NewUUID()
	testEnt := testentity.TestEntity{
		UUIDKey:  uuid,
		StrKey:   "key",
		Int64Key: 1,
		StrV:     "hello",
	}

	err = client.Upsert(context.Background(), nil, &testEnt)
	assert.NoError(t, err)

	readEnt := testentity.TestEntity{
		UUIDKey:  uuid,
		StrKey:   "key",
		Int64Key: 1,
	}

	err = client.Read(context.Background(), nil, &readEnt)
	assert.NoError(t, err)

	assert.Equal(t, "hello", readEnt.StrV)
}
