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

package main

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/mocks"
)

func TestScopeShow(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	scopes := map[string]dosa.FieldValue{
		"name":  "test_dev",
		"owner": "tester",
		"type":  int32(dosa.Development),
	}

	reg, e1 := dosa.NewRegistrar("production", "prefix", &dosa.ScopeMetadata{})
	assert.Nil(t, e1)

	conn := mocks.NewMockConnector(ctrl)
	conn.EXPECT().CheckSchema(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any())
	conn.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(scopes, nil)
	show := newScopeShow(func(opts GlobalOptions) (dosa.Client, error) {
		client := dosa.NewClient(reg, conn)
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		err := client.Initialize(ctx)
		assert.Nil(t, err, "couldn't initialize client")
		return client, nil
	})
	client, e2 := show.makeClient()
	assert.Nil(t, e2)

	md, e3 := show.run(client, "test_dev")

	assert.Nil(t, e3)
	assert.Equal(t, "test_dev", md.Name)
	assert.Equal(t, "tester", md.Owner)
	assert.Equal(t, int32(dosa.Development), md.Type)
}
