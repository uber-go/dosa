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

package dosa

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEmptyHeaders(t *testing.T) {
	ctx := context.Background()
	hdrs := GetHeaders(ctx)
	_, ok := hdrs["foobar"]
	assert.False(t, ok)
}

func TestOneHeader(t *testing.T) {
	ctx := context.Background()
	headers := map[string]string{
		"Foo":   "Bar",
		"Fubar": "You",
	}
	ctx = AddHeader(ctx, "Foo", "Bar")
	ctx = AddHeader(ctx, "Fubar", "You")

	hdrs := GetHeaders(ctx)
	assert.Equal(t, hdrs, headers)
}

func TestHeadersWithDeadline(t *testing.T) {
	ts, _ := time.Parse(time.RFC3339, "2028-01-02T15:04:05Z")
	ctx, cancel := context.WithDeadline(context.Background(), ts)
	ctx = AddHeader(ctx, "Foo", "Bar")
	ctx = AddHeader(ctx, "Fubar", "You")
	hdrs := GetHeaders(ctx)
	assert.Equal(t, "You", hdrs["Fubar"])

	// The new context should have the same deadline
	dl, ok := ctx.Deadline()
	assert.True(t, ok)
	assert.Equal(t, dl, ts)

	// Cancelling the original should also cancel the copy
	assert.Nil(t, ctx.Err())
	cancel()
	assert.NotNil(t, ctx.Err())
}
