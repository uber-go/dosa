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
