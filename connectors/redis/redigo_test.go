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

package redis_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa/connectors/redis"
)

func TestRedisNotRunning(t *testing.T) {
	if !redis.IsRunning() {
		c := redis.NewRedigoClient(redis.ServerConfig{})
		err := c.Del("somekey")
		assert.Error(t, err)
	}
}

func TestRedisSyntax(t *testing.T) {
	if !redis.IsRunning() {
		t.SkipNow()
	}

	// Test the redigo implementation does not error
	c := redis.NewRedigoClient(testRedisConfig.ServerSettings)

	err := c.SetEx("testkey", []byte("testvalue"), 100*time.Second)
	assert.NoError(t, err)

	v, err := c.Get("testkey")
	assert.NoError(t, err)
	assert.Equal(t, []byte("testvalue"), v)

	err = c.Del("testkey")
	assert.NoError(t, err)

	_, err = c.Get("testkey")
	assert.EqualError(t, err, "not found")
}

func TestShutdown(t *testing.T) {
	if !redis.IsRunning() {
		t.SkipNow()
	}

	c := redis.NewRedigoClient(testRedisConfig.ServerSettings)
	err := c.Shutdown()
	assert.NoError(t, err)
}

func TestWrongPort(t *testing.T) {
	config := redis.ServerConfig{
		Host: "localhost",
		Port: 1111,
	}
	c := redis.NewRedigoClient(config)
	_, err := c.Get("testkey")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "1111")
}
