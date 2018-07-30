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

package redis_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa/connectors/redis"
	"github.com/uber-go/dosa/mocks"
)

func TestRedisNotRunning(t *testing.T) {
	if !redis.IsRunning() {
		c := redis.NewRedigoClient(redis.ServerConfig{}, nil)
		err := c.Del("somekey")
		assert.Error(t, err)
	}
}

func TestRedisSyntax(t *testing.T) {
	if !redis.IsRunning() {
		t.SkipNow()
	}

	// Test the redigo implementation does not error
	c := redis.NewRedigoClient(testRedisConfig.ServerSettings, nil)

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

	c := redis.NewRedigoClient(testRedisConfig.ServerSettings, nil)
	err := c.Shutdown()
	assert.NoError(t, err)
}

func TestWrongPort(t *testing.T) {
	config := redis.ServerConfig{
		Host: "localhost",
		Port: 1111,
	}
	c := redis.NewRedigoClient(config, nil)
	_, err := c.Get("testkey")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "1111")
}

// Test that we track latencies for all the redis commands
func TestTimerCalled(t *testing.T) {
	if !redis.IsRunning() {
		t.SkipNow()
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	stats := mocks.NewMockScope(ctrl)

	ctrl2 := gomock.NewController(t)
	defer ctrl2.Finish()
	timer := mocks.NewMockTimer(ctrl2)

	c := redis.NewRedigoClient(testRedisConfig.ServerSettings, stats)

	redisCommands := map[string]func(t *testing.T){
		"GET": func(t *testing.T) { c.Get("a") },
		"SET": func(t *testing.T) { c.SetEx("a", []byte{1}, 9*time.Second) },
		"DEL": func(t *testing.T) { c.Del("a") },
	}
	for command, f := range redisCommands {
		stats.EXPECT().SubScope("redis").Return(stats)
		stats.EXPECT().SubScope("latency").Return(stats)
		stats.EXPECT().Timer(command).Return(timer)
		timer.EXPECT().Start().Return(time.Now())
		timer.EXPECT().Stop()
		t.Run(fmt.Sprintf("%v test", command), f)
	}
}
