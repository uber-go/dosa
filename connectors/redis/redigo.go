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

package redis

import (
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/metrics"
)

// NewRedigoClient returns a redigo implementation of SimpleRedis
func NewRedigoClient(config ServerConfig, scope metrics.Scope) SimpleRedis {
	c := &simpleRedis{config: config, stats: metrics.CheckIfNilStats(scope)}
	c.pool = &redis.Pool{
		MaxActive:   config.MaxActive,
		MaxIdle:     config.MaxIdle,
		IdleTimeout: config.IdleTimeout,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial(
				"tcp",
				c.getURL(),
				redis.DialConnectTimeout(config.ConnectTimeout),
				redis.DialReadTimeout(config.ReadTimeout),
				redis.DialWriteTimeout(config.WriteTimeout))
			if err != nil {
				return nil, err
			}
			return c, err
		},
		Wait: false,
	}
	return c
}

type simpleRedis struct {
	config ServerConfig
	pool   *redis.Pool
	stats  metrics.Scope
}

func (c *simpleRedis) getURL() string {
	return fmt.Sprintf("%s:%d", c.config.Host, c.config.Port)
}

// Get returns an error if the key is not found in cache
func (c *simpleRedis) Get(key string) ([]byte, error) {
	bytes, err := redis.Bytes(c.do("GET", key))
	if err == redis.ErrNil {
		err = &dosa.ErrNotFound{}
	}
	return bytes, err
}

func (c *simpleRedis) SetEx(key string, value []byte, ttl time.Duration) error {
	_, err := c.do("SET", key, value, "EX", ttl.Seconds())
	return err
}

func (c *simpleRedis) Del(key string) error {
	_, err := c.do("DEL", key)
	return err
}

// Shutdown closes the underlying connection pool to redis
func (c *simpleRedis) Shutdown() error {
	return c.pool.Close()
}

// Do is a proxy method that calls Redigo's `Do` method and returns its output. It remembers
// to close connections taken from the pool
func (c *simpleRedis) do(commandName string, args ...interface{}) (interface{}, error) {
	t := c.stats.SubScope("redis").SubScope("latency").Timer(commandName)
	t.Start()
	defer t.Stop()

	conn := c.pool.Get()
	defer func() { _ = conn.Close() }()
	return conn.Do(commandName, args...)
}
