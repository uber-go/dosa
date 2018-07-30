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
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/base"
	"github.com/uber-go/dosa/metrics"
)

const keySeparator = ","

// SimpleRedis is a minimal interface to Redis commands
type SimpleRedis interface {
	Get(key string) ([]byte, error)
	SetEx(key string, value []byte, ttl time.Duration) error
	Del(key string) error
	Shutdown() error
}

// ErrNotImplemented is returned for interface methods that do not have an implementation
type ErrNotImplemented struct{}

// Error returns a constant string "Not implemented"
func (*ErrNotImplemented) Error() string {
	return "Not implemented"
}

// ErrInvalidEntity is returned for dosa entities that cannot be categorized as a key-value schema for redis
type ErrInvalidEntity struct {
	msg string
}

// Error returns why the schema does not conform to key-value format
func (e *ErrInvalidEntity) Error() string {
	return fmt.Sprintf("This entity schema and value not supported by redis. %v", e.msg)
}

// NewErrInvalidEntity returns an ErrInvalidEntity
func NewErrInvalidEntity(msg string) *ErrInvalidEntity {
	return &ErrInvalidEntity{msg: msg}
}

// Config holds the settings for a RedisConnector
type Config struct {
	// ServerSettings are the settings specific to redis server
	ServerSettings ServerConfig
	// TTL for how long values should live in the cache
	TTL time.Duration
	// KeyPrefix is used as the prefix to construct redis key
	KeyPrefix string
}

// ServerConfig holds the settings for redis
type ServerConfig struct {
	Host string
	Port int
	// MaxIdle is the maximum number of idle connections in the pool.
	MaxIdle int
	// IdleTimeout directs to close connections after remaining idle for this duration.
	// If the value is zero, then idle connections are not closed. Applications should set
	// the timeout to a value less than the server's timeout.
	IdleTimeout time.Duration
	// Maximum number of connections allocated by the pool at a given time.
	// When zero, there is no limit on the number of connections in the pool.
	MaxActive int

	ConnectTimeout time.Duration
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
}

// NewConnector initializes a Redis Connector
func NewConnector(config Config, scope metrics.Scope) dosa.Connector {
	return &Connector{
		client:    NewRedigoClient(config.ServerSettings, scope),
		ttl:       config.TTL,
		stats:     metrics.CheckIfNilStats(scope),
		keyPrefix: config.KeyPrefix,
	}
}

// Connector for redis database
type Connector struct {
	base.Connector
	client    SimpleRedis
	ttl       time.Duration
	stats     metrics.Scope
	keyPrefix string
}

// CreateIfNotExists not implemented
func (c *Connector) CreateIfNotExists(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	return new(ErrNotImplemented)
}

// MultiRead not implemented
func (c *Connector) MultiRead(ctx context.Context, ei *dosa.EntityInfo, keys []map[string]dosa.FieldValue, minimumFields []string) (results []*dosa.FieldValuesOrError, err error) {
	return nil, new(ErrNotImplemented)
}

// MultiUpsert not implemented
func (c *Connector) MultiUpsert(ctx context.Context, ei *dosa.EntityInfo, multiValues []map[string]dosa.FieldValue) (result []error, err error) {
	return nil, new(ErrNotImplemented)
}

// RemoveRange not implemented
func (c *Connector) RemoveRange(ctx context.Context, ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition) error {
	return new(ErrNotImplemented)
}

// MultiRemove not implemented
func (c *Connector) MultiRemove(ctx context.Context, ei *dosa.EntityInfo, multiKeys []map[string]dosa.FieldValue) (result []error, err error) {
	return nil, new(ErrNotImplemented)
}

// Range not implemented.
func (c *Connector) Range(ctx context.Context, ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition, minimumFields []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	return nil, "", new(ErrNotImplemented)
}

// Scan not implemented.
func (c *Connector) Scan(ctx context.Context, ei *dosa.EntityInfo, minimumFields []string, token string, limit int) (multiValues []map[string]dosa.FieldValue, nextToken string, err error) {
	return nil, "", new(ErrNotImplemented)
}

// Shutdown not implemented
func (c *Connector) Shutdown() error {
	err := c.client.Shutdown()
	c.logCallCount("Shutdown", err)
	return err
}

// Read reads an object based on primary key
func (c *Connector) Read(ctx context.Context, ei *dosa.EntityInfo, keys map[string]dosa.FieldValue, fieldsToRead []string) (map[string]dosa.FieldValue, error) {
	err := validateSchema(ei)
	if err != nil {
		return nil, err
	}

	keyName, valueName := nameOfKeyValue(ei)

	cacheKey, err := buildKey(c.keyPrefix, ei.Ref.Scope, ei.Ref.NamePrefix, ei.Def.Name, keys[keyName])
	if err != nil {
		return nil, err
	}

	cacheValue, err := c.client.Get(cacheKey)
	c.logHitRate("Read", err)
	if err != nil {
		return nil, err
	}

	result := make(map[string]dosa.FieldValue)
	// Copy original keys into response
	for k, v := range keys {
		result[k] = v
	}
	result[valueName] = cacheValue
	return result, nil
}

// Upsert means update an existing object or create a new object
func (c *Connector) Upsert(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	err := validateSchema(ei)
	if err != nil {
		return err
	}

	keyName, valueName := nameOfKeyValue(ei)

	cacheValue, ok := values[valueName]
	if !ok || cacheValue == nil {
		return NewErrInvalidEntity("No value specified.")
	}

	cacheValueBytes := cacheValue.([]byte)
	if len(cacheValueBytes) == 0 {
		return NewErrInvalidEntity("No value specified.")
	}

	cacheKey, err := buildKey(c.keyPrefix, ei.Ref.Scope, ei.Ref.NamePrefix, ei.Def.Name, values[keyName])
	if err != nil {
		return err
	}

	err = c.client.SetEx(cacheKey, cacheValueBytes, c.ttl)
	c.logCallCount("Upsert", err)
	return err
}

// Remove deletes a key
func (c *Connector) Remove(ctx context.Context, ei *dosa.EntityInfo, keys map[string]dosa.FieldValue) error {
	err := validateSchema(ei)
	if err != nil {
		return err
	}
	keyName, _ := nameOfKeyValue(ei)
	cacheKey, err := buildKey(c.keyPrefix, ei.Ref.Scope, ei.Ref.NamePrefix, ei.Def.Name, keys[keyName])
	if err != nil {
		return err
	}

	err = c.client.Del(cacheKey)
	c.logCallCount("Remove", err)
	return err
}

func (c *Connector) logHitRate(method string, err error) {
	if err != nil && dosa.ErrorIsNotFound(err) {
		c.incStat("miss", method)
		return
	}
	c.logCallCount(method, err)
}

func (c *Connector) logCallCount(method string, err error) {
	if err != nil {
		c.incStat("error", method)
	} else {
		c.incStat("success", method)
	}
}

func (c *Connector) incStat(action, method string) {
	c.stats.SubScope("cache").Tagged(map[string]string{"method": method}).Counter(action).Inc(1)
}

// return order is key, value
func nameOfKeyValue(ei *dosa.EntityInfo) (string, string) {
	keyName := ei.Def.Key.PartitionKeys[0]
	cols := ei.Def.Columns
	if cols[0].Name == keyName {
		return keyName, cols[1].Name
	}
	return keyName, cols[0].Name
}

func validateSchema(ei *dosa.EntityInfo) error {
	if len(ei.Def.Key.PartitionKeys) != 1 || len(ei.Def.Key.ClusteringKeys) != 0 {
		return NewErrInvalidEntity("Should only have a single key.")
	}
	if len(ei.Def.Columns) != 2 {
		return NewErrInvalidEntity("Should have one key, one value.")
	}
	if ei.Def.Columns[0].Type != dosa.Blob || ei.Def.Columns[1].Type != dosa.Blob {
		return NewErrInvalidEntity("Types should be []byte.")
	}
	return nil
}

func buildKey(keyPrefix, scope, namePrefix, name string, keyValue interface{}) (string, error) {
	keyNamespace := strings.Join([]string{scope, namePrefix, name}, keySeparator)
	keyString := keyValue.([]byte)
	if len(keyString) == 0 {
		return "", NewErrInvalidEntity("No key specified.")
	}

	return strings.Join([]string{keyPrefix, keyNamespace, string(keyString)}, keySeparator), nil
}
