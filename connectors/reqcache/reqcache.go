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

package reqcache

import (
	"context"

	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/base"
	"github.com/uber-go/dosa/connectors/memory"
)

type cacheKey string

// ReqCacheKey is the key used for the context request cache.
// Best practice is to use a private type to avoid any possible collisions
const ReqCacheKey = cacheKey("reqcache")

// Connector is the request cache connector type.
type Connector struct {
	base.Connector
}

// NewConnector constructs a request context connector
func NewConnector(db dosa.Connector) *Connector {
	return &Connector{base.Connector{db}}
}

// CacheableContext produces a context that has a cache in it
// from one that does not
func CacheableContext(ctx context.Context) context.Context {
	cache := memory.NewConnector()
	return context.WithValue(ctx, ReqCacheKey, cache)
}

func cacheFromContext(ctx context.Context) dosa.Connector {
	rc := ctx.Value(ReqCacheKey)
	if rc == nil {
		return nil
	}
	if conn, ok := rc.(dosa.Connector); ok {
		return conn
	}
	return nil
}

// CreateIfNotExists creates the object downstream, then upserts it into the local cache
// when successful
func (c *Connector) CreateIfNotExists(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	err := c.Next.CreateIfNotExists(ctx, ei, values)
	if err == nil {
		if cache := cacheFromContext(ctx); cache != nil { // if there is a cache
			_ = cache.Upsert(ctx, ei, values) // then update it
		}
	}
	return err
}

// Read reads first from the cache, and if not present, will read from the downstream
// connector
func (c *Connector) Read(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue, minimumFields []string) (map[string]dosa.FieldValue, error) {
	cache := cacheFromContext(ctx)
	if cache != nil {
		// cache present, attempt to read from it
		vals, err := cache.Read(ctx, ei, values, minimumFields)

		if err == nil {
			return vals, nil
		}
	}
	// cache not present or is missing the value, read it from the db
	vals, err := c.Next.Read(ctx, ei, values, dosa.All())
	// stash it in case it is read again
	if cache != nil {
		_ = cache.Upsert(ctx, ei, vals)
	}

	return vals, err
}

// MultiRead is not implemented
func (c *Connector) MultiRead(ctx context.Context, ei *dosa.EntityInfo, keys []map[string]dosa.FieldValue, minimumFields []string) (results []*dosa.FieldValuesOrError, err error) {
	panic("not implemented")
}

// Upsert first writes to the upstream and, if successful, will
// update the object in the cache
func (c *Connector) Upsert(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	err := c.Next.Upsert(ctx, ei, values)
	if err == nil {
		if cache := cacheFromContext(ctx); cache != nil { // if there is a cache
			// TODO: this doesn't work correctly for partial updates unless it was already cached
			_ = cache.Upsert(ctx, ei, values)
		}
	}
	return err
}

// MultiUpsert is not implemented
func (c *Connector) MultiUpsert(ctx context.Context, ei *dosa.EntityInfo, multiValues []map[string]dosa.FieldValue) (result []error, err error) {
	panic("not implemented")
}

// Remove removes the item from the remote database, and, if successful,
// removes it from the cache
func (c *Connector) Remove(ctx context.Context, ei *dosa.EntityInfo, keys map[string]dosa.FieldValue) error {
	err := c.Next.Remove(ctx, ei, keys)
	if err == nil {
		if cache := cacheFromContext(ctx); cache != nil { // if there is a cache
			_ = cache.Remove(ctx, ei, keys)
		}
	}
	return err
}

// RemoveRange removes a range from the datastore, and, if successful,
// removes it from the cache
func (c *Connector) RemoveRange(ctx context.Context, ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition) error {
	err := c.Next.RemoveRange(ctx, ei, columnConditions)
	if err == nil {
		if cache := cacheFromContext(ctx); cache != nil {
			_ = cache.RemoveRange(ctx, ei, columnConditions)
		}
	}
	return err
}

// MultiRemove is not implemented
func (c *Connector) MultiRemove(ctx context.Context, ei *dosa.EntityInfo, multiKeys []map[string]dosa.FieldValue) (result []error, err error) {
	panic("not implemented")
}

// Range reads from the upstream datasource, and, if successful, updates
// the cache for each object returned by range
func (c *Connector) Range(ctx context.Context, ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition, minimumFields []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	vals, token, err := c.Next.Range(ctx, ei, columnConditions, dosa.All(), token, limit)

	if err == nil {
		if cache := cacheFromContext(ctx); cache != nil {
			// if there is a cache, insert each found value into the cache
			for _, v := range vals {
				_ = cache.Upsert(ctx, ei, v)
			}
		}
	}
	return vals, token, err
}

// Search is not implemented
func (c *Connector) Search(ctx context.Context, ei *dosa.EntityInfo, fieldPairs dosa.FieldNameValuePair, minimumFields []string, token string, limit int) (multiValues []map[string]dosa.FieldValue, nextToken string, err error) {
	panic("not implemented")
}

// Scan currently just defers to the downstream for scanning
// It does not update the request cache.
func (c *Connector) Scan(ctx context.Context, ei *dosa.EntityInfo, minimumFields []string, token string, limit int) (multiValues []map[string]dosa.FieldValue, nextToken string, err error) {
	return c.Next.Scan(ctx, ei, minimumFields, token, limit)
}

// Shutdown shuts down the connector
func (c *Connector) Shutdown() error {
	return c.Next.Shutdown()
}
