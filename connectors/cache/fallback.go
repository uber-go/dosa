// Copyright (c) 2020 Uber Technologies, Inc.
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

package cache

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/base"
	"github.com/uber-go/dosa/encoding"
	"github.com/uber-go/dosa/metrics"
)

const (
	key   = "key"
	value = "value"
)

type rangeResults struct {
	Rows      []map[string]dosa.FieldValue
	TokenNext string
}

type rangeQuery struct {
	Conditions []*dosa.ColumnCondition `json:",omitempty"`
	Token      string
	Limit      int
}

// Options returns a function that's being used for connector initialization
type Options func(*Connector) error

// WithSkipWriteInvalidateEntities provides the option for client to set the entites
func WithSkipWriteInvalidateEntities(entities ...dosa.DomainObject) Options {
	return func(c *Connector) error {
		c.skipWriteInvalidateEntitiesMap = createSkipWriteCacheInvalidateSet(entities)
		return nil
	}
}

// contextKey used by SetCacheableEndpoints to set endpoint names
type contextKey string

var (
	// contextEndpoint allows users to pass in calling endpoint name
	contextEndpoint contextKey = "endpoint"
	// endpointActiveStatus marks the endpoint as active
	endpointActiveStatus bool = true
)

// SetContextEndpoint set endpoint in context
func SetContextEndpoint(ctx context.Context, endpoint string) context.Context {
	return context.WithValue(ctx, contextEndpoint, endpoint)
}

// GetContextEndpoint get endpoint from context
func GetContextEndpoint(ctx context.Context) string {
	endpoint, _ := ctx.Value(contextEndpoint).(string)
	return endpoint
}

// SetCacheableEndpoints sets cacheable endpoints
func SetCacheableEndpoints(endpoints ...string) Options {
	return func(c *Connector) error {
		for _, endpoint := range endpoints {
			c.cacheableEndpointStatus[endpoint] = endpointActiveStatus
		}
		return nil
	}
}

// NewConnector creates a fallback cache connector
func NewConnector(origin, fallback dosa.Connector, scope metrics.Scope, entities []dosa.DomainObject, options ...Options) *Connector {
	c := newConnector(origin, fallback, scope, encoding.NewGobEncoder(), entities)
	for _, option := range options {
		err := option(c)
		if err != nil {
			// It's a no-op when getting an error now
			continue
		}
	}
	return c
}

func newConnector(origin, fallback dosa.Connector, scope metrics.Scope, encoder encoding.Encoder, entities []dosa.DomainObject) *Connector {
	bc := base.Connector{Next: origin}
	set := createCachedEntitiesSet(entities)
	cacheableEndpointStatus := make(map[string]bool)
	return &Connector{
		Connector:               bc,
		fallback:                fallback,
		encoder:                 encoder,
		cacheableEntities:       set,
		cacheableEndpointStatus: cacheableEndpointStatus,
		stats: scope,
	}
}

// Connector is a fallback cache connector
type Connector struct {
	base.Connector
	fallback                       dosa.Connector
	encoder                        encoding.Encoder
	cacheableEntities              map[string]bool
	cacheableEndpointStatus        map[string]bool
	skipWriteInvalidateEntitiesMap map[string]bool
	mux                            sync.Mutex
	stats                          metrics.Scope
	// Used primarily for testing so that nothing is called in a goroutine
	synchronous bool
}

// Upsert removes (invalidates) the entry from the fallback if the entity is not in the skipWriteInvalidateEntitiesMap
func (c *Connector) Upsert(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	if c.isWriteCacheable(ei) {
		w := func() error {
			return c.removeValueFromFallback(ctx, ei, createCacheKey(ei, values))
		}
		_ = c.cacheWrite(w)
	}
	return c.Next.Upsert(ctx, ei, values)
}

func (c *Connector) Read(ctx context.Context, ei *dosa.EntityInfo, keys map[string]dosa.FieldValue, minimumFields []string) (values map[string]dosa.FieldValue, err error) {
	// Read from source of truth first
	source, sourceErr := c.Next.Read(ctx, ei, keys, dosa.All())
	// Add the primary keys back into results map as dosa.All() does not fetch the keys
	if sourceErr == nil {
		populateValuesWithKeys(keys, source)
	}
	// If we are not caching for this entity, just return
	if !c.isReadCacheable(ctx, ei) {
		return source, sourceErr
	}

	// if source of truth is good, return result and write result to cache
	if sourceErr == nil {
		w := func() error {
			return c.write(ctx, ei, keys, source)
		}
		_ = c.cacheWrite(w)

		return source, sourceErr
	}

	return c.read(ctx, ei, keys, source, sourceErr, "READ")
}

// if source of truth fails, try the fallback. If the fallback fails, return the original error
func (c *Connector) read(
	ctx context.Context,
	ei *dosa.EntityInfo,
	keys map[string]dosa.FieldValue,
	source map[string]dosa.FieldValue,
	sourceErr error,
	methodName string,
) (values map[string]dosa.FieldValue, err error) {
	if dosa.ErrorIsNotFound(sourceErr) {
		return source, sourceErr
	}

	ckey := createCacheKey(ei, keys)
	value, err := c.getValueFromFallback(ctx, ei, ckey)
	c.logFallback(methodName, ei.Def.Name, err)
	if err != nil {
		return source, sourceErr
	}
	result := map[string]dosa.FieldValue{}
	err = c.encoder.Decode(value, &result)
	if err != nil {
		return source, sourceErr
	}
	return rawRowAsPointers(ei, result), err
}

func (c *Connector) write(ctx context.Context, ei *dosa.EntityInfo, keys map[string]dosa.FieldValue, source map[string]dosa.FieldValue) (err error) {
	cacheKey := createCacheKey(ei, keys)
	return c.writeKeyValueToFallback(ctx, ei, cacheKey, source)
}

// Range returns range from origin, reverts to fallback if origin fails
func (c *Connector) Range(ctx context.Context, ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition, minimumFields []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	sourceRows, sourceToken, sourceErr := c.Next.Range(ctx, ei, columnConditions, dosa.All(), token, limit)
	if !c.isReadCacheable(ctx, ei) || dosa.ErrorIsNotFound(sourceErr) {
		return sourceRows, sourceToken, sourceErr
	}
	cacheKey := rangeQuery{
		Conditions: dosa.NormalizeConditions(columnConditions),
		Token:      token,
		Limit:      limit,
	}
	rangeResult := rangeResults{
		TokenNext: sourceToken,
		Rows:      sourceRows,
	}

	if sourceErr == nil {
		w := func() error {
			return c.writeKeyValueToFallback(ctx, ei, cacheKey, rangeResult)
		}
		_ = c.cacheWrite(w)

		return sourceRows, sourceToken, sourceErr
	}

	value, err := c.getValueFromFallback(ctx, ei, cacheKey)
	c.logFallback("RANGE", ei.Def.Name, err)
	if err != nil {
		return sourceRows, sourceToken, sourceErr
	}
	unpack := rangeResults{}
	err = c.encoder.Decode(value, &unpack)
	if err != nil {
		return sourceRows, sourceToken, sourceErr
	}

	return rawRowsAsPointers(ei, unpack.Rows), unpack.TokenNext, err
}

// Scan returns scan result from origin.
func (c *Connector) Scan(ctx context.Context, ei *dosa.EntityInfo, minimumFields []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	// Make Scan call call Scan directly
	return c.Next.Scan(ctx, ei, minimumFields, token, limit)
}

// MultiRead reads from fallback for the keys that failed
// There are a few scenarios for the fallback:
// 1. The original multiread call fails overall with an error XYZ. The fallback will try to read as many keys as possible.
// - If none of the keys are in the fallback, the original XYZ error is returned.
// - If there are partial successes, the fallback will return an array of dosa.FieldValuesOrError.
//   The keys that are not found will have the original overall failure XYZ set as the error
// 2. The original multiread does not have an error but some of the results have errors. The fallback will try to read
// the keys that have an error and replace the failed result with the result from fallback. If the key is not in fallback,
// do not modify the original result
func (c *Connector) MultiRead(ctx context.Context, ei *dosa.EntityInfo, keys []map[string]dosa.FieldValue, minimumFields []string) (results []*dosa.FieldValuesOrError, err error) {
	// Read from source of truth first
	source, sourceErr := c.Next.MultiRead(ctx, ei, keys, dosa.All())
	// Add the primary keys back into results map as dosa.All() does not fetch the keys
	if sourceErr == nil {
		for idx, result := range source {
			populateValuesWithKeys(keys[idx], result.Values)
		}
	}

	if dosa.ErrorIsNotFound(sourceErr) || !c.isReadCacheable(ctx, ei) {
		return source, sourceErr
	}

	// if no errors from source of truth, write result to cache
	if sourceErr == nil {
		w := func() error {
			for idx, result := range source {
				if result.Error == nil {
					_ = c.write(ctx, ei, keys[idx], result.Values)
				}
			}
			return nil
		}
		_ = c.cacheWrite(w)
	}

	// When error, try to read from cache.
	// But since this is MultiRead, even if there is no error,
	// it might still have returned only partial results.
	// Try to fetch the remaining missing results from cache
	multiResult := make([]*dosa.FieldValuesOrError, len(keys))
	var useCacheResult bool
	var wg sync.WaitGroup
	wg.Add(len(keys))
	for idx, primaryKey := range keys {
		singleResult := &dosa.FieldValuesOrError{Error: sourceErr}
		if len(source) != 0 {
			singleResult = source[idx]
		}
		// Default to the original singleResult and then overwrite with results from cache
		multiResult[idx] = singleResult

		go func(idx int, keys map[string]dosa.FieldValue, source *dosa.FieldValuesOrError) {
			defer wg.Done()
			// Do not need to read from cache for results that had no error
			if source.Error == nil {
				return
			}
			result, err := c.read(ctx, ei, keys, source.Values, source.Error, "MULTIREAD")
			// Keep track of if reading from cache actually had a different outcome
			if err == nil {
				useCacheResult = true
			}
			multiResult[idx] = &dosa.FieldValuesOrError{
				Values: result,
				Error:  err,
			}
		}(idx, primaryKey, singleResult)
	}
	wg.Wait()
	if useCacheResult {
		return multiResult, nil
	}
	return source, sourceErr
}

// Remove deletes an entry
func (c *Connector) Remove(ctx context.Context, ei *dosa.EntityInfo, keys map[string]dosa.FieldValue) error {
	if c.isWriteCacheable(ei) {
		w := func() error {
			return c.removeValueFromFallback(ctx, ei, createCacheKey(ei, keys))
		}
		_ = c.cacheWrite(w)
	}

	return c.Next.Remove(ctx, ei, keys)
}

// MultiUpsert deletes the entries getting upserted from the fallback if the entity is not in the skipWriteInvalidateEntitiesMap
func (c *Connector) MultiUpsert(ctx context.Context, ei *dosa.EntityInfo, multiValues []map[string]dosa.FieldValue) (result []error, err error) {
	if c.isWriteCacheable(ei) {
		w := func() error {
			for _, values := range multiValues {
				_ = c.removeValueFromFallback(ctx, ei, createCacheKey(ei, values))
			}
			return nil
		}
		_ = c.cacheWrite(w)
	}
	return c.Next.MultiUpsert(ctx, ei, multiValues)
}

// MultiRemove deletes multiple entries from the fallback
func (c *Connector) MultiRemove(ctx context.Context, ei *dosa.EntityInfo, multiKeys []map[string]dosa.FieldValue) (result []error, err error) {
	if c.isWriteCacheable(ei) {
		w := func() error {
			for _, keys := range multiKeys {
				_ = c.removeValueFromFallback(ctx, ei, createCacheKey(ei, keys))
			}
			return nil
		}
		_ = c.cacheWrite(w)
	}

	return c.Next.MultiRemove(ctx, ei, multiKeys)
}

func (c *Connector) getValueFromFallback(ctx context.Context, ei *dosa.EntityInfo, ckey interface{}) ([]byte, error) {
	adaptedEi := adaptToKeyValue(ei)
	keyValue, err := c.encoder.Encode(ckey)
	if err != nil {
		return nil, err
	}
	response, err := c.fallback.Read(ctx, adaptedEi, map[string]dosa.FieldValue{key: keyValue}, dosa.All())
	if err != nil {
		return nil, err
	}

	// unpack the value
	cacheValue, ok := response[value].([]byte)
	if !ok {
		return nil, errors.New("No value in cache for key")
	}
	return cacheValue, nil
}

func (c *Connector) removeValueFromFallback(ctx context.Context, ei *dosa.EntityInfo, ckey interface{}) error {
	if c.shouldSkipInvalidateCacheOnWrite(ei) {
		return nil
	}
	cacheKey, err := c.encoder.Encode(ckey)
	if err != nil {
		return err
	}

	newCtx, cancel := createContextForFallback(ctx)
	defer cancel()

	adaptedEi := adaptToKeyValue(ei)
	return c.fallback.Remove(newCtx, adaptedEi, map[string]dosa.FieldValue{key: cacheKey})
}

func (c *Connector) writeKeyValueToFallback(ctx context.Context, ei *dosa.EntityInfo, ckey, cvalue interface{}) error {
	cacheKey, err := c.encoder.Encode(ckey)
	if err != nil {
		return err
	}

	cacheValue, err := c.encoder.Encode(cvalue)
	if err != nil {
		return err
	}

	newValues := map[string]dosa.FieldValue{
		key:   cacheKey,
		value: cacheValue,
	}

	newCtx, cancel := createContextForFallback(ctx)
	defer cancel()
	adaptedEi := adaptToKeyValue(ei)

	return c.fallback.Upsert(newCtx, adaptedEi, newValues)
}

func (c *Connector) logFallback(method, entityName string, err error) {
	if c.stats != nil {
		s := c.stats.SubScope("fallback").Tagged(map[string]string{"method": method, "entityName": entityName})
		if err != nil {
			s.Counter("failure").Inc(1)
		} else {
			s.Counter("success").Inc(1)
		}
	}
}

func (c *Connector) setSynchronousMode(sync bool) {
	c.synchronous = sync
}

func (c *Connector) cacheWrite(w func() error) error {
	if c.synchronous {
		return w()
	}
	go func() { _ = w() }()
	return nil
}

func (c *Connector) shouldSkipInvalidateCacheOnWrite(ei *dosa.EntityInfo) bool {
	return c.skipWriteInvalidateEntitiesMap[ei.Def.Name]
}

func (c *Connector) isReadCacheable(ctx context.Context, ei *dosa.EntityInfo) bool {
	// Reads are cached based on entity and endpoint configurations
	return c.cacheableEntities[ei.Def.Name] && c.isEndpointCacheable(ctx)
}

func (c *Connector) isWriteCacheable(ei *dosa.EntityInfo) bool {
	// Writes are cached (cache invalidated) based on entity configurations
	return c.cacheableEntities[ei.Def.Name]
}

func (c *Connector) isEndpointCacheable(ctx context.Context) bool {
	// Cacheable endpoints not set via. SetCacheableEndpoints
	// return true to default behaviour
	if len(c.cacheableEndpointStatus) == 0 {
		return true
	}

	endpoint := GetContextEndpoint(ctx)
	return c.cacheableEndpointStatus[endpoint]
}

func createCacheMapFromEntites(entities []dosa.DomainObject) map[string]bool {
	set := map[string]bool{}
	for _, e := range entities {
		if e == nil {
			continue
		}
		t, err := dosa.TableFromInstance(e)
		if err != nil {
			continue
		}
		set[t.EntityDefinition.Name] = true
	}
	return set
}

// returns a set of entity names that should skip the cache invalidation on write
func createSkipWriteCacheInvalidateSet(entities []dosa.DomainObject) map[string]bool {
	return createCacheMapFromEntites(entities)
}

// returns a set of entity names that should go through the fallback cache
// All other entities will just return results from origin
func createCachedEntitiesSet(entities []dosa.DomainObject) map[string]bool {
	return createCacheMapFromEntites(entities)
}

// new entity info is being derived from the original to support the structure of a caching connector
func adaptToKeyValue(ei *dosa.EntityInfo) *dosa.EntityInfo {
	adaptedEi := &dosa.EntityInfo{}
	adaptedEi.Ref = ei.Ref
	adaptedEi.Def = &dosa.EntityDefinition{
		Name: ei.Def.Name,
		Key: &dosa.PrimaryKey{
			PartitionKeys: []string{key},
		},
		Columns: []*dosa.ColumnDefinition{
			{Name: value, Type: dosa.Blob},
			{Name: key, Type: dosa.Blob},
		},
	}
	return adaptedEi
}

// used for single entry reads/writes
func createCacheKey(ei *dosa.EntityInfo, values map[string]dosa.FieldValue) interface{} {
	var keys []string
	for pk := range ei.Def.KeySet() {
		if _, ok := values[pk]; ok {
			keys = append(keys, pk)
		}
	}

	// sort the keys so that we encode in a deterministic order
	sort.Strings(keys)
	var orderedKeys []map[string]dosa.FieldValue
	for _, k := range keys {
		orderedKeys = append(orderedKeys, map[string]dosa.FieldValue{k: values[k]})
	}

	return orderedKeys
}

func createContextForFallback(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, 5*time.Minute)
}

// Convert the values returned from Read calls into pointers to those values.
// Everything read from cache is always the absolute value and not a pointer to the value.
// Dosa's registry.go uses reflection to set values onto a dosa entity.
// It works better to de-reference a pointer instead of trying to get the address of a value
// as some things are not easily addressable
func rawRowAsPointers(ei *dosa.EntityInfo, values map[string]dosa.FieldValue) map[string]dosa.FieldValue {
	convertedValues := map[string]dosa.FieldValue{}
	// Using the type of the column as source of truth, convert from an interface{} back
	// to its native type before taking the address.
	// Values from the cache with a type mismatch will not be included in the results
	for colName, colType := range ei.Def.ColumnTypes() {
		if val, ok := values[colName]; ok {
			switch colType {
			case dosa.Blob:
				// do nothing with byte arrays
				convertedValues[colName] = val
			case dosa.TUUID:
				if u, ok := val.(dosa.UUID); ok {
					convertedValues[colName] = &u
				}
			case dosa.String:
				if s, ok := val.(string); ok {
					convertedValues[colName] = &s
				}
			case dosa.Int32:
				if i, ok := val.(int32); ok {
					convertedValues[colName] = &i
				}
			case dosa.Int64:
				if i, ok := val.(int64); ok {
					convertedValues[colName] = &i
				}
			case dosa.Double:
				if d, ok := val.(float64); ok {
					convertedValues[colName] = &d
				}
			case dosa.Timestamp:
				if t, ok := val.(time.Time); ok {
					convertedValues[colName] = &t
				}
			case dosa.Bool:
				if b, ok := val.(bool); ok {
					convertedValues[colName] = &b
				}
			default:
				convertedValues[colName] = &val
			}
		}
	}
	return convertedValues
}

func rawRowsAsPointers(ei *dosa.EntityInfo, values []map[string]dosa.FieldValue) []map[string]dosa.FieldValue {
	convertedValues := []map[string]dosa.FieldValue{}
	for _, v := range values {
		convertedValues = append(convertedValues, rawRowAsPointers(ei, v))
	}
	return convertedValues
}

// Add keys into values map. This mutates the values argument
func populateValuesWithKeys(keys map[string]dosa.FieldValue, values map[string]dosa.FieldValue) {
	if values != nil {
		for k, v := range keys {
			values[k] = v
		}
	}
}
