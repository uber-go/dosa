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

// NewConnector creates a fallback cache connector
func NewConnector(origin, fallback dosa.Connector, scope metrics.Scope, entities ...dosa.DomainObject) *Connector {
	return newConnector(origin, fallback, scope, encoding.NewGobEncoder(), entities...)
}

func newConnector(origin, fallback dosa.Connector, scope metrics.Scope, encoder encoding.Encoder, entities ...dosa.DomainObject) *Connector {
	bc := base.Connector{Next: origin}
	set := createCachedEntitiesSet(entities)
	return &Connector{
		Connector:         bc,
		fallback:          fallback,
		encoder:           encoder,
		cacheableEntities: set,
		stats:             scope,
	}
}

// Connector is a fallback cache connector
type Connector struct {
	base.Connector
	fallback          dosa.Connector
	encoder           encoding.Encoder
	cacheableEntities map[string]bool
	mux               sync.Mutex
	stats             metrics.Scope
	// Used primarily for testing so that nothing is called in a goroutine
	synchronous bool
}

// SetCachedEntities sets the entities that will write and read from the fallback
func (c *Connector) SetCachedEntities(entities ...dosa.DomainObject) {
	c.mux.Lock()
	defer c.mux.Unlock()
	c.cacheableEntities = createCachedEntitiesSet(entities)
}

// Upsert removes (invalidates) the entry from the fallback
func (c *Connector) Upsert(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	if c.isCacheable(ei) {
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
	if !c.isCacheable(ei) {
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

func (c *Connector) write(ctx context.Context, ei *dosa.EntityInfo, keys map[string]dosa.FieldValue, source map[string]dosa.FieldValue) (err error) {
	cacheKey := createCacheKey(ei, keys)
	return c.writeKeyValueToFallback(ctx, ei, cacheKey, source)
}

// If source had an error, try the fallback. If the fallback fails, return the original error
func (c *Connector) read(
	ctx context.Context,
	ei *dosa.EntityInfo,
	keys map[string]dosa.FieldValue,
	source map[string]dosa.FieldValue,
	sourceErr error, methodName string,
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

// Range returns range from origin, reverts to fallback if origin fails
func (c *Connector) Range(ctx context.Context, ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition, minimumFields []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	sourceRows, sourceToken, sourceErr := c.Next.Range(ctx, ei, columnConditions, dosa.All(), token, limit)
	if !c.isCacheable(ei) {
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
	if dosa.ErrorIsNotFound(sourceErr) {
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
	// Scan will just call range with no conditions
	return c.Range(ctx, ei, nil, minimumFields, token, limit)
}

// Remove deletes an entry
func (c *Connector) Remove(ctx context.Context, ei *dosa.EntityInfo, keys map[string]dosa.FieldValue) error {
	if c.isCacheable(ei) {
		w := func() error {
			return c.removeValueFromFallback(ctx, ei, createCacheKey(ei, keys))
		}
		_ = c.cacheWrite(w)
	}

	return c.Next.Remove(ctx, ei, keys)
}

// MultiUpsert deletes the entries getting upserted from the fallback
func (c *Connector) MultiUpsert(ctx context.Context, ei *dosa.EntityInfo, multiValues []map[string]dosa.FieldValue) (result []error, err error) {
	if c.isCacheable(ei) {
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
	if c.isCacheable(ei) {
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

func (c *Connector) isCacheable(ei *dosa.EntityInfo) bool {
	return c.cacheableEntities[ei.Def.Name]
}

// returns a set of entity names that should go through the fallback cache
// All other entities will just return results from origin
func createCachedEntitiesSet(entities []dosa.DomainObject) map[string]bool {
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
	keys := []string{}
	for pk := range ei.Def.KeySet() {
		if _, ok := values[pk]; ok {
			keys = append(keys, pk)
		}
	}

	// sort the keys so that we encode in a deterministic order
	sort.Strings(keys)
	orderedKeys := []map[string]dosa.FieldValue{}
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
