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

// Upsert writes to origin and removes (invalidates) the entry from the fallback
func (c *Connector) Upsert(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	w := func() error {
		newCtx, cancel := createContextForFallback(ctx)
		defer cancel()

		cacheKey := createCacheKey(ei, values, c.encoder)
		adaptedEi := adaptToKeyValue(ei)
		newValues := map[string]dosa.FieldValue{
			key: cacheKey,
		}
		return c.fallback.Remove(newCtx, adaptedEi, newValues)
	}

	originalErr := c.Next.Upsert(ctx, ei, values)
	if originalErr == nil && c.isCacheable(ei) {
		_ = c.cacheWrite(w)
	}
	return originalErr
}

func (c *Connector) Read(ctx context.Context, ei *dosa.EntityInfo, keys map[string]dosa.FieldValue, minimumFields []string) (values map[string]dosa.FieldValue, err error) {
	// Read from source of truth first
	source, sourceErr := c.Next.Read(ctx, ei, keys, dosa.All())
	// Add the primary keys back into results map as dosa.All() does not fetch the keys
	if sourceErr == nil && source != nil {
		for k, v := range keys {
			source[k] = v
		}
	}
	// If we are not caching for this entity, just return
	if !c.isCacheable(ei) {
		return source, sourceErr
	}

	cacheKey := createCacheKey(ei, keys, c.encoder)
	adaptedEi := adaptToKeyValue(ei)
	// if source of truth is good, return result and write result to cache
	if sourceErr == nil {
		w := func() error {
			newCtx, cancel := createContextForFallback(ctx)
			defer cancel()

			cacheValue, err := c.encoder.Encode(source)
			if err != nil {
				return err
			}
			newValues := map[string]dosa.FieldValue{
				key:   cacheKey,
				value: cacheValue}

			return c.fallback.Upsert(newCtx, adaptedEi, newValues)
		}
		_ = c.cacheWrite(w)

		return source, sourceErr
	}
	if dosa.ErrorIsNotFound(sourceErr) {
		return source, sourceErr
	}

	// if source of truth fails, try the fallback. If the fallback fails,
	// return the original error
	value, err := c.getValueFromFallback(ctx, adaptedEi, cacheKey)
	c.logFallback("READ", ei.Def.Name, err)
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
	keysMap := rangeQuery{
		Conditions: dosa.NormalizeConditions(columnConditions),
		Token:      token,
		Limit:      limit,
	}
	cacheKey, err := c.encoder.Encode(keysMap)

	if err != nil {
		cacheKey = []byte{}
	}
	adaptedEi := adaptToKeyValue(ei)

	if sourceErr == nil {
		w := func() error {
			newCtx, cancel := createContextForFallback(ctx)
			defer cancel()

			rangeResults := rangeResults{
				TokenNext: sourceToken,
				Rows:      sourceRows,
			}
			cacheValue, err := c.encoder.Encode(rangeResults)
			if err != nil {
				return err
			}
			newValues := map[string]dosa.FieldValue{
				key:   cacheKey,
				value: cacheValue,
			}

			return c.fallback.Upsert(newCtx, adaptedEi, newValues)
		}
		_ = c.cacheWrite(w)

		return sourceRows, sourceToken, sourceErr
	}
	if dosa.ErrorIsNotFound(sourceErr) {
		return sourceRows, sourceToken, sourceErr
	}

	value, err := c.getValueFromFallback(ctx, adaptedEi, cacheKey)
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
	w := func() error {
		newCtx, cancel := createContextForFallback(ctx)
		defer cancel()
		cacheKey := createCacheKey(ei, keys, c.encoder)
		adaptedEi := adaptToKeyValue(ei)
		return c.fallback.Remove(newCtx, adaptedEi, map[string]dosa.FieldValue{key: cacheKey})
	}

	originalErr := c.Next.Remove(ctx, ei, keys)
	if originalErr == nil && c.isCacheable(ei) {
		_ = c.cacheWrite(w)
	}
	return originalErr
}

func (c *Connector) getValueFromFallback(ctx context.Context, ei *dosa.EntityInfo, keyValue []byte) ([]byte, error) {
	response, err := c.fallback.Read(ctx, ei, map[string]dosa.FieldValue{key: keyValue}, dosa.All())
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
func createCacheKey(ei *dosa.EntityInfo, values map[string]dosa.FieldValue, e encoding.Encoder) []byte {
	keys := []string{}
	for pk := range ei.Def.KeySet() {
		if _, ok := values[pk]; ok {
			keys = append(keys, pk)
		}
	}

	if len(keys) == 0 {
		return []byte{}
	}

	// sort the keys so that we encode in a deterministic order
	sort.Strings(keys)
	orderedKeys := []map[string]dosa.FieldValue{}
	for _, k := range keys {
		orderedKeys = append(orderedKeys, map[string]dosa.FieldValue{k: values[k]})
	}

	cacheKey, err := e.Encode(orderedKeys)
	if err != nil {
		return []byte{}
	}
	return cacheKey
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
