package cache

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/base"
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
func NewConnector(origin dosa.Connector, fallback dosa.Connector, encoder Encoder, scope metrics.Scope, entities ...dosa.DomainObject) *Connector {
	bc := base.Connector{Next: origin}
	set := createCachedEntitiesSet(entities)
	return &Connector{
		Connector:         bc,
		origin:            origin,
		fallback:          fallback,
		encoder:           encoder,
		cacheableEntities: set,
		stats:             scope,
	}
}

// Connector is a fallback cache connector
type Connector struct {
	base.Connector
	origin            dosa.Connector
	fallback          dosa.Connector
	encoder           Encoder
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

// Upsert dual writes to the fallback cache and the origin
func (c *Connector) Upsert(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	w := func() error {
		newCtx, cancel := createContextForFallback(ctx)
		defer cancel()

		cacheKey := createCacheKey(ei, values, c.encoder)
		cacheValue, err := c.encoder.Encode(values)
		if err != nil {
			return err
		}
		adaptedEi := adaptToKeyValue(ei)
		newValues := map[string]dosa.FieldValue{
			key:   cacheKey,
			value: cacheValue,
		}
		return c.fallback.Upsert(newCtx, adaptedEi, newValues)
	}
	if c.isCacheable(ei) {
		_ = c.cacheWrite(w)
	}

	return c.origin.Upsert(ctx, ei, values)
}

func (c *Connector) Read(ctx context.Context, ei *dosa.EntityInfo, keys map[string]dosa.FieldValue, minimumFields []string) (values map[string]dosa.FieldValue, err error) {
	// Read from source of truth first
	source, sourceErr := c.origin.Read(ctx, ei, keys, dosa.All())
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
	// if source of truth fails, try the fallback. If the fallback fails,
	// return the original error
	value, err := c.getValueFromFallback(ctx, adaptedEi, cacheKey)
	c.logFallback("READ", err)
	if err != nil {
		return source, sourceErr
	}
	result := map[string]dosa.FieldValue{}
	err = c.encoder.Decode(value, &result)
	if err != nil {
		return source, sourceErr
	}
	return result, err
}

// Range returns range from origin, reverts to fallback if origin fails
func (c *Connector) Range(ctx context.Context, ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition, minimumFields []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	sourceRows, sourceToken, sourceErr := c.origin.Range(ctx, ei, columnConditions, dosa.All(), token, limit)
	if !c.isCacheable(ei) {
		return sourceRows, sourceToken, sourceErr
	}
	keysMap := rangeQuery{
		Conditions: dosa.NormalizeConditions(columnConditions),
		Token:      token,
		Limit:      limit,
	}
	cacheKey, _ := c.encoder.Encode(keysMap)
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
	value, err := c.getValueFromFallback(ctx, adaptedEi, cacheKey)
	c.logFallback("RANGE", err)
	if err != nil {
		return sourceRows, sourceToken, sourceErr
	}
	unpack := rangeResults{}
	err = c.encoder.Decode(value, &unpack)
	if err != nil {
		return sourceRows, sourceToken, sourceErr
	}
	return unpack.Rows, unpack.TokenNext, err
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

	if c.isCacheable(ei) {
		_ = c.cacheWrite(w)
	}

	return c.origin.Remove(ctx, ei, keys)
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

func (c *Connector) logFallback(method string, err error) {
	if c.stats != nil {
		s := c.stats.SubScope("fallback").Tagged(map[string]string{"method": method})
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
func createCacheKey(ei *dosa.EntityInfo, values map[string]dosa.FieldValue, e Encoder) []byte {
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
