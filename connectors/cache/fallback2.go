package cache

import (
	"context"

	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/base"
	"strings"
	"strconv"
	"encoding/json"
	"errors"
)

type rangeResults struct {
	TokenNext string
	Rows      []map[string]dosa.FieldValue
}

type rangeQuery struct {
	Token string
	Limit int
	Conditions map[string][]*dosa.Condition `json:",omitempty"`
}

func NewConnector2(origin dosa.Connector, fallback dosa.Connector) dosa.Connector {
	return &Connector2 {
		origin:   origin,
		fallback: fallback,
	}

}

type Connector2 struct {
	base.Connector
	origin   dosa.Connector
	fallback dosa.Connector
}

// Upsert calls Next
func (c *Connector2) Upsert(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	go func() {
		// Get all the partition keys and their values
		keysMap := map[string]dosa.FieldValue{}
		for pk := range ei.Def.PartitionKeySet() {
			if value, ok := values[pk]; ok {
				keysMap[pk] = value
			}
		}

		key, _ := json.Marshal(keysMap)
		value, _ := json.Marshal(values)
		eiCopy := adaptToKeyValue(ei)
		newValues := map[string]dosa.FieldValue{
			"key":   key,
			"value": value,
		}
		return c.fallback.Upsert(ctx, eiCopy, newValues)
	}()
	return c.origin.Upsert(ctx, ei, values)
}

func (c *Connector2) Read(ctx context.Context, ei *dosa.EntityInfo, keys map[string]dosa.FieldValue, minimumFields []string) (values map[string]dosa.FieldValue, err error) {
	// Read from source of truth first
	source, sourceErr := c.origin.Read(ctx, ei, keys, dosa.All())

	// TODO  validate that the keys parameter strictly has partition keys?
	cacheKey, _ := json.Marshal(keys)
	eiCopy := adaptToKeyValue(ei)
	// if source of truth is good, return result.
	if sourceErr == nil {
		// Write result to cache
		go func() {
			cacheValue, _ := json.Marshal(source)
			newValues := map[string]dosa.FieldValue{
				"key": cacheKey,
				"value": cacheValue}
			c.fallback.Upsert(ctx, eiCopy, newValues)
		}()

		return source, sourceErr
	}
	// if source of truth fails, try the fallback. If the fallback fails,
	// return the original error
	value, err :=c.getValueFromFallback(ctx, eiCopy, cacheKey)
	if err != nil {
		return source, sourceErr
	}
	result := map[string]dosa.FieldValue{}
	err = json.Unmarshal(value, &result)
	if err != nil {
		return source, sourceErr
	}
	return result, err
}

func (c *Connector2) Range(ctx context.Context, ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition, minimumFields []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	// Read from source of truth first
	sourceRows, sourceToken, sourceErr := c.origin.Range(ctx, ei, columnConditions, dosa.All(), token, limit)

	// TODO serializing dosa.Condition array? conditions could be any order
	keysMap := rangeQuery{
		Conditions: columnConditions,
		Token:      token,
		Limit:      limit,
	}
	key, _ := json.Marshal(keysMap)
	eiCopy := adaptToKeyValue(ei)

	if sourceErr == nil {
		// 	// if source of truth is good, return result and write result to cache
		go func() {
			rangeResults := rangeResults{
				TokenNext: sourceToken,
				Rows: sourceRows,
			}
			value, _ := json.Marshal(rangeResults)
			newValues := map[string]dosa.FieldValue{
				"key": key,
				"value": value,
			}
			c.fallback.Upsert(ctx, eiCopy, newValues)
		}()
		return sourceRows, sourceToken, sourceErr
	}
	// if source of truth fails, try the fallback

	value, err :=c.getValueFromFallback(ctx, eiCopy, key)
	if err != nil {
		return sourceRows, sourceToken, sourceErr
	}
	unpack := rangeResults{}
	err = json.Unmarshal(value, &unpack)
	if err != nil {
		return sourceRows, sourceToken, sourceErr
	}
	return unpack.Rows, unpack.TokenNext, err
}

// Scan returns scan result from origin.
func (c *Connector2) Scan(ctx context.Context, ei *dosa.EntityInfo, minimumFields []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	// Scan will just call range with no conditions
	return c.Range(ctx, ei, nil, minimumFields, token, limit)
}

func (c *Connector2) getValueFromFallback(ctx context.Context, ei *dosa.EntityInfo, key []byte) ([]byte, error) {
	// if source of truth fails, try the fallback. If the fallback fails,
	// return the original error
	response, err := c.fallback.Read(ctx, ei, map[string]dosa.FieldValue{"key": key}, dosa.All())
	if err != nil {
		return nil, err
	}

	// unpack the value
	value, ok := response["value"].([]byte)
	if !ok {
		return nil, errors.New("No value in cache for key")
	}
	return value, nil
}

func adaptToKeyValue(ei *dosa.EntityInfo) *dosa.EntityInfo {
	eiCopy := &dosa.EntityInfo{}
	eiCopy.Ref = ei.Ref
	eiCopy.Def = &dosa.EntityDefinition{
		Name: ei.Def.Name,
		Key: &dosa.PrimaryKey{
			PartitionKeys: []string{"key"},
		},
		Columns: []*dosa.ColumnDefinition{
			{Name: "value", Type: dosa.Blob},
			{Name: "key", Type: dosa.Blob},
		},
		// Indexes is null
	}
	return eiCopy
}
