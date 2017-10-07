package schemaadapter

import (
	"context"
	"encoding/json"

	"github.com/uber-go/dosa"
)

// NewConnector initializes a Schema Reducer Connector
func NewConnector(baseConnector dosa.Connector) dosa.Connector {
	return &Connector{baseConnector}
}

// Connector for schema transformer
type Connector struct {
	dosa.Connector
}

// Upsert calls Next
func (c *Connector) Upsert(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
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
	return c.Connector.Upsert(ctx, eiCopy, newValues)
}

// Remove removes a key
func (c *Connector) Remove(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	// TODO do we assume all the values passed in are strictly keys?
	key, _ := json.Marshal(values)
	eiCopy := adaptToKeyValue(ei)

	newValues := map[string]dosa.FieldValue{
		"key": key,
	}
	return c.Connector.Remove(ctx, eiCopy, newValues)
}

// Range calls Next
func (c *Connector) Range(ctx context.Context, ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition, minimumFields []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	// TODO serializing dosa.Condition array? conditions could be any order

	keysMap := map[string]interface{}{
		"conditions": columnConditions,
		"token":      token,
		"limit":      limit,
	}
	key, _ := json.Marshal(keysMap)

	eiCopy := adaptToKeyValue(ei)
	newValues := map[string]dosa.FieldValue{
		"key": key,
	}

	response, err := c.Connector.Read(ctx, eiCopy, newValues, minimumFields)
	if err != nil {
		return nil, "", err
	}

	type rangeResults struct {
		TokenNext string
		Rows      []map[string]dosa.FieldValue
	}

	unpack := rangeResults{}
	err = json.Unmarshal(response["value"].([]byte), &unpack)
	return unpack.Rows, unpack.TokenNext, err
}

// Scan changes the schema and passes the read onto the redis connector
func (c *Connector) Scan(ctx context.Context, ei *dosa.EntityInfo, minimumFields []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	keysMap := map[string]interface{}{
		"token": token,
		"limit": limit,
	}
	key, _ := json.Marshal(keysMap)
	eiCopy := adaptToKeyValue(ei)
	newValues := map[string]dosa.FieldValue{
		"key": key,
	}

	response, err := c.Connector.Read(ctx, eiCopy, newValues, minimumFields)
	if err != nil {
		return nil, "", err
	}

	type scanResults struct {
		TokenNext string
		Rows      []map[string]dosa.FieldValue
	}
	unpack := scanResults{}
	err = json.Unmarshal(response["value"].([]byte), &unpack)
	return unpack.Rows, unpack.TokenNext, err
}

// Read changes the schema and passes the read onto the redis connector
func (c *Connector) Read(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue, minimumFields []string) (map[string]dosa.FieldValue, error) {
	// Encode read arguments
	// Using json Marshal highly depends on the keys being sorted in the output.
	// Should this behavior change in future versions of golang, cannot use Marshal
	// TODO should we check values map only has partition keys in it?
	key, _ := json.Marshal(values)
	eiCopy := adaptToKeyValue(ei)
	newValues := map[string]dosa.FieldValue{
		"key": key,
	}

	response, err := c.Connector.Read(ctx, eiCopy, newValues, minimumFields)

	if err != nil {
		return nil, err
	}

	// unpack the value
	result := map[string]dosa.FieldValue{}
	err = json.Unmarshal(response["value"].([]byte), &result)
	return result, err
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
