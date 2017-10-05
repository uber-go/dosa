package cache

import (
	"context"

	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/base"
)

func NewConnector(origin dosa.Connector, fallback dosa.Connector) dosa.Connector {
	return &Connector{
		origin:   origin,
		fallback: fallback,
	}

}

type Connector struct {
	base.Connector
	origin   dosa.Connector
	fallback dosa.Connector
}

// Upsert calls Next
func (c *Connector) Upsert(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	go c.fallback.Upsert(ctx, ei, values)
	return c.origin.Upsert(ctx, ei, values)
}

func (c *Connector) Read(ctx context.Context, ei *dosa.EntityInfo, keys map[string]dosa.FieldValue, minimumFields []string) (values map[string]dosa.FieldValue, err error) {
	// Read from source of truth first
	source, sourceErr := c.origin.Read(ctx, ei, keys, minimumFields)
	// if source of truth is good, return result.
	if sourceErr == nil {
		// Write result to cache
		go c.fallback.Upsert(ctx, ei, source)
		return source, sourceErr
	}
	// if source of truth fails, try the fallback
	return c.fallback.Read(ctx, ei, keys, minimumFields)
}

func (c *Connector) Range(ctx context.Context, ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition, minimumFields []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	// Read from source of truth first
	sourceRows, sourceToken, sourceErr := c.origin.Range(ctx, ei, columnConditions, minimumFields, token, limit)
	// if source of truth is good, return result.
	if sourceErr == nil {
		// Write result to cache
		go func() {
			adaptedEi := &dosa.EntityInfo{}
			adaptedEi.Ref = ei.Ref
			adaptedEi.Def = &dosa.EntityDefinition{
				Name: ei.Def.Name,
				Key: &dosa.PrimaryKey{
					PartitionKeys: []string{"conditions", "token", "limit"},
				},
				Columns: []*dosa.ColumnDefinition{
					{Name: "conditions", Type: dosa.Blob},
					{Name: "token", Type: dosa.Blob},
					{Name: "limit", Type: dosa.Blob},
					{Name: "tokenNext", Type: dosa.Blob},
					{Name: "rows", Type: dosa.Blob},
				},
				// Indexes is null
			}

			values := map[string]dosa.FieldValue{
				"conditions": columnConditions,
				"token":      token,
				"limit":      limit,
				"tokenNext":  sourceToken,
				"rows":       sourceRows,
			}

			c.fallback.Upsert(ctx, adaptedEi, values)
		}()
		return sourceRows, sourceToken, sourceErr
	}
	// if source of truth fails, try the fallback
	return c.fallback.Range(ctx, ei, columnConditions, minimumFields, token, limit)
}

func (c *Connector) Scan(ctx context.Context, ei *dosa.EntityInfo, minimumFields []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	// Read from source of truth first
	sourceRows, sourceToken, sourceErr := c.origin.Scan(ctx, ei, minimumFields, token, limit)
	// if source of truth is good, return result.
	if sourceErr == nil {
		// Write result to cache
		go func() {
			adaptedEi := &dosa.EntityInfo{}
			adaptedEi.Ref = ei.Ref
			adaptedEi.Def = &dosa.EntityDefinition{
				Name: ei.Def.Name,
				Key: &dosa.PrimaryKey{
					PartitionKeys: []string{"token", "limit"},
				},
				Columns: []*dosa.ColumnDefinition{
					{Name: "token", Type: dosa.Blob},
					{Name: "limit", Type: dosa.Blob},
					{Name: "tokenNext", Type: dosa.Blob},
					{Name: "rows", Type: dosa.Blob},
				},
				// Indexes is null
			}

			values := map[string]dosa.FieldValue{
				"token":     token,
				"limit":     limit,
				"tokenNext": sourceToken,
				"rows":      sourceRows,
			}

			c.fallback.Upsert(ctx, adaptedEi, values)
		}()
		return sourceRows, sourceToken, sourceErr
	}
	// if source of truth fails, try the fallback
	return c.fallback.Range(ctx, ei, minimumFields, token, limit)
}
