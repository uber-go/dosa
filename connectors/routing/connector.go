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

package routing

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/base"
)

// PluginFunc is a plugin function that takes scope, namePrefix and operation name,
// then gives wanted scope and namePrefix
type PluginFunc func(scope, namePrefix, opName string) (string, string, error)

// Connector holds a slice of configured connectors to route to
type Connector struct {
	base.Connector
	// config connector slice is sorted in a manner:
	// for the value of Config name prefix, strict string without "*" always comes first,
	// and then string with "*" suffix (glob match) and pure "*".
	// There shouldn't be any scope with a prefix "*" like "*.service.v1"
	config     Config
	connectors map[string]dosa.Connector
	// PluginFunc is a plugin that passes in
	// the scope, namePrefix and operation name, returns wanted scope and namePrefix
	PluginFunc PluginFunc
}

// NewConnector initializes the Connector
// connectorMap has a key of connectorName, and the value is a dosa.connector instance
func NewConnector(cfg Config, connectorMap map[string]dosa.Connector, plugin PluginFunc) *Connector {
	return &Connector{
		connectors: connectorMap,
		config:     cfg,
		PluginFunc: plugin,
	}
}

// get connector by scope, namePrefix and operation name provided
func (rc *Connector) getConnector(scope string, namePrefix string, opName string) (_ dosa.Connector, err error) {
	if rc.PluginFunc != nil {
		// plugin operation
		// plugin should always be first considered if it exists
		scope, namePrefix, err = rc.PluginFunc(scope, namePrefix, opName)
		if err != nil {
			return nil, errors.Wrap(err, "failed to execute getConnector due to Plugin function error")
		}
	}
	return rc._getConnector(scope, namePrefix)
}

// if no specific scope is found,
// Connector routes to the default scope that defined in routing config yaml file
func (rc *Connector) _getConnector(scope, namePrefix string) (dosa.Connector, error) {
	router := rc.config.FindRouter(scope, namePrefix)

	c, ok := rc.connectors[router.Connector]
	if !ok {
		return nil, fmt.Errorf("can't find %q connector", router.Connector)
	}

	return c, nil
}

// CreateIfNotExists selects corresponding connector
func (rc *Connector) CreateIfNotExists(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	connector, err := rc.getConnector(ei.Ref.Scope, ei.Ref.NamePrefix, "CreateIfNotExists")
	if err != nil {
		return err
	}
	return connector.CreateIfNotExists(ctx, ei, values)
}

// Read selects corresponding connector
func (rc *Connector) Read(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue, minimumFields []string) (map[string]dosa.FieldValue, error) {
	connector, err := rc.getConnector(ei.Ref.Scope, ei.Ref.NamePrefix, "Read")
	if err != nil {
		return nil, err
	}
	return connector.Read(ctx, ei, values, minimumFields)
}

// MultiRead selects corresponding connector
func (rc *Connector) MultiRead(ctx context.Context, ei *dosa.EntityInfo, values []map[string]dosa.FieldValue, minimumFields []string) ([]*dosa.FieldValuesOrError, error) {
	connector, err := rc.getConnector(ei.Ref.Scope, ei.Ref.NamePrefix, "MultiRead")
	if err != nil {
		return nil, err
	}
	return connector.MultiRead(ctx, ei, values, minimumFields)
}

// Upsert selects corresponding connector
func (rc *Connector) Upsert(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	connector, err := rc.getConnector(ei.Ref.Scope, ei.Ref.NamePrefix, "Upsert")
	if err != nil {
		return err
	}
	return connector.Upsert(ctx, ei, values)
}

// MultiUpsert selects corresponding connector
func (rc *Connector) MultiUpsert(ctx context.Context, ei *dosa.EntityInfo, values []map[string]dosa.FieldValue) ([]error, error) {
	connector, err := rc.getConnector(ei.Ref.Scope, ei.Ref.NamePrefix, "MultiUpsert")
	if err != nil {
		return nil, err
	}
	return connector.MultiUpsert(ctx, ei, values)
}

// Remove selects corresponding connector
func (rc *Connector) Remove(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	connector, err := rc.getConnector(ei.Ref.Scope, ei.Ref.NamePrefix, "Remove")
	if err != nil {
		// here returns err because connector is not found
		return err
	}
	// original remove method should never return err
	return connector.Remove(ctx, ei, values)
}

// RemoveRange selects corresponding connector
func (rc *Connector) RemoveRange(ctx context.Context, ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition) error {
	connector, err := rc.getConnector(ei.Ref.Scope, ei.Ref.NamePrefix, "RemoveRange")
	if err != nil {
		return err
	}
	return connector.RemoveRange(ctx, ei, columnConditions)
}

// MultiRemove selects corresponding connector
func (rc *Connector) MultiRemove(ctx context.Context, ei *dosa.EntityInfo, multiValues []map[string]dosa.FieldValue) ([]error, error) {
	connector, err := rc.getConnector(ei.Ref.Scope, ei.Ref.NamePrefix, "MultiRemove")
	if err != nil {
		return nil, err
	}
	return connector.MultiRemove(ctx, ei, multiValues)
}

// Range selects corresponding connector
func (rc *Connector) Range(ctx context.Context, ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition, minimumFields []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	connector, err := rc.getConnector(ei.Ref.Scope, ei.Ref.NamePrefix, "Range")
	if err != nil {
		return nil, "", err
	}
	return connector.Range(ctx, ei, columnConditions, minimumFields, token, limit)
}

// Scan selects corresponding connector
func (rc *Connector) Scan(ctx context.Context, ei *dosa.EntityInfo, minimumFields []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	connector, err := rc.getConnector(ei.Ref.Scope, ei.Ref.NamePrefix, "Scan")
	if err != nil {
		return nil, "", err
	}
	return connector.Scan(ctx, ei, minimumFields, token, limit)
}

// CheckSchema calls selected connector
func (rc *Connector) CheckSchema(ctx context.Context, scope, namePrefix string, ed []*dosa.EntityDefinition) (int32, error) {
	connector, err := rc.getConnector(scope, namePrefix, "CheckSchema")
	if err != nil {
		return dosa.InvalidVersion, base.ErrNoMoreConnector{}
	}
	return connector.CheckSchema(ctx, scope, namePrefix, ed)
}

// UpsertSchema calls selected connector
func (rc *Connector) UpsertSchema(ctx context.Context, scope, namePrefix string, ed []*dosa.EntityDefinition) (*dosa.SchemaStatus, error) {
	connector, err := rc.getConnector(scope, namePrefix, "UpsertSchema")
	if err != nil {
		return nil, base.ErrNoMoreConnector{}
	}
	return connector.UpsertSchema(ctx, scope, namePrefix, ed)
}

// CheckSchemaStatus calls selected connector
func (rc *Connector) CheckSchemaStatus(ctx context.Context, scope string, namePrefix string, version int32) (*dosa.SchemaStatus, error) {
	connector, err := rc.getConnector(scope, namePrefix, "CheckSchemaStatus")
	if err != nil {
		return nil, base.ErrNoMoreConnector{}
	}
	return connector.CheckSchemaStatus(ctx, scope, namePrefix, version)
}

// CreateScope calls selected connector
func (rc *Connector) CreateScope(ctx context.Context, scope string) error {
	// will fall to default connector
	connector, err := rc.getConnector(scope, "", "CreateScope")
	if err != nil {
		return base.ErrNoMoreConnector{}
	}
	return connector.CreateScope(ctx, scope)
}

// TruncateScope calls selected connector
func (rc *Connector) TruncateScope(ctx context.Context, scope string) error {
	// will fall to default connector
	connector, err := rc.getConnector(scope, "", "TruncateScope")
	if err != nil {
		return base.ErrNoMoreConnector{}
	}
	return connector.TruncateScope(ctx, scope)
}

// DropScope calls selected connector
func (rc *Connector) DropScope(ctx context.Context, scope string) error {
	// will fall to default connector
	connector, err := rc.getConnector(scope, "", "DropScope")
	if err != nil {
		return base.ErrNoMoreConnector{}
	}
	return connector.DropScope(ctx, scope)
}

// ScopeExists calls selected connector
func (rc *Connector) ScopeExists(ctx context.Context, scope string) (bool, error) {
	// will fall to default connector
	connector, err := rc.getConnector(scope, "", "ScopeExists")
	if err != nil {
		return false, base.ErrNoMoreConnector{}
	}
	return connector.ScopeExists(ctx, scope)
}

// Shutdown shut down all connectors that routing connector talks to
func (rc *Connector) Shutdown() error {
	hasError := false
	rConnErr := errors.New("failed to shutdown")
	for _, c := range rc.connectors {
		err := c.Shutdown()
		if err != nil {
			// save errors here, continue to shut down other connectors
			hasError = true
			err = errors.Wrap(rConnErr, err.Error())
			continue
		}
	}

	if hasError {
		return rConnErr
	}
	return nil
}
