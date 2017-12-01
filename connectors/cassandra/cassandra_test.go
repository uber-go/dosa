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

package cassandra_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/gocql/gocql"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/cassandra"
	_ "github.com/uber-go/dosa/connectors/cassandra"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa/testentity"
)

func TestNewConnector(t *testing.T) {
	c, err := cassandra.NewConnector(
		gocql.NewCluster("127.0.0.1:"+strconv.Itoa(cassandra.CassandraPort)),
		&cassandra.UseNamePrefix{},
		nil,
	)
	if !assert.NoError(t, err, "Error connecting to Cassandra") {
		t.Fatal(err)
	}

	ctx := context.Background()
	ei, _ := dosa.TableFromInstance(&testentity.TestEntity{})
	sr := dosa.SchemaRef{Scope: testScope, NamePrefix: "example"}

	err = c.Upsert(ctx, &dosa.EntityInfo{Ref: &sr, Def: &ei.EntityDefinition}, map[string]dosa.FieldValue{
		"an_uuid_key": dosa.UUID("c778ba9e-a241-471c-9b5b-4b4c1ef1c5b7"),
		"strkey":      "test",
		"int64key":    int64(11),
	})

	if err != nil {
		t.Fatal(err)
	}
}

// This test verifies that we can connect to a local database when no configuration is specified
func TestNewConnectorWithNilConfig(t *testing.T) {
	c, err := dosa.GetConnector("cassandra", nil)
	assert.NoError(t, err)
	assert.IsType(t, &cassandra.Connector{}, c)
	c.Shutdown()
}
func TestNewConnectorWithKeyspaceMapper(t *testing.T) {
	c, err := dosa.GetConnector("cassandra", map[string]interface{}{"keyspace_mapper": &cassandra.UseNamePrefix{}})
	assert.NoError(t, err)
	assert.IsType(t, &cassandra.Connector{}, c)
	assert.IsType(t, &cassandra.UseNamePrefix{}, c.(*cassandra.Connector).KsMapper)
	c.Shutdown()
}

func TestNewConnectorWithBadHost(t *testing.T) {
	_, err := dosa.GetConnector("cassandra", map[string]interface{}{"yaml": `hosts: ["127.0.0.254"]`})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "127.0.0.254")
}

func TestNewConnectorWithBadYaml(t *testing.T) {
	_, err := dosa.GetConnector("cassandra", map[string]interface{}{"yaml": "notvalidyaml"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "gocql.ClusterConfig")
}

func TestNewConnectorWithBadKeyspaceMapper(t *testing.T) {
	_, err := dosa.GetConnector("cassandra", map[string]interface{}{"keyspace_mapper": 42})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "type int")
}
