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

package cassandra

import (
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/base"
	"gopkg.in/yaml.v2"
)

const localhostipv4 = "127.0.0.1"

// Connector implements the data access to the cassandra cluster
type Connector struct {
	base.Connector
	config   *gocql.ClusterConfig
	Session  *gocql.Session
	KsMapper KeyspaceMapper
}

// NewConnector creates an instance of Connector
func NewConnector(clusterConfig *gocql.ClusterConfig, ksMapper KeyspaceMapper, next dosa.Connector) (dosa.Connector, error) {
	session, err := clusterConfig.CreateSession()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create session to Cassandra")
	}

	c := Connector{
		config:   clusterConfig,
		KsMapper: ksMapper,
		Session:  session,
	}
	return &c, nil
}

// Shutdown cleans up the connector
func (c *Connector) Shutdown() error {
	c.Session.Close()
	return nil
}

func init() {
	dosa.RegisterConnector("cassandra", func(args dosa.CreationArgs) (dosa.Connector, error) {
		// set up defaults for configuration
		config := gocql.NewCluster(localhostipv4)
		keyspaceMapper := KeyspaceMapper(DefaultKeyspaceMapper)

		// parameters were provided, so lets use them
		if args != nil {
			// TODO the most common case is hosts-only so make that easier
			if configYaml, ok := args["yaml"]; ok {
				if err := yaml.Unmarshal([]byte(configYaml.(string)), config); err != nil {
					return nil, errors.Wrap(err, "Unable to parse yaml configuration")
				}
			}

			if mapper, ok := args["keyspace_mapper"]; ok {
				if m, ok := mapper.(KeyspaceMapper); ok {
					keyspaceMapper = m
				} else {
					return nil, errors.Errorf("provided keyspace_mapper (type %T) does not implement KeyspaceMapper", mapper)
				}
			}
		}

		connector, err := NewConnector(config, keyspaceMapper, nil)
		return connector, err
	})
}
