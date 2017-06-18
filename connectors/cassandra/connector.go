package cassandra

import (
	"fmt"

	"code.uber.internal/infra/dosa-gateway/datastore/common"
	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/base"
)

// Connector implements the data access to the cassandra cluster
type Connector struct {
	base.Connector
	clusters map[string]*Cluster
	resolver common.Resolver
}

// NewConnector creates an instance of Connector
func NewConnector(clusterConfigs map[string]Config, r common.Resolver, next dosa.Connector) (*Connector, error) {
	clusters := make(map[string]*Cluster)
	for name, clusterConfig := range clusterConfigs {
		var err error
		clusters[name], err = NewCluster(clusterConfig.ClusterConfig)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("failed to connect to Cassandra cluster %s", name))
		}
	}
	c := &Connector{
		clusters: clusters,
		resolver: r,
	}
	c.Connector.Next = next
	return c, nil
}

// Shutdown cleans up the connector
func (c *Connector) Shutdown() error {
	for _, c := range c.clusters {
		c.Close()
	}
	return nil
}

func (c *Connector) getCluster(cluster string) (*Cluster, error) {
	cl, ok := c.clusters[cluster]
	if !ok {
		return nil, fmt.Errorf("can't find cluster %s", cluster)
	}
	return cl, nil
}

func (c *Connector) resolve(ei *dosa.EntityInfo) (*Cluster, string, string, error) {
	clusterName, keyspace := c.resolver.Resolve(ei.Ref.Scope, ei.Ref.NamePrefix)

	table := ei.Def.Name

	cluster, err := c.getCluster(clusterName)
	if err != nil {
		return nil, "", "", err
	}
	return cluster, keyspace, table, nil
}

type sortedFields []string

func (list sortedFields) Len() int { return len(list) }

func (list sortedFields) Swap(i, j int) { list[i], list[j] = list[j], list[i] }

func (list sortedFields) Less(i, j int) bool {
	return list[i] < list[j]
}
