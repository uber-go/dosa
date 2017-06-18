package cassandra

import (
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
)

// Cluster contains the cluster configuration and session info
type Cluster struct {
	config  gocql.ClusterConfig
	session *gocql.Session
}

// NewCluster creates a Cluster instance based on config
func NewCluster(config gocql.ClusterConfig) (*Cluster, error) {
	session, err := config.CreateSession()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create session to Cassandra")
	}

	return &Cluster{
		config:  config,
		session: session,
	}, nil
}

// Close closes the session to cassandra
func (c *Cluster) Close() {
	c.session.Close()
}
