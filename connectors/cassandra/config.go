package cassandra

import (
	"time"

	"code.uber.internal/infra/dosa-gateway/datastore/unsutils"
	"fmt"
	"github.com/gocql/gocql"
)

// Config contains the setting information for cassandra cluster and uns
type Config struct {
	gocql.ClusterConfig `yaml:",inline"`
	UNS                 unsutils.Config `yaml:"uns"`
}

// UnmarshalYAML unmarshals the config into gocql cluster config
func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	internal := &internalCluster{}
	if err := unmarshal(internal); err != nil {
		return err
	}

	c.ClusterConfig = *gocql.NewCluster(internal.Hosts...)
	if internal.CQLVersion != nil {
		c.ClusterConfig.CQLVersion = *internal.CQLVersion
	}

	if internal.ProtoVersion != nil {
		c.ClusterConfig.ProtoVersion = *internal.ProtoVersion
	}

	if internal.Timeout != nil {
		c.ClusterConfig.Timeout = *internal.Timeout
	}

	if internal.ConnectTimeout != nil {
		c.ClusterConfig.ConnectTimeout = *internal.ConnectTimeout
	}

	if internal.Port != nil {
		c.ClusterConfig.Port = *internal.Port
	}

	if internal.NumConns != nil {
		c.ClusterConfig.NumConns = *internal.NumConns
	}

	if internal.Consistency != nil {
		c.ClusterConfig.Consistency = gocql.ParseConsistency(*internal.Consistency)
	}

	if internal.RetryPolicy != nil {
		c.ClusterConfig.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: *internal.RetryPolicy}
	}

	if internal.SocketKeepalive != nil {
		c.ClusterConfig.SocketKeepalive = *internal.SocketKeepalive
	}

	if internal.MaxPreparedStmts != nil {
		c.ClusterConfig.MaxPreparedStmts = *internal.MaxPreparedStmts
	}

	if internal.MaxRoutingKeyInfo != nil {
		c.ClusterConfig.MaxRoutingKeyInfo = *internal.MaxRoutingKeyInfo
	}

	if internal.PageSize != nil {
		c.ClusterConfig.PageSize = *internal.PageSize
	}

	if internal.SerialConsistency != nil {
		switch *internal.SerialConsistency {
		case gocql.Serial.String():
			c.ClusterConfig.SerialConsistency = gocql.Serial
		case gocql.LocalSerial.String():
			c.ClusterConfig.SerialConsistency = gocql.LocalSerial
		}
	}

	if internal.HostSelectionPolicy != nil {
		switch *internal.HostSelectionPolicy {
		case "RoundRobinHostPolicy":
			c.ClusterConfig.PoolConfig = gocql.PoolConfig{HostSelectionPolicy: gocql.RoundRobinHostPolicy()}
		case "TokenAwareHostPolicy":
			c.ClusterConfig.PoolConfig = gocql.PoolConfig{HostSelectionPolicy: gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())}
		default:
			return fmt.Errorf("unrecognized host selection policy: %s", *internal.HostSelectionPolicy)
		}
	}

	if internal.DataCenter != nil {
		c.ClusterConfig.HostFilter = gocql.DataCentreHostFilter(*internal.DataCenter)
	}
	return nil
}

type internalCluster struct {
	Hosts               []string       `yaml:"hosts"`
	CQLVersion          *string        `yaml:"cqlVersion"`
	ProtoVersion        *int           `yaml:"protoVersion"`
	Timeout             *time.Duration `yaml:"timeout"`
	ConnectTimeout      *time.Duration `yaml:"connectTimeout"`
	Port                *int           `yaml:"port"`
	NumConns            *int           `yaml:"numConns"`
	Consistency         *string        `yaml:"consistency"`
	RetryPolicy         *int           `yaml:"retryPolicy"`
	SocketKeepalive     *time.Duration `yaml:"socketKeepalive"`
	MaxPreparedStmts    *int           `yaml:"maxPreparedStmts"`
	MaxRoutingKeyInfo   *int           `yaml:"maxRoutingKeyInfo"`
	PageSize            *int           `yaml:"pageSize"`
	SerialConsistency   *string        `yaml:"serialConsistency"`
	HostSelectionPolicy *string        `yaml:"hostSelectionPolicy"`
	DataCenter          *string        `yaml:"dataCenter"`
}
