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
	"fmt"
	"time"

	"github.com/gocql/gocql"
)

// Config is a wrapper for the gocql.ClusterConfig, adding
// support for yaml
type Config struct {
	gocql.ClusterConfig `yaml:",inline"`
}

// UnmarshalYAML unmarshals the config into gocql cluster config
func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	internal := &internalCluster{}
	if err := unmarshal(internal); err != nil {
		return err
	}

	c.ClusterConfig = *gocql.NewCluster(internal.Hosts...)
	if internal.CQLVersion != nil {
		c.CQLVersion = *internal.CQLVersion
	}

	if internal.ProtoVersion != nil {
		c.ProtoVersion = *internal.ProtoVersion
	}

	if internal.Timeout != nil {
		c.Timeout = *internal.Timeout
	}

	if internal.ConnectTimeout != nil {
		c.ConnectTimeout = *internal.ConnectTimeout
	}

	if internal.Port != nil {
		c.Port = *internal.Port
	}

	if internal.NumConns != nil {
		c.NumConns = *internal.NumConns
	}

	if internal.Consistency != nil {
		c.Consistency = gocql.ParseConsistency(*internal.Consistency)
	}

	if internal.RetryPolicy != nil {
		c.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: *internal.RetryPolicy}
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
		default:
			return fmt.Errorf("invalid serial consistency %q", *internal.SerialConsistency)
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
