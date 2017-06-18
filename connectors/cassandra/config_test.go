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
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
)

func TestConfigUnmarshalYAML(t *testing.T) {
	str := `hosts: ["127.0.0.1"]
cqlVersion: 3.0.1
protoVersion:      3
timeout:           2000ms
connectTimeout:    1000ms
port:              9091
numConns:          5
consistency:       ONE
retryPolicy:       1
socketKeepalive:   100ms
maxPreparedStmts:  5000
maxRoutingKeyInfo: 5000
pageSize:          2000
serialConsistency:  SERIAL
hostSelectionPolicy: TokenAwareHostPolicy
dataCenter: dca1
`
	config := Config{}
	err := yaml.Unmarshal([]byte(str), &config)
	assert.NoError(t, err)
	expected := gocql.NewCluster("127.0.0.1")
	timeout, _ := time.ParseDuration("2000ms")
	expected.CQLVersion = "3.0.1"
	expected.Timeout = timeout
	connectTimeout, _ := time.ParseDuration("1000ms")
	expected.ConnectTimeout = connectTimeout
	expected.ProtoVersion = 3
	expected.Port = 9091
	expected.NumConns = 5
	expected.Consistency = gocql.One
	expected.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 1}
	socketKeepalive, _ := time.ParseDuration("100ms")
	expected.SocketKeepalive = socketKeepalive
	expected.MaxRoutingKeyInfo = 5000
	expected.MaxPreparedStmts = 5000
	expected.PageSize = 2000
	expected.SerialConsistency = gocql.Serial
	expected.PoolConfig = gocql.PoolConfig{HostSelectionPolicy: gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())}
	expected.HostFilter = gocql.DataCentreHostFilter("dca1")
	assert.Equal(t, expected.PoolConfig, config.ClusterConfig.PoolConfig)
	assert.Equal(t, expected.RetryPolicy, config.ClusterConfig.RetryPolicy)
	assert.Equal(t, expected.CQLVersion, config.ClusterConfig.CQLVersion)
	assert.Equal(t, expected.ProtoVersion, config.ClusterConfig.ProtoVersion)
	assert.Equal(t, expected.Timeout, config.ClusterConfig.Timeout)
	assert.Equal(t, expected.MaxPreparedStmts, config.ClusterConfig.MaxPreparedStmts)
	assert.Equal(t, expected.MaxRoutingKeyInfo, config.ClusterConfig.MaxRoutingKeyInfo)
	assert.Equal(t, expected.NumConns, config.ClusterConfig.NumConns)
	assert.Equal(t, expected.SerialConsistency, config.ClusterConfig.SerialConsistency)
	assert.Equal(t, expected.SocketKeepalive, config.ClusterConfig.SocketKeepalive)
	assert.Equal(t, expected.PageSize, config.ClusterConfig.PageSize)
	// no way I can test this. The hostfilter is an anonymous func
	//assert.Equal(t, expected.HostFilter, config.ClusterConfig.HostFilter)
	assert.Equal(t, expected.Port, config.ClusterConfig.Port)
	assert.Equal(t, expected.ConnectTimeout, config.ClusterConfig.ConnectTimeout)
	assert.Equal(t, expected.Hosts, config.ClusterConfig.Hosts)
	assert.Equal(t, expected.Consistency, config.ClusterConfig.Consistency)
}

func TestSerialConsistency(t *testing.T) {
	config := Config{}
	err := yaml.Unmarshal([]byte("serialConsistency: LOCAL_SERIAL"), &config)
	assert.NoError(t, err)
	assert.Equal(t, gocql.LocalSerial, config.ClusterConfig.SerialConsistency)
}

func TestRoundRobinHostPolicy(t *testing.T) {
	config := Config{}
	err := yaml.Unmarshal([]byte("hostSelectionPolicy: RoundRobinHostPolicy"), &config)
	assert.NoError(t, err)
}

func TestConfigInvalid(t *testing.T) {
	cases := []struct {
		conf, expected string
	}{
		{"hostSelectionPolicy: InvalidHostSelectionPolicy", "InvalidHostSelectionPolicy"},
		{"serialConsistency: InvalidSerialConsistency", "InvalidSerialConsistency"},
	}

	for _, tc := range cases {
		config := Config{}
		err := yaml.Unmarshal([]byte(tc.conf), &config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), tc.expected)
	}
}
