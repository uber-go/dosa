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
	assert.Equal(t, config.ClusterConfig.PoolConfig, expected.PoolConfig)
	assert.Equal(t, config.ClusterConfig.RetryPolicy, expected.RetryPolicy)
	assert.Equal(t, config.ClusterConfig.CQLVersion, expected.CQLVersion)
	assert.Equal(t, config.ClusterConfig.ProtoVersion, expected.ProtoVersion)
	assert.Equal(t, config.ClusterConfig.Timeout, expected.Timeout)
	assert.Equal(t, config.ClusterConfig.MaxPreparedStmts, expected.MaxPreparedStmts)
	assert.Equal(t, config.ClusterConfig.MaxRoutingKeyInfo, expected.MaxRoutingKeyInfo)
	assert.Equal(t, config.ClusterConfig.NumConns, expected.NumConns)
	assert.Equal(t, config.ClusterConfig.SerialConsistency, expected.SerialConsistency)
	assert.Equal(t, config.ClusterConfig.SocketKeepalive, expected.SocketKeepalive)
	assert.Equal(t, config.ClusterConfig.PageSize, expected.PageSize)
	// no way I can test this. The hostfilter is an anonymous func
	//assert.Equal(t, config.ClusterConfig.HostFilter, expected.HostFilter)
	assert.Equal(t, config.ClusterConfig.Port, expected.Port)
	assert.Equal(t, config.ClusterConfig.ConnectTimeout, expected.ConnectTimeout)
	assert.Equal(t, config.ClusterConfig.Hosts, expected.Hosts)
	assert.Equal(t, config.ClusterConfig.Consistency, expected.Consistency)

}
