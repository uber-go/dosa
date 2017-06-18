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
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/cassandra"
	. "github.com/uber-go/dosa/connectors/cassandra"
)

var testConnector dosa.Connector

func VerifyOrStartCassandra() error {
	if IsRunning() {
		return nil
	}
	return startViaCcm()
}

func GetTestConnector(t *testing.T) dosa.Connector {
	if testConnector == nil {
		err := VerifyOrStartCassandra()
		if err != nil {
			t.Fatal(err)
			return nil
		}
		testConnector, err = NewConnector(
			gocql.NewCluster("127.0.0.1:"+strconv.Itoa(CassandraPort)),
			&cassandra.UseNamePrefix{},
			nil,
		)
		if err != nil {
			t.Fatal(err)
			return nil
		}
		testStore = testConnector.(*Connector)
		err = initTestSchema("test.datastore", testEntityInfo)
		if err != nil {
			t.Fatal(err)
			return nil
		}
	}
	return testConnector
}

func ShutdownTestConnector() {
	testConnector.Shutdown()
	testConnector = nil
}

func ShouldSkip() bool {
	if IsRunning() {
		return false
	}
	if err := startViaCcm(); err != nil {
		return true
	}
	return false
}

const ccm = "ccm"
const cassVersion = "3.0.11"

// see if we can start up a cassandra instance using ccm
func startViaCcm() error {
	path, err := exec.LookPath(ccm)
	if err != nil {
		return errors.Wrap(err, "ccm executable not found")

	}
	// call remove, but don't care if it fails
	_ = exec.Command(path, "remove", "test").Run()
	cmd := exec.Command(path, "create", "test", "-v", cassVersion, "-n", "1", "-d", "--vnodes", "--jvm_arg=-Xmx256 -XX:NewSize=100m")
	if output, err := cmd.CombinedOutput(); err != nil {
		return errors.Wrapf(err, "failed to create test cluster: %s", string(output))
	}
	// set up some configuration to make this cluster take less memory
	cmd = exec.Command(path, "updateconf",
		"concurrent_reads: 2",
		"concurrent_writes: 2",
		"rpc_server_type: sync",
		"rpc_min_threads: 2",
		"rpc_max_threads: 2",
		"write_request_timeout_in_ms: 5000",
		"read_request_timeout_in_ms: 5000",
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		return errors.Wrapf(err, "failed to configure test cluster: %s", string(output))
	}
	// start the service
	if output, err := exec.Command(path, "start", "-v", "--wait-for-binary-proto").CombinedOutput(); err != nil {
		return errors.Wrapf(err, "failed to start test cluster: %s", string(output))
	}
	time.Sleep(1 * time.Second)
	return nil
}
