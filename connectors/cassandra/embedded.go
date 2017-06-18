// Package cassandra includes convenient functions to start the embedded server
package cassandra

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"time"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/uql-schema-client.git/mapi"
	"github.com/gocql/gocql"
)

const (
	// EmbeddedAppID is the app id used for test against stapi-all server
	EmbeddedAppID = "embedded-test-app-id"
)

var (
	// StapiPort is the port number for the stapi management server, when this
	// cannot be parsed out of the start.sh script, it will default to
	// defStapiPort
	StapiPort = 9090

	// CassandraPort is the port number for the cassandra server, when this
	// cannnot be parsed out of the start.sh script, it will default to
	// defCassandraPort
	CassandraPort = 9091

	mux sync.Mutex
)

// EnsureServerUp starts the embedded server if server is not running.
// It also creates keyspaces and applies schemas based on default path of uql scripts
func EnsureServerUp(startScriptRelativePath string) error {
	mux.Lock()
	defer mux.Unlock()

	// ensure Cassandra server is up
	session, err := newSession()
	if err != nil {
		errC := startEmbeddedCassandra(startScriptRelativePath)
		if errC != nil {
			log.Error("Unable to start Cassandra cluster: ", errC.Error())
			return errC
		}
		for retries := 0; err != nil; retries++ {
			if retries > 60 {
				log.Error("Too many retries to connect to the cassandra server.")
				return err
			}
			time.Sleep(1 * time.Second)
			session, err = newSession()
		}
	}

	session.Close()
	log.Info("Cassandra server is up.")

	// ensure stapi server is up
	managementBasePath := fmt.Sprintf("http://127.0.0.1:%d", StapiPort)
	management := mapi.NewDatastoresAPI(managementBasePath, EmbeddedAppID)
	ctx := context.Background()
	if _, err = management.GetAllConfigs(ctx); err != nil {
		log.Error("stapi server is not up yet ...")
		for retries := 0; err != nil; retries++ {
			if retries > 20 {
				log.Error("Too many retries to conenct to the stapi server.")
				return err
			}
			time.Sleep(1 * time.Second)
			_, err = management.GetAllConfigs(ctx)

		}

	}
	log.Info("stapi schema management server is up")

	return nil
}

// startEmbeddedCassandra invokes a script which starts the java process
// the start up usually takes about 4 seconds.
func startEmbeddedCassandra(relativePath string) error {
	cmd := exec.Cmd{
		Path: "/bin/sh",
		Args: []string{"/bin/sh", "start.sh"},
		Dir:  relativePath,
	}

	// pipe java process console output to log
	cmdReader, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error creating StdoutPipe for Cmd", err)
		os.Exit(1)
	}

	err = cmd.Start()
	if err != nil {
		return err
	}

	// drain the console
	in := bufio.NewScanner(cmdReader)
	go func() {
		for in.Scan() {
			log.Info(in.Text())
		}
	}()

	return nil
}

// newSession makes a connection to the server
func newSession() (*gocql.Session, error) {
	ContactPoints := []string{"127.0.0.1"}
	cluster := gocql.NewCluster(ContactPoints...)
	cluster.Port = CassandraPort
	cluster.Consistency = gocql.One
	cluster.PoolConfig.HostSelectionPolicy = gocql.RoundRobinHostPolicy()
	cluster.ProtoVersion = 4
	return cluster.CreateSession()
}
