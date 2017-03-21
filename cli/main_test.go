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

package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNoSubcommand(t *testing.T) {
	c := StartCapture()
	exit = func(r int) {
		assert.Equal(t, 1, r)
	}
	os.Args = []string{"dosa"}
	main()
	assert.Contains(t, c.stop(true), "schema or scope")
}

func TestMissingSubcommands(t *testing.T) {
	c := StartCapture()
	exit = func(r int) {}
	os.Args = []string{"dosa", "schema"}
	main()
	assert.Contains(t, c.stop(true), "check, dump or upsert")
}

func TestHostOptionButNothingElse(t *testing.T) {
	c := StartCapture()
	exit = func(r int) {}
	os.Args = []string{"dosa", "--host", "10.10.10.10"}
	main()
	assert.Contains(t, c.stop(true), "schema or scope")
}

// this test uses a trailing dot in the hostname to avoid multiple DNS lookups
func TestInvalidHost(t *testing.T) {
	c := StartCapture()
	exit = func(r int) {}
	os.Args = []string{"dosa", "--host", "invalid-hostname.", "schema", "check", "--prefix", "foo"}
	main()
	output := c.stop(true)
	assert.Contains(t, output, "invalid-hostname")
	assert.Contains(t, output, "no such host")
}

// this error message is a bit strange, but is caused because the port could be a
// "port name" in /etc/services
func TestInvalidPort(t *testing.T) {
	c := StartCapture()
	exit = func(r int) {}
	os.Args = []string{"dosa", "-p", "invalid-port", "schema", "check", "--prefix", "foo"}
	main()
	output := c.stop(true)
	assert.Contains(t, output, "invalid-port")
	// can't check the actual error since it differs per platform; travis reports "unknown port"
	// versus MacOS's "not known"
}

func TestInvalidTransport(t *testing.T) {
	c := StartCapture()
	exit = func(r int) {}
	os.Args = []string{"dosa", "--transport", "invalid-transport", "schema", "check", "--prefix", "foo"}
	main()
	output := c.stop(true)
	assert.Contains(t, output, "invalid transport")
}

/* TODO: implement these integration test cases

// expect: top-level usage, transport=tchannel
dosa --transport tchannel

// expect: invalid service name
dosa --service ...

// expect: top-level usage, service=foo
dosa --service foo

// expect: invalid caller name
dosa --caller ...

// expect: top-level usage, caller=foo
dosa --caller foo

// expect: invalid timeout
dosa --timeout ...

// expect: top-level usage, timeout=3s
dosa --timeout 3s


// Schema

// expect: schema usage
dosa schema


// Schema check

// expect: schema check usage
dosa schema check

// expect: schema check, prefix=foo, default client config
dosa schema check --prefix foo

// expect: schema check, prefix=foo,
dosa schema check --prefix foo -e test.go

// expect: schema check, prefix=foo excludes=[test.go, test2.go]
dosa schema check --prefix foo -e test.go -e test2.go

// expect: schema check, prefix=foo, scope=bar
dosa schema check --prefix foo -s bar

// expect: schema check, prefix=foo, dirs=[a b c]
dosa schema check --prefix foo a b c


// Schema upsert

// expect: schema upsert usage
dosa schema upsert

// expect: schema upsert, prefix=foo, default client config
dosa schema upsert --prefix foo

// expect: schema upsert, prefix=foo,
dosa schema upsert --prefix foo -e test.go

// expect: schema upsert, prefix=foo excludes=[test.go, test2.go]
dosa schema upsert --prefix foo -e test.go -e test2.go

// expect: schema upsert, prefix=foo, scope=bar
dosa schema upsert --prefix foo -s bar

// expect: schema upsert, prefix=foo, dirs=[a b c]
dosa schema upsert --prefix foo a b c


// Scope

// expect: scope usage
dosa scope


// Scope create

// expect: scope create usage
dosa scope create

// expect: invalid scope
dosa scope create ...

// expect: scope create, scopes=[a]
 dosa scope create a

// expect: scope create, scopes=[a b c]
dosa scope create b


// Scope drop

// expect: scope drop usage
dosa scope drop

// expect: invalid scope
dosa scope drop ...

// expect: scope drop, scopes=[a]
 dosa scope drop a

// expect: scope drop, scopes=[a b c]
dosa scope drop b


// Scope truncate

// expect: scope truncate usage
dosa scope truncate

// expect: invalid scope
dosa scope truncate ...

// expect: scope truncate, scopes=[a]
 dosa scope truncate a

// expect: scope truncate, scopes=[a b c]
dosa scope truncate b

*/
