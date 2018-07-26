// Copyright (c) 2018 Uber Technologies, Inc.
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

/*
Package main is the entrypoint for the dosa CLI. Some examples below:


Options for all commands:

Override default host:port for gateway:

	$ dosa -h 192.168.0.2 -p 8080 <cmd>

Provide a custom service name, "foo" for gateway (when using TChannel):

	$ dosa --service dosa-foo <cmd> ...

Use "dosacli-infra" as caller name for RPCs to gateway:

	$ dosa --caller dosacli-infra <cmd>

Provide a custom timeout of 20 seconds for RPCs to gateway:

	$ dosa --timeout 20s <cmd>


Managing Scopes:

Create a new scope called "infra_dev":

	$ dosa scope create infra_dev

List all scopes created by $USER (you):

	$ dosa scope list

Scope subcommand usage:

	$ dosa scope help


Managing Schema:

Dump schema to stdout as CQL (default is Avro):

	$ dosa schema dump -as cql

Check the compatibility status of all schema in the "infra_dev" scope:

	$ dosa schema status -s infra_dev

Check the compatibility status of the schema with prefix "oss.user" in the "infra_dev" scope:

	$ dosa schema status -s infra_dev -np oss.user

Upsert schema with prefix "oss.user" to the "infra_dev" scope:

	$ dosa schema upsert -s infra_dev -np oss.user


Code Generation:

TODO

	$ dosa gen ...


Defining Custom Commands:

TODO

	$ dosa mycmd ...

*/
package main
