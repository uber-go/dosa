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
	"errors"
	"os"
	"testing"

	"context"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/mocks"
)

func TestSchema_ExpandDirectories(t *testing.T) {
	assert := assert.New(t)
	const tmpdir = ".testexpanddirectories"
	os.RemoveAll(tmpdir)
	defer os.RemoveAll(tmpdir)

	if err := os.Mkdir(tmpdir, 0770); err != nil {
		t.Fatalf("can't create %s: %s", tmpdir, err)
	}
	// note: these must be in lexical order :(
	dirs := []string{"a", "a/b", "c", "c/d", "c/e"}

	os.Chdir(tmpdir)
	for _, dirToCreate := range dirs {
		os.Mkdir(dirToCreate, 0770)
	}
	os.Create("a/b/file")

	cases := []struct {
		args []string
		dirs []string
		err  error
	}{
		{
			args: []string{},
			dirs: []string{"."},
		},
		{
			args: []string{"."},
			dirs: []string{"."},
		},
		{
			args: []string{"./..."},
			dirs: append([]string{"."}, dirs...),
		},
		{
			args: []string{"bogus"},
			err:  errors.New("no such file or directory"),
		},
		{
			args: []string{"a/b/file"},
			err:  errors.New("not a directory"),
		},
	}

	for _, c := range cases {
		dirs, err := expandDirectories(c.args)
		if c.err != nil {
			assert.Contains(err.Error(), c.err.Error())
		} else {
			assert.Nil(err)
			assert.Equal(c.dirs, dirs)
		}
	}
	os.Chdir("..")
}

func TestSchema_Check_PrefixRequired(t *testing.T) {
	c := StartCapture()
	exit = func(r int) {}
	os.Args = []string{"dosa", "schema", "check", "../testentity"}
	main()
	assert.Contains(t, c.stop(true), "--prefix' was not specified")
}

func TestSchema_Upsert_PrefixRequired(t *testing.T) {
	c := StartCapture()
	exit = func(r int) {}
	os.Args = []string{"dosa", "schema", "upsert", "../testentity"}
	main()
	assert.Contains(t, c.stop(true), "--prefix' was not specified")
}

func TestSchema_Check_InvalidDirectory(t *testing.T) {
	c := StartCapture()
	exit = func(r int) {}
	os.Args = []string{"dosa", "schema", "check", "--prefix", "foo", "../testentity", "/dev/null"}
	main()
	assert.Contains(t, c.stop(true), "is not a directory")
}

func TestSchema_Upsert_InvalidDirectory(t *testing.T) {
	c := StartCapture()
	exit = func(r int) {}
	os.Args = []string{"dosa", "schema", "upsert", "--prefix", "foo", "../testentity", "/dev/null"}
	main()
	assert.Contains(t, c.stop(true), "is not a directory")
}

func TestSchema_Check_NoEntitiesFound(t *testing.T) {
	c := StartCapture()
	exit = func(r int) {}
	os.Args = []string{"dosa", "schema", "check", "--prefix", "foo", "-e", "testentity.go", "../testentity"}
	main()
	assert.Contains(t, c.stop(true), "no entities found")
}

func TestSchema_Upsert_NoEntitiesFound(t *testing.T) {
	c := StartCapture()
	exit = func(r int) {}
	os.Args = []string{"dosa", "schema", "upsert", "--prefix", "foo", "-e", "testentity.go", "../testentity"}
	main()
	assert.Contains(t, c.stop(true), "no entities found")
}

func TestSchema_Check_Happy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	exit = func(r int) {
		assert.Equal(t, 0, r)
	}
	dosa.RegisterConnector("mock", func(map[string]interface{}) (dosa.Connector, error) {
		mc := mocks.NewMockConnector(ctrl)
		mc.EXPECT().CheckSchema(gomock.Any(), "scope", "foo", gomock.Any()).
			Do(func(ctx context.Context, scope string, namePrefix string, ed []*dosa.EntityDefinition) {
				dl, ok := ctx.Deadline()
				assert.True(t, ok)
				assert.True(t, dl.After(time.Now()))
				assert.Equal(t, 1, len(ed))
				assert.Equal(t, "awesome_test_entity", ed[0].Name)
			}).Return([]int32{1}, nil)
		return mc, nil
	})
	os.Args = []string{"dosa", "--connector", "mock", "schema", "check", "--prefix", "foo", "-e", "_test.go", "-e", "excludeme.go", "-s", "scope", "-v", "../testentity"}
	main()
}

func TestSchema_Upsert_Happy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	exit = func(r int) {
		assert.Equal(t, 0, r)
	}
	dosa.RegisterConnector("mock", func(map[string]interface{}) (dosa.Connector, error) {
		mc := mocks.NewMockConnector(ctrl)
		mc.EXPECT().UpsertSchema(gomock.Any(), "scope", "foo", gomock.Any()).
			Do(func(ctx context.Context, scope string, namePrefix string, ed []*dosa.EntityDefinition) {
				dl, ok := ctx.Deadline()
				assert.True(t, ok)
				assert.True(t, dl.After(time.Now()))
				assert.Equal(t, 1, len(ed))
				assert.Equal(t, "awesome_test_entity", ed[0].Name)
			}).Return([]int32{1}, nil)
		return mc, nil
	})
	os.Args = []string{"dosa", "--connector", "mock", "schema", "upsert", "--prefix", "foo", "-e", "_test.go", "-e", "excludeme.go", "-s", "scope", "-v", "../testentity"}
	main()
}

func TestSchema_Dump_NotImplemented(t *testing.T) {
	c := StartCapture()
	exit = func(r int) {}
	os.Args = []string{"dosa", "schema", "dump", "--prefix", "foo", "../testentity"}
	main()
	output := c.stop(false)
	t.Skip("TODO This functionality is not implemented yet")
	assert.Contains(t, output, "create table awesome_test_entity (")
}
