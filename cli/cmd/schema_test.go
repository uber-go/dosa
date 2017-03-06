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

package cmd

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
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
