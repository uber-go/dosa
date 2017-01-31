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

	"github.com/stretchr/testify/assert"
)

func TestExpandDirectories(t *testing.T) {
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

	testCases := []struct {
		Argument []string
		Error    error
		Expected []string
	}{
		{
			Argument: []string{},
			Expected: []string{"."},
		},
		{
			Argument: []string{"."},
			Expected: []string{"."},
		},
		{
			Argument: []string{"./..."},
			Expected: append([]string{"."}, dirs...),
		},
		{
			Argument: []string{"bogus"},
			Error:    errors.New("no such file or directory"),
		},
		{
			Argument: []string{"a/b/file"},
			Error:    errors.New("not a directory"),
		},
	}

	for _, testCase := range testCases {
		actual, err := expandDirectories(testCase.Argument)
		if testCase.Error != nil {
			assert.Contains(err.Error(), testCase.Error.Error())
		} else {
			assert.Nil(err)
			assert.Equal(testCase.Expected, actual)
		}
	}
	os.Chdir("..")
}
