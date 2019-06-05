// Copyright (c) 2019 Uber Technologies, Inc.
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
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCallerFlag_String(t *testing.T) {
	f := callerFlag("")
	f.setString("foo.bar.baz")
	assert.Equal(t, "foo-bar-baz", f.String())

	err := f.UnmarshalFlag("qux.quux.corge")
	assert.NoError(t, err)
	assert.Equal(t, "qux-quux-corge", f.String())
}

func TestCallerFlag_Default(t *testing.T) {
	oldEnv := os.Getenv("USER")
	expected := "dosacli-firstname"
	f := callerFlag("")

	os.Setenv("USER", "firstname")
	err := f.UnmarshalFlag("")
	assert.NoError(t, err)
	assert.Equal(t, expected, f.String(), "Uses $USER environment variable for default caller")

	os.Setenv("USER", "Firstname")
	err = f.UnmarshalFlag("")
	assert.NoError(t, err)
	assert.Equal(t, expected, f.String(), "Converts uppercase to lowercase")

	os.Setenv("USER", "Fùrstname")
	err = f.UnmarshalFlag("")
	assert.NoError(t, err)
	assert.Equal(t, "dosacli-fùrstname", f.String(), "Converts as expected when non-ascii characters are present")

	os.Setenv("USER", oldEnv)
}

func TestTimeFlag_Duration(t *testing.T) {
	sut := timeFlag(0)
	sut.setDuration(time.Minute)
	assert.Equal(t, time.Minute, sut.Duration())

	err := sut.UnmarshalFlag("100")
	assert.NoError(t, err)
	assert.Equal(t, 100*time.Millisecond, sut.Duration())

	err = sut.UnmarshalFlag("do_this_dont_do_that_cant_you_read_the_time")
	assert.Error(t, err)
}
