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

package dosa

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsValidName(t *testing.T) {
	dataProvider := []struct {
		arg string
		err string
	}{
		{
			arg: "has_underscore",
		},
		{
			arg: "mixeDCase",
			err: "invalid name",
		},
		{
			arg: "md5",
		},
		{
			arg: "_name",
		},
		{
			arg: "_alreadynormalized9",
		},
		{
			arg: "123numberprefix",
			err: "invalid name",
		},
		{
			arg: "",
			err: "invalid name",
		},
		{
			arg: "longname012345678901234567890123456789",
			err: "invalid name",
		},
		{
			arg: "世界",
			err: "invalid name",
		},
		{
			arg: "an apple",
			err: "invalid name",
		},
		{
			arg: "token",
			err: "reserved word",
		},
		{
			arg: "keyspaces",
			err: "reserved word",
		},
		{
			arg: "schema",
			err: "reserved word",
		},
	}

	for _, testData := range dataProvider {
		err := IsValidName(testData.arg)
		if testData.err == "" {
			assert.NoError(t, err, testData.arg)
		} else {
			assert.Error(t, err, testData.arg)
			assert.Contains(t, err.Error(), testData.err)
		}
	}
}

func TestNormalizeName(t *testing.T) {
	dataProvider := []struct {
		arg      string
		allowed  bool
		expected string
	}{
		{
			arg:      "lOwerEVeryTHiNG",
			allowed:  true,
			expected: "lowereverything",
		},
		{
			arg:      "MD5",
			allowed:  true,
			expected: "md5",
		},
		{
			arg:      "_MyName",
			allowed:  true,
			expected: "_myname",
		},
		{
			arg:      "_alreadynormalized9",
			allowed:  true,
			expected: "_alreadynormalized9",
		},
		// Invalid
		{
			arg: "an apple",
		},
		{
			arg: "9Monkeys",
		},
	}

	for _, testData := range dataProvider {
		name, err := NormalizeName(testData.arg)
		if testData.allowed {
			assert.NoError(t, err, testData.arg)
			assert.Equal(t, testData.expected, name, testData.arg)
		} else {
			assert.Error(t, err, testData.arg)
		}
	}
}

func TestIsValidNamePrefix(t *testing.T) {
	err := IsValidNamePrefix("service.foo")
	assert.NoError(t, err)

	err = IsValidNamePrefix("MyService.Foo.V2")
	assert.NoError(t, err)

	err = IsValidNamePrefix("")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid name")

	err = IsValidNamePrefix("service.an entity")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid name")

	err = IsValidNamePrefix("germanRush.über")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid name")

	err = IsValidNamePrefix("this.prefix.has.more.than.thrity.two.characters.in.it")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid name")
}

func TestNormalizeNamePrefix(t *testing.T) {
	cases := []struct {
		arg      string
		bogus    bool
		expected string
	}{
		{
			arg:      "lOwerEVeryTHiNG",
			expected: "lowereverything",
		},
		{
			arg:      "_MyName",
			expected: "_myname",
		},
		{
			arg:      "_alreadynormalized9",
			expected: "_alreadynormalized9",
		},
		{
			arg:      "_My.Name",
			expected: "_my.name",
		},
		{
			arg:      "_already.normalized.9",
			expected: "_already.normalized.9",
		},
		{
			arg:   "an apple",
			bogus: true,
		},
		{
			arg:   "apple!",
			bogus: true,
		},
		{
			arg:   "a.b.c.d!",
			bogus: true,
		},
		{
			arg:   "9Monkeys",
			bogus: true,
		},
	}

	for _, tc := range cases {
		name, err := NormalizeNamePrefix(tc.arg)
		if tc.bogus {
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "invalid name")
		} else {
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, name)
		}
	}
}
