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

package dosa

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrimaryKey(t *testing.T) {
	k, err := parsePrimaryKey("t", "partkey")
	assert.Equal(t, k.PartitionKeys, []string{"partkey"})
	assert.Nil(t, err)
}

func TestMissingComma(t *testing.T) {
	k, err := parsePrimaryKey("t", "(partkey)primkey")
	assert.Nil(t, k)
	assert.Contains(t, err.Error(), "comma")
}

func TestDuplicateField(t *testing.T) {
	k, err := parsePrimaryKey("t", "(partkey),partkey")
	assert.Nil(t, k)
	assert.Contains(t, err.Error(), "\"partkey\"")
	assert.Contains(t, err.Error(), "duplicate")
}

func TestAscendingDefault(t *testing.T) {
	k1, err1 := parsePrimaryKey("t", "(partkey),primkey")
	k2, err2 := parsePrimaryKey("t", "( partkey ), primkey asc")
	assert.Nil(t, err1)
	assert.Nil(t, err2)
	assert.Equal(t, k1, k2)
}

func TestBogusAscendingDescending(t *testing.T) {
	k, err := parsePrimaryKey("t", "(partkey),primkey bogus")
	assert.Nil(t, k)
	assert.Contains(t, err.Error(), "primkey")
	assert.Contains(t, err.Error(), "bogus")
	assert.Contains(t, err.Error(), "clustering")
}

func TestExtraAscendingDescending(t *testing.T) {
	k, err := parsePrimaryKey("t", "(partkey),primkey asc desc")
	assert.Nil(t, k)
	assert.Contains(t, err.Error(), "primkey")
	assert.Contains(t, err.Error(), "lustering")
}

func TestDescendingInSimpleModeTypo(t *testing.T) {
	k, err := parsePrimaryKey("t", "partkey,primkey bogus")
	assert.Nil(t, k)
	assert.Contains(t, err.Error(), "primkey")
	assert.Contains(t, err.Error(), "bogus")
	assert.Contains(t, err.Error(), "clustering")
}
