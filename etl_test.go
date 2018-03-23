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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestETLState(t *testing.T) {

	testCases := []struct {
		name     string
		etl      string
		expected ETLState
		err      error
	}{
		{
			name:     "ETL On",
			etl:      "on",
			expected: EtlOn,
			err:      nil,
		},
		{
			name:     "ETL Off",
			etl:      "off",
			expected: EtlOff,
			err:      nil,
		},
		{
			name:     "ETL invalid",
			etl:      "boom",
			expected: EtlOff,
			err:      fmt.Errorf("%s: boom", errUnrecognizedETLState),
		},
	}

	for _, tc := range testCases {
		t.Log(tc.name)
		s, err := ToETLState(tc.etl)
		assert.Equal(t, tc.err, err)
		assert.Equal(t, tc.expected, s)
	}
}
