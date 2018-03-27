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
	"time"

	"github.com/pkg/errors"
)

// NoTTL returns predefined identifier for not setting TTL
func NoTTL() time.Duration {
	return time.Duration(-1)
}

// ValidateTTL returns whether the TTL is validated or not
// Any TTL with value of TTL less than 1 second is not allowed except value 0.
// The TTL (0) means to clear previous TTL associated with the data record and make the record stay permanently.
func ValidateTTL(ttl time.Duration) error {
	if ttl < 1*time.Second && ttl != 0 {
		return errors.New("TTL is not allowed to set less than 1 second")
	}

	return nil
}
