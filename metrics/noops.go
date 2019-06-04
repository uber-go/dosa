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

package metrics

import (
	"time"
)

// CheckIfNilStats checks whether stats is nil. If it is nil then return default scope
func CheckIfNilStats(stats Scope) Scope {
	if stats == nil {
		return &NoopScope{}
	}
	return stats
}

// NoopScope scopes nothing
type NoopScope struct{}

// NoopCounter Counts nothing
type NoopCounter struct{}

// NoopTimer times nothing
type NoopTimer struct{}

// Counter is a noop
func (s *NoopScope) Counter(name string) Counter {
	return &NoopCounter{}
}

// Tagged is a noop
func (s *NoopScope) Tagged(tags map[string]string) Scope {
	return &NoopScope{}
}

// SubScope is a noop
func (s *NoopScope) SubScope(name string) Scope {
	return &NoopScope{}
}

// Timer is a noop
func (s *NoopScope) Timer(name string) Timer {
	return &NoopTimer{}
}

// Inc is a noop
func (c *NoopCounter) Inc(delta int64) {
	return
}

// Start is a noop
func (t *NoopTimer) Start() time.Time {
	return time.Time{}
}

// Stop is a noop
func (t *NoopTimer) Stop() {
	return
}
