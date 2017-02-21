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

// ScanOp represents the scan query
type ScanOp struct{}

// NewScanOp returns a new ScanOp instance
func NewScanOp(DomainObject) *ScanOp {
	return &ScanOp{}
}

// String satisfies the Stringer interface
func (s *ScanOp) String() string {
	/* TODO */
	return ""
}

// Limit sets the number of rows returned per call. Default is 128.
func (s *ScanOp) Limit(n int) *ScanOp {
	/* TODO */
	return s
}

// Offset sets the pagination token. If not set, an empty token would be used.
func (s *ScanOp) Offset(token string) *ScanOp {
	/* TODO */
	return s
}

// Fields list the non-key fields users want to fetch.
// PrimaryKey fields are always fetched.
func (s *ScanOp) Fields([]string) *ScanOp {
	/* TODO */
	return s
}
