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
	"io"
	"bytes"
)

// ScanOp represents the scan query
type ScanOp struct {
	object       DomainObject
	limit        int
	token        string
	fieldsToRead []string
}

// NewScanOp returns a new ScanOp instance
func NewScanOp(obj DomainObject) *ScanOp {
	return &ScanOp{object: obj}
}

// String satisfies the Stringer interface
func (s *ScanOp) String() string {
	result := &bytes.Buffer{}
	result.WriteString("ScanOp")
	addLimitTokenString(result, s.limit, s.token)
	return result.String()
}

func addLimitTokenString(w io.Writer, limit int, token string) {
	if limit > 0 {
		fmt.Fprintf(w, " limit %d", limit)
	}
	if token != "" {
		fmt.Fprintf(w, " token %q", token)
	}
}

// Limit sets the number of rows returned per call. Default is 128.
func (s *ScanOp) Limit(n int) *ScanOp {
	s.limit = n
	return s
}

// Offset sets the pagination token. If not set, an empty token would be used.
func (s *ScanOp) Offset(token string) *ScanOp {
	s.token = token
	return s
}

// Fields list the non-key fields users want to fetch.
// PrimaryKey fields are always fetched.
func (s *ScanOp) Fields(fields []string) *ScanOp {
	s.fieldsToRead = fields
	return s
}
