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
	"bytes"
	"fmt"
	"io"

	"reflect"

	"github.com/golang/mock/gomock"
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

type scanOpMatcher struct {
	limit  int
	token  string
	fields map[string]bool
	typ    reflect.Type
}

// EqScanOp provides a gomock Matcher that matches any ScanOp with a limit,
// token, and fields to read that are the same as those specificed by the op argument.
func EqScanOp(op *ScanOp) gomock.Matcher {
	fields := make(map[string]bool, len(op.fieldsToRead))
	for _, field := range op.fieldsToRead {
		fields[field] = true
	}

	return scanOpMatcher{
		limit:  op.limit,
		token:  op.token,
		fields: fields,
		typ:    reflect.TypeOf(op.object).Elem(),
	}
}

// Matches satisfies the gomock.Matcher interface
func (m scanOpMatcher) Matches(x interface{}) bool {
	op, ok := x.(*ScanOp)
	if !ok {
		return false
	}

	for _, field := range op.fieldsToRead {
		if !m.fields[field] {
			return false
		}
	}

	return op.limit == m.limit && op.token == m.token && reflect.TypeOf(op.object).Elem() == m.typ
}

// String satisfies the gomock.Matcher and Stringer interface
func (m scanOpMatcher) String() string {
	fieldList := make([]string, 0, len(m.fields))
	for field := range m.fields {
		fieldList = append(fieldList, field)
	}
	return fmt.Sprintf(
		" is equal to ScanOp with limit %d, token %q, dosa entity type %v, and fields %v",
		m.limit,
		m.token,
		m.typ,
		fieldList,
	)
}
