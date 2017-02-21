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

// SearchOp represents the search query using a "searchable" field.
type SearchOp struct{}

// NewSearchOp returns a new SearchOp instance
func NewSearchOp(DomainObject) *SearchOp {
	return &SearchOp{}
}

// String satisfies the stringer interface
func (s *SearchOp) String() string {
	/* TODO */
	return ""
}

// By indicates the "searchable" field name and its value.
func (s *SearchOp) By(fieldName string, fieldValue interface{}) *SearchOp {
	/* TODO */
	return s
}

// Limit sets the number of rows returned per call. Default is 128.
func (s *SearchOp) Limit(n int) *SearchOp {
	/* TODO */
	return s
}

// Offset sets the pagination token. If not set, an empty token would be used.
func (s *SearchOp) Offset(token string) *SearchOp {
	/* TODO */
	return s
}

// Fields list the non-key fields users want to fetch. If not set, all normalized fields
// (supplied with “storing” annotation) would be fetched.
// PrimaryKey fields are always fetched.
func (s *SearchOp) Fields([]string) *SearchOp {
	/* TODO */
	return s
}
