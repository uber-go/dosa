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

// RangeOp is used to specify constraints to Range calls
type RangeOp struct{}

// NewRangeOp returns a new RangeOp instance
func NewRangeOp(DomainObject) *RangeOp {
	return &RangeOp{}
}

// String satisfies the Stringer interface
func (r *RangeOp) String() string {
	/* TODO */
	return ""
}

// Eq is used to express an equality constraint for a range query
func (r *RangeOp) Eq(string, interface{}) *RangeOp {
	/* TODO */
	return r
}

// Gt is used to express an "greater than" constraint for a range query
func (r *RangeOp) Gt(key string, value interface{}) *RangeOp {
	/* TODO */
	return r
}

// GtOrEq is used to express an "greater than or equal" constraint for a
// range query
func (r *RangeOp) GtOrEq(key string, value interface{}) *RangeOp {
	/* TODO */
	return r
}

// Lt is used to express a "less than" constraint for a range query
func (r *RangeOp) Lt(key string, value interface{}) *RangeOp {
	/* TODO */
	return r
}

// LtOrEq is used to express a "less than or equal" constraint for a
// range query
func (r *RangeOp) LtOrEq(key string, value interface{}) *RangeOp {
	/* TODO */
	return r
}

// Fields list the non-key fields users want to fetch. If not set, all fields would be fetched.
// PrimaryKey fields are always fetched.
func (r *RangeOp) Fields([]string) *RangeOp {
	/* TODO */
	return r
}

// Limit sets the number of rows returned per call. If not set, a default
// value would be applied
func (r *RangeOp) Limit(n int) *RangeOp {
	/* TODO */
	return r
}

// Offset sets the pagination token. If not set, an empty token would be used.
func (r *RangeOp) Offset(token string) *RangeOp {
	/* TODO */
	return r
}
