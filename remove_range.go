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

// RemoveRangeOp is used to specify contraints on RemoveRange calls
type RemoveRangeOp struct {
	conditioner
}

// NewRemoveRangeOp returns a new RangeOp instance
func NewRemoveRangeOp(object DomainObject) *RemoveRangeOp {
	rop := &RemoveRangeOp{
		conditioner: conditioner{
			object:     object,
			conditions: map[string][]*Condition{},
		},
	}
	return rop
}

// Eq is used to express an equality constraint for a remove range operation
func (r *RemoveRangeOp) Eq(fieldName string, value interface{}) *RemoveRangeOp {
	r.appendOp(Eq, fieldName, value)
	return r
}

// Gt is used to express an "greater than" constraint for a remove range operation
func (r *RemoveRangeOp) Gt(fieldName string, value interface{}) *RemoveRangeOp {
	r.appendOp(Gt, fieldName, value)
	return r
}

// GtOrEq is used to express an "greater than or equal" constraint for a
// remove range operation
func (r *RemoveRangeOp) GtOrEq(fieldName string, value interface{}) *RemoveRangeOp {
	r.appendOp(GtOrEq, fieldName, value)
	return r
}

// Lt is used to express a "less than" constraint for a remove range operation
func (r *RemoveRangeOp) Lt(fieldName string, value interface{}) *RemoveRangeOp {
	r.appendOp(Lt, fieldName, value)
	return r
}

// LtOrEq is used to express a "less than or equal" constraint for a
// remove range operation
func (r *RemoveRangeOp) LtOrEq(fieldName string, value interface{}) *RemoveRangeOp {
	r.appendOp(LtOrEq, fieldName, value)
	return r
}
