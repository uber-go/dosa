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

	"fmt"
	"time"

	"github.com/stretchr/testify/assert"
)

// White box tests for unexported, stateless helper methods

func TestEnsureTypeMatch(t *testing.T) {
	type testCase struct {
		tp        Type
		v         interface{}
		expectErr bool
	}

	cases := []testCase{
		{TUUID, UUID("267275CD-D312-4EFB-A304-020A43971D68"), false},
		{TUUID, "267275CD-D312-4EFB-A304-020A43971D68", true},
		{Int64, int64(0), false},
		{Int64, "0", true},
		{Int32, int32(0), false},
		{Int32, 1.2, true},
		{String, "abc", false},
		{String, []byte{1, 2, 3}, true},
		{Blob, []byte{1, 2, 3}, false},
		{Blob, "1,2,3", true},
		{Bool, true, false},
		{Bool, "false", true},
		{Double, float64(5.5), false},
		{Double, 1, true},
		{Timestamp, time.Now(), false},
		{Timestamp, "Fri Feb 24 15:43:46 PST 2017", true},
	}

	for _, c := range cases {
		if c.expectErr {
			assert.Error(t, ensureTypeMatch(c.tp, c.v))
		} else {
			assert.NoError(t, ensureTypeMatch(c.tp, c.v))
		}
	}
	assert.Panics(t, func() {
		ensureTypeMatch(Invalid, false)
	})
}

func TestCheckTypeAndOp(t *testing.T) {
	for _, tp := range []Type{Int32, Int64, String, Double, Timestamp} {
		for _, op := range []Operator{Eq, Lt, LtOrEq, Gt, GtOrEq} {
			assert.NoError(t, checkTypeAndOp(tp, op))
		}
	}

	for _, tp := range []Type{TUUID, Blob, Bool} {
		for _, op := range []Operator{Lt, LtOrEq, Gt, GtOrEq} {
			assert.Error(t, checkTypeAndOp(tp, op))
		}
		assert.NoError(t, checkTypeAndOp(tp, Eq))
	}
}

func TestCompare(t *testing.T) {
	type testCase struct {
		tp       Type
		a, b     interface{}
		expected int
	}

	cases := []testCase{
		{Int64, int64(0), int64(1), -1},
		{Int64, int64(1), int64(0), 1},
		{Int64, int64(1), int64(1), 0},
		{Int32, int32(0), int32(1), -1},
		{Int32, int32(1), int32(0), 1},
		{Int32, int32(1), int32(1), 0},
		{String, "abc", "defg", -1},
		{String, "defg", "abc", 1},
		{String, "abc", "abc", 0},
		{Double, float64(0.0), float64(1.0), -1},
		{Double, float64(1.0), float64(0.0), 1},
		{Double, float64(1.0), float64(1.0), 0},
		{Timestamp, time.Unix(5, 0), time.Unix(6, 0), -1},
		{Timestamp, time.Unix(6, 0), time.Unix(5, 0), 1},
		{Timestamp, time.Unix(5, 0), time.Unix(5, 0), 0},
	}

	for _, c := range cases {
		assert.Equal(t, c.expected, compare(c.tp, c.a, c.b), fmt.Sprintf("%v", c))
	}
	assert.Panics(t, func() {
		compare(Invalid, 0, 0)
	})
}

func TestEnsureValidConditions(t *testing.T) {
	type testCase struct {
		tp         Type
		conditions []*Condition
		valid      bool
		msg        string
	}

	cases := []testCase{
		{TUUID, []*Condition{{Eq, UUID("267275CD-D312-4EFB-A304-020A43971D68")}}, true, "single condition on UUID"},
		{String, []*Condition{{Lt, "zzz"}, {Gt, "aaa"}}, true, "valid Lt Gt string"},
		{Timestamp, []*Condition{{LtOrEq, time.Now()}, {Gt, time.Unix(0, 0)}}, true, "valid LtOrEq - Gt timestamp"},
		{Double, []*Condition{{GtOrEq, float64(1.0)}, {Lt, float64(100)}}, true, "valid GtOrEq - Lt double"},
		{Int32, []*Condition{{GtOrEq, int32(100)}, {LtOrEq, int32(100)}}, true, "valid GtOrEq - LtOrEq int32"},
		{Int64, []*Condition{{Eq, int64(100)}, {Eq, int64(99)}}, false, "two Eqs"},
		{Int64, []*Condition{{Lt, int64(100)}, {LtOrEq, int64(99)}}, false, "mutually exclusive Lt - LtOrEq"},
		{Int32, []*Condition{{Lt, int32(100)}, {Gt, int32(2)}, {Gt, int32(0)}}, false, "more than 3 conditions"},
		{Int32, []*Condition{{Eq, int32(100)}, {LtOrEq, int32(100)}}, false, "Eq excludes any other condition"},
		{Int32, []*Condition{{Lt, int32(100)}, {Gt, int32(200)}}, false, "invalid range <100 && > 200"},
		{Int32, []*Condition{{GtOrEq, int32(200)}, {LtOrEq, int32(100)}}, false, "invalid >=200 && <= 100"},
		{Int32, []*Condition{{GtOrEq, int32(200)}, {Lt, int32(100)}}, false, "invalid >=200 && < 100"},
		{Int32, []*Condition{{LtOrEq, int32(100)}, {Gt, int32(200)}}, false, "invalid <=100 && > 200"},
	}

	for _, c := range cases {
		if c.valid {
			assert.NoError(t, ensureValidConditions(c.tp, c.conditions), c.msg)
		} else {
			assert.Error(t, ensureValidConditions(c.tp, c.conditions), c.msg)
		}
	}
}
