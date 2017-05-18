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

package dosa_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"time"

	"github.com/uber-go/dosa"
)

type AllTypes struct {
	dosa.Entity `dosa:"primaryKey=BoolType"`
	BoolType    bool
	Int32Type   int32
	Int64Type   int64
	DoubleType  float64
	StringType  string
	BlobType    []byte
	TimeType    time.Time
	UUIDType    dosa.UUID
}

func TestNewScanOp(t *testing.T) {
	assert.NotNil(t, dosa.NewScanOp(&dosa.Entity{}))
}

func TestScanOpStringer(t *testing.T) {
	for _, test := range ScanTestCases {
		assert.Equal(t, test.stringer, test.sop.String(), test.descript)
	}
}

var ScanTestCases = []struct {
	descript  string
	sop       *dosa.ScanOp
	stringer  string
	converted string
	err       string
}{
	{
		descript: "empty scanop, valid",
		sop:      dosa.NewScanOp(&AllTypes{}),
		stringer: "ScanOp",
	},
	{
		descript: "empty with limit",
		sop:      dosa.NewScanOp(&AllTypes{}).Limit(10),
		stringer: "ScanOp limit 10",
	},
	{
		descript: "empty with token",
		sop:      dosa.NewScanOp(&AllTypes{}).Offset("toketoketoke"),
		stringer: "ScanOp token \"toketoketoke\"",
	},
	{
		descript: "with valid field list",
		sop:      dosa.NewScanOp(&AllTypes{}).Fields([]string{"StringType"}),
		stringer: "ScanOp",
	},
}

func TestScanOpMatcher(t *testing.T) {
	scanOp0 := dosa.NewScanOp(&AllTypes{}).Limit(1)
	scanOp1 := dosa.NewScanOp(&AllTypes{}).Limit(1)
	scanOp2 := dosa.NewScanOp(&AllTypes{}).Limit(1).Offset("token1")
	scanOp3 := dosa.NewScanOp(&AllTypes{}).Limit(1).Fields([]string{"BoolType", "TimeType"})
	scanOp4 := dosa.NewScanOp(&dosa.Entity{}).Limit(1)

	matcher := dosa.EqScanOp(scanOp0)
	assert.True(t, matcher.Matches(scanOp1))
	assert.False(t, matcher.Matches(scanOp2))
	assert.False(t, matcher.Matches(scanOp3))
	assert.False(t, matcher.Matches(scanOp4))
	assert.False(t, matcher.Matches(3))
}
