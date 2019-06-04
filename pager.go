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

package dosa

import (
	"fmt"
	"io"
)

type pager struct {
	limit        int
	token        string
	fieldsToRead []string
}

func addLimitTokenString(w io.Writer, limit int, token string) {
	if limit == AdaptiveRangeLimit || limit > 0 {
		_, _ = fmt.Fprintf(w, " limit %d", limit)
	}
	if token != "" {
		_, _ = fmt.Fprintf(w, " token %q", token)
	}
}

func (p pager) equals(p2 pager) bool {
	if len(p.fieldsToRead) != len(p2.fieldsToRead) {
		return false
	}

	for i, field := range p.fieldsToRead {
		if p2.fieldsToRead[i] != field {
			return false
		}
	}

	return p.limit == p2.limit && p.token == p2.token
}
