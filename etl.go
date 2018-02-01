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

import "fmt"

// ETLState is a custom type based on the entity ETL tag
type ETLState string

const (
	// EtlOn means ETL on the entity is turn ON
	EtlOn ETLState = "on"
	// EtlOff means ETL on the entity is turn OFF
	EtlOff ETLState = "off"
)

const errUnrecognizedETLState = "unrecognized ETL state"

// ToETLState converts the etl tag value (in string) to the corresponding ETLState
func ToETLState(s string) (ETLState, error) {
	st := ETLState(s)
	switch st {
	case EtlOn:
		return EtlOn, nil
	case EtlOff:
		return EtlOff, nil
	default:
		return EtlOff, fmt.Errorf("%s: %s", errUnrecognizedETLState, s)
	}
}
