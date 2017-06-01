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
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
)

// NullFloat64 is a type that holds an optional float64 value.
type NullFloat64 struct {
	sql.NullFloat64
}

// NewNullFloat64 returns a new NullFloat64 object that is initialized to the
// specified value.
func NewNullFloat64(value float64) NullFloat64 {
	return NullFloat64{
		sql.NullFloat64{
			Valid:   true,
			Float64: value,
		},
	}
}

// Set is the preferred method to initialize the NullFloat64 to a valid value.
func (i *NullFloat64) Set(v float64) {
	i.Float64 = v
	i.Valid = true
}

// Nullify marks the object as null.
func (i *NullFloat64) Nullify() {
	i.Valid = false
}

// Get is the preferred method to access the underlying value. It returns ErrNullValue
// if there's no valid value associated with the object.
func (i NullFloat64) Get() (float64, error) {
	if !i.Valid {
		return 0, ErrNullValue
	}

	return i.Float64, nil
}

// MarshalText encodes a NullFloat64 into text representation. This is helpful
// to cleanly facilitate encoding of structs that embed NullFloat64 as a field.
func (i NullFloat64) MarshalText() ([]byte, error) {
	if !i.Valid {
		return []byte{}, nil
	}

	return []byte(strconv.FormatFloat(i.Float64, 'g', -1, 64)), nil
}

// UnmarshalText decodes a NullFloat64 object from the byte representation. This is
// helpful in decoding structs that embed NullFloat64 as a field.
func (i *NullFloat64) UnmarshalText(data []byte) error {
	str := string(data)
	if str == "" || str == "null" {
		i.Valid = false
		return nil
	}

	var err error
	i.Float64, err = strconv.ParseFloat(str, 64)
	i.Valid = err == nil
	return err
}

// MarshalJSON encodes a NullFloat64 object into appropriate JSON representation.
// It encodes a nil value as "null" otherwise uses a base-10 string representation
// of the underlying value.
func (i NullFloat64) MarshalJSON() ([]byte, error) {
	if !i.Valid {
		return []byte("null"), nil
	}

	return []byte(strconv.FormatFloat(i.Float64, 'g', -1, 64)), nil
}

// UnmarshalJSON decodes a NullFloat64 object from the specific json blob.
func (i *NullFloat64) UnmarshalJSON(data []byte) error {
	var err error
	var unknown interface{}
	if err := json.Unmarshal(data, &unknown); err != nil {
		return err
	}

	switch v := unknown.(type) {
	case float64:
		i.Float64 = float64(v)
	case nil:
		i.Valid = false
		return nil
	default:
		err = fmt.Errorf("failed to unmarshal %T into NullFloat64", unknown)
	}

	i.Valid = err == nil
	return err
}
