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
	"encoding/json"
	"fmt"
	"time"
)

// NullTime represents a nullable instance of time.Time.
type NullTime struct {
	Time  time.Time
	Valid bool
}

// NewNullTime returns a valid NullTime object that is initialized to the
// specified time value.
func NewNullTime(v time.Time) NullTime {
	return NullTime{
		Time:  v,
		Valid: true,
	}
}

// Set is the preferred method to initialize the NullTime to a valid value.
func (i *NullTime) Set(v time.Time) {
	i.Time = v
	i.Valid = true
}

// Nullify marks the object as null.
func (i *NullTime) Nullify() {
	i.Valid = false
}

// Get is the preferred method to access the underlying value. It returns ErrNullValue
// if there's no valid value associated with the object.
func (i NullTime) Get() (time.Time, error) {
	if !i.Valid {
		return time.Time{}, ErrNullValue
	}

	return i.Time, nil
}

// MarshalText encodes a NullTime into text representation. This is helpful
// to cleanly facilitate encoding of structs that embed NullTime as a field.
func (i NullTime) MarshalText() ([]byte, error) {
	if !i.Valid {
		return []byte("null"), nil
	}

	return i.Time.MarshalText()
}

// UnmarshalText decodes a NullTime object from the byte representation. This is
// helpful in decoding structs that embed NullTime as a field.
func (i *NullTime) UnmarshalText(data []byte) error {
	str := string(data)
	if str == "" || str == "null" {
		i.Valid = false
		return nil
	}

	if err := i.Time.UnmarshalText(data); err != nil {
		i.Valid = false
		return err
	}

	i.Valid = true
	return nil
}

// MarshalJSON encodes a NullTime object into appropriate JSON representation.
// It encodes a nil value as "null".
func (i NullTime) MarshalJSON() ([]byte, error) {
	if !i.Valid {
		return []byte("null"), nil
	}

	return i.Time.MarshalJSON()
}

// UnmarshalJSON decodes a NullTime object from the specific json blob.
func (i *NullTime) UnmarshalJSON(data []byte) error {
	var err error
	var unknown interface{}
	if err := json.Unmarshal(data, &unknown); err != nil {
		return err
	}

	switch unknown.(type) {
	case string:
		err = i.Time.UnmarshalJSON(data)
	case nil:
		i.Valid = false
		return nil
	default:
		err = fmt.Errorf("failed to unmarshal %T into NullTime", unknown)
	}

	i.Valid = err == nil
	return err
}
