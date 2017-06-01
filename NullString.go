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
)

// NullString is a type that holds an optional boolean value.
type NullString struct {
	sql.NullString
}

// NewNullString returns a new NullString object that is initialized to the
// specified value.
func NewNullString(value string) NullString {
	return NullString{
		sql.NullString{
			Valid:  true,
			String: value,
		},
	}
}

// Set is the preferred method to initialize the NullString to a valid value.
func (i *NullString) Set(v string) {
	i.String = v
	i.Valid = true
}

// Nullify marks the object as null.
func (i *NullString) Nullify() {
	i.Valid = false
}

// Get is the preferred method to access the underlying value. It returns ErrNullValue
// if there's no valid value associated with the object.
func (i NullString) Get() (string, error) {
	if !i.Valid {
		return "", ErrNullValue
	}

	return i.String, nil
}

// MarshalText encodes a NullString into text representation. This is helpful
// to cleanly facilitate encoding of structs that embed NullString as a field.
func (i NullString) MarshalText() ([]byte, error) {
	if !i.Valid {
		return []byte{}, nil
	}

	return []byte(i.String), nil
}

// UnmarshalText decodes a NullString object from the byte representation. This is
// helpful in decoding structs that embed NullString as a field.
func (i *NullString) UnmarshalText(data []byte) error {
	i.String = string(data)
	i.Valid = i.String != ""

	return nil
}

// MarshalJSON encodes a NullString object into appropriate JSON representation.
// It encodes a nil value as "null".
func (i NullString) MarshalJSON() ([]byte, error) {
	if !i.Valid {
		return []byte("null"), nil
	}

	return json.Marshal(i.String)
}

// UnmarshalJSON decodes a NullString object from the specific json blob.
func (i *NullString) UnmarshalJSON(data []byte) error {
	var err error
	var unknown interface{}
	if err := json.Unmarshal(data, &unknown); err != nil {
		return err
	}

	switch v := unknown.(type) {
	case string:
		i.String = string(v)
	case nil:
		i.Valid = false
		return nil
	default:
		err = fmt.Errorf("failed to unmarshal %T into NullString", unknown)
	}

	i.Valid = err == nil
	return err
}
