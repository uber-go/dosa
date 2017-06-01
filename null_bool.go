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

// NullBool is a type that holds an optional boolean value.
type NullBool struct {
	sql.NullBool
}

// NewNullBool returns a new NullBool object that is initialized to the
// specified value.
func NewNullBool(value bool) NullBool {
	return NullBool{
		sql.NullBool{
			Valid: true,
			Bool:  value,
		},
	}
}

// Set is the preferred method to initialize the NullBool to a valid value.
func (i *NullBool) Set(v bool) {
	i.Bool = v
	i.Valid = true
}

// Nullify marks the object as null.
func (i *NullBool) Nullify() {
	i.Valid = false
}

// Get is the preferred method to access the underlying value. It returns ErrNullValue
// if there's no valid value associated with the object.
func (i NullBool) Get() (bool, error) {
	if !i.Valid {
		return false, ErrNullValue
	}

	return i.Bool, nil
}

// MarshalText encodes a NullBool into text representation. This is helpful
// to cleanly facilitate encoding of structs that embed NullBool as a field.
func (i NullBool) MarshalText() ([]byte, error) {
	if !i.Valid {
		return []byte{}, nil
	}

	return []byte(strconv.FormatBool(i.Bool)), nil
}

// UnmarshalText decodes a NullBool object from the byte representation. This is
// helpful in decoding structs that embed NullBool as a field.
func (i *NullBool) UnmarshalText(data []byte) error {
	str := string(data)
	if str == "" || str == "null" {
		i.Valid = false
		return nil
	}

	var err error
	i.Bool, err = strconv.ParseBool(str)
	i.Valid = err == nil
	return err
}

// MarshalJSON encodes a NullBool object into appropriate JSON representation.
// It encodes a nil value as "null".
func (i NullBool) MarshalJSON() ([]byte, error) {
	if !i.Valid {
		return []byte("null"), nil
	}

	return []byte(strconv.FormatBool(i.Bool)), nil
}

// UnmarshalJSON decodes a NullBool object from the specific json blob.
func (i *NullBool) UnmarshalJSON(data []byte) error {
	var err error
	var unknown interface{}
	if err := json.Unmarshal(data, &unknown); err != nil {
		return err
	}

	switch unknown.(type) {
	case bool:
		err = json.Unmarshal(data, &i.Bool)
	case nil:
		i.Valid = false
		return nil
	default:
		err = fmt.Errorf("failed to unmarshal %T into NullBool", unknown)
	}

	i.Valid = err == nil
	return err
}
