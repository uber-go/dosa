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
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
)

// NullUUID holds an optional UUID value.
type NullUUID struct {
	Valid bool
	UUID  string
}

// NewNullUUID returns a new NullUUID object.
func NewNullUUID() NullUUID {
	return NullUUID{
		Valid: true,
		UUID:  uuid.NewV4().String(),
	}
}

// NullUUIDFromString returns a NullUUID object from specified string.
func NullUUIDFromString(v string) (NullUUID, error) {
	result := NullUUID{}
	err := result.Set(v)
	if err != nil {
		return result, err
	}

	return result, nil
}

// NullUUIDFromBytes returns a NullUUID object from specified byte slice.
func NullUUIDFromBytes(v []byte) (NullUUID, error) {
	result := NullUUID{}
	guid, err := uuid.FromBytes(v)
	if err != nil {
		return result, err
	}

	result.Valid = true
	result.UUID = guid.String()
	return result, nil
}

// Bytes gets the bytes from a UUID
func (u NullUUID) Bytes() ([]byte, error) {
	if u.Valid {
		id, err := uuid.FromString(u.UUID)
		if err != nil {
			return nil, errors.Wrap(err, "invalid uuid string")
		}
		return id.Bytes(), nil
	}

	return nil, ErrNullValue
}

// Set is preferred method to initialize the NullUUID object to a specific UUID.
func (u *NullUUID) Set(v string) error {
	// Ensure its a valid UUID
	_, err := uuid.FromString(v)
	if err != nil {
		return errors.Wrap(err, "invalid uuid string")
	}

	u.Valid = true
	u.UUID = v
	return nil
}

// Get is the preferred method to access the underlying value. Its return ErrNullValue
// if the value is null.
func (u NullUUID) Get() (string, error) {
	if !u.Valid {
		return "", ErrNullValue
	}

	return u.UUID, nil
}

// Nullify marks the object as null.
func (u *NullUUID) Nullify() {
	u.Valid = false
}

// MarshalText encodes a NullUUID  into text representation. This is helpful
// to cleanly facilitate encoding of structs that embed NullUUID as a field.
func (u NullUUID) MarshalText() ([]byte, error) {
	if !u.Valid {
		return []byte{}, nil
	}

	return []byte(u.UUID), nil
}

// UnmarshalText decodes a NullUUID object from the byte representation. This is
// helpful in decoding structs that embed NullUUID as a field.
func (u *NullUUID) UnmarshalText(data []byte) error {
	u.UUID = string(data)
	u.Valid = u.UUID != ""

	return nil
}

// MarshalJSON encodes a NullUUID object into appropriate JSON representation.
// It encodes a nil value as "null".
func (u NullUUID) MarshalJSON() ([]byte, error) {
	if !u.Valid {
		return []byte("null"), nil
	}

	return json.Marshal(u.UUID)
}

// UnmarshalJSON decodes a NullUUID object from the specific json blob.
func (u *NullUUID) UnmarshalJSON(data []byte) error {
	var err error
	var unknown interface{}
	if err := json.Unmarshal(data, &unknown); err != nil {
		return err
	}

	switch v := unknown.(type) {
	case string:
		err = u.Set(string(v))
	case nil:
		u.Valid = false
		return nil
	default:
		err = fmt.Errorf("failed to unmarshal %T into NullUUID", unknown)
	}

	u.Valid = err == nil
	return err
}
