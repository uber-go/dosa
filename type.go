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
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
)

//go:generate stringer -type=Type

// Type defines a data type for an entity field
type Type int

const (
	// Invalid type to be used as zero value for Type
	Invalid Type = iota

	// TUUID is different from dosa.UUID
	TUUID

	// String represents a string
	String

	// Int32 represents an int32
	Int32

	// Int64 represents either an int64 or a regular int
	Int64

	// Double is a float64
	Double

	// Blob is a byte slice
	Blob

	// Timestamp is a time.Time
	Timestamp

	// Bool is a bool type
	Bool

	// TNullString represents an optional string value
	TNullString

	// TNullInt64 represents an optional int64 value
	TNullInt64

	// TNullFloat64 represents an optional float64 value
	TNullFloat64

	// TNullBool represents an optional bool value
	TNullBool
)

// UUID stores a string format of uuid.
// Validation is done before saving to datastore.
// The format of uuid used in datastore is orthogonal to the string format here.
type UUID string

// NewUUID is a helper for returning a new dosa.UUID value
func NewUUID() UUID {
	return UUID(uuid.NewV4().String())
}

// Bytes gets the bytes from a UUID
func (u UUID) Bytes() ([]byte, error) {
	id, err := uuid.FromString(string(u))
	if err != nil {
		return nil, errors.Wrap(err, "invalid uuid string")
	}
	return id.Bytes(), nil
}

// BytesToUUID creates a UUID from a byte slice
func BytesToUUID(bs []byte) (UUID, error) {
	id, err := uuid.FromBytes(bs)
	if err != nil {
		return "", errors.Wrap(err, "invalid uuid bytes")
	}
	return UUID(id.String()), nil
}

// FromString converts string to dosa Type
func FromString(s string) Type {
	switch s {
	case TUUID.String():
		return TUUID
	case String.String():
		return String
	case Int32.String():
		return Int32
	case Int64.String():
		return Int64
	case Double.String():
		return Double
	case Blob.String():
		return Blob
	case Timestamp.String():
		return Timestamp
	case Bool.String():
		return Bool
	case TNullString.String():
		return TNullString
	case TNullInt64.String():
		return TNullInt64
	case TNullFloat64.String():
		return TNullFloat64
	case TNullBool.String():
		return TNullBool
	default:
		return Invalid
	}
}

func isInvalidPrimaryKeyType(t Type) bool {
	switch t {
	case Invalid, TNullString, TNullFloat64, TNullInt64, TNullBool:
		return true
	default:
		return false
	}
}
