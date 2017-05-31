package dosa

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
)

// NullInt64 is a type that holds an optional int64 value.
type NullInt64 struct {
	sql.NullInt64
}

// NewNullInt64 returns a new NullInt64 object that is initialized to the
// specified value.
func NewNullInt64(value int64) NullInt64 {
	return NullInt64{
		sql.NullInt64{
			Valid: true,
			Int64: value,
		},
	}
}

// Set is the preferred method to initialize the NullInt64 to a valid value.
func (i *NullInt64) Set(v int64) {
	i.Int64 = v
	i.Valid = true
}

// Nullify marks the object as null.
func (i *NullInt64) Nullify() {
	i.Valid = false
}

// Get is the preferred method to access the underlying value. It returns ErrNullValue
// if there's no valid value associated with the object.
func (i NullInt64) Get() (int64, error) {
	if !i.Valid {
		return 0, ErrNullValue
	}

	return i.Int64, nil
}

// MarshalText encodes a NullInt64 into text representation. This is helpful
// to cleanly facilitate encoding of structs that embed NullInt64 as a field.
func (i NullInt64) MarshalText() ([]byte, error) {
	if !i.Valid {
		return []byte{}, nil
	}

	return []byte(strconv.FormatInt(i.Int64, 10)), nil
}

// UnmarshalText decodes a NullInt64 object from the byte representation. This is
// helpful in decoding structs that embed NullInt64 as a field.
func (i *NullInt64) UnmarshalText(data []byte) error {
	str := string(data)
	if str == "" || str == "null" {
		i.Valid = false
		return nil
	}

	var err error
	i.Int64, err = strconv.ParseInt(str, 10, 64)
	i.Valid = err == nil
	return err
}

// MarshalJSON encodes a NullInt64 object into appropriate JSON representation.
// It encodes a nil value as "null" otherwise uses a base-10 string representation
// of the underlying value.
func (i NullInt64) MarshalJSON() ([]byte, error) {
	if !i.Valid {
		return []byte("null"), nil
	}

	return []byte(strconv.FormatInt(i.Int64, 10)), nil
}

// UnmarshalJSON decodes a NullInt64 object from the specific json blob.
func (i *NullInt64) UnmarshalJSON(data []byte) error {
	var err error
	var unknown interface{}
	if err := json.Unmarshal(data, &unknown); err != nil {
		return err
	}

	switch unknown.(type) {
	case float64:
		// json.Unmarshal by default parses numbers as float64.
		err = json.Unmarshal(data, &i.Int64)
	case nil:
		i.Valid = false
		return nil
	default:
		err = fmt.Errorf("failed to unmarshal %T into NullInt64", unknown)
	}

	i.Valid = err == nil
	return err
}
