package dosa

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
)

// NullInt is a type that holds an optional int64 value.
type NullInt struct {
	Valid bool
	Value int64
}

// NewNullInt returns a new NullInt object that is initialized to the
// specified value.
func NewNullInt(value int64) NullInt {
	return NullInt{
		Valid: true,
		Value: value,
	}
}

// Set is the preferred method to initialize the NullInt to a valid value.
func (i *NullInt) Set(v int64) {
	i.Value = v
	i.Valid = true
}

// Nullify marks the object as null.
func (i *NullInt) Nullify() {
	i.Valid = false
}

// Get is the preferred method to access the underlying value. It returns ErrNullValue
// if there's no valid value associated with the object.
func (i NullInt) Get() (int64, error) {
	if !i.Valid {
		return 0, ErrNullValue
	}

	return i.Value, nil
}

// MarshalText encodes a NullInt into text representation. This is helpful
// to cleanly facilitate encoding of structs that embed NullInt as a field.
func (i NullInt) MarshalText() ([]byte, error) {
	if !i.Valid {
		return []byte{}, nil
	}

	return []byte(strconv.FormatInt(i.Value, 10)), nil
}

// UnmarshalText decodes a NullInt object from the byte representation. This is
// helpful in decoding structs that embed NullInt as a field.
func (i *NullInt) UnmarshalText(data []byte) error {
	str := string(data)
	if str == "" || str == "null" {
		i.Valid = false
		return nil
	}

	var err error
	i.Value, err = strconv.ParseInt(str, 10, 64)
	i.Valid = err == nil
	return err
}

// MarshalJSON encodes a NullInt object into appropriate JSON representation.
// It encodes a nil value as "null" otherwise uses a base-10 string representation
// of the underlying value.
func (i NullInt) MarshalJSON() ([]byte, error) {
	if !i.Valid {
		return []byte("null"), nil
	}

	return []byte(strconv.FormatInt(i.Value, 10)), nil
}

// UnmarshalJSON decodes a NullInt object from the specific json blob.
func (i *NullInt) UnmarshalJSON(data []byte) error {
	var err error
	var unknown interface{}
	if err := json.Unmarshal(data, &unknown); err != nil {
		return err
	}

	switch unknown.(type) {
	case float64:
		// json.Unmarshal by default parses numbers as float64.
		err = json.Unmarshal(data, &i.Value)
	case nil:
		i.Valid = false
		return nil
	default:
		err = fmt.Errorf("failed to unmarshal %v into NullInt", reflect.TypeOf(unknown).Name())
	}

	i.Valid = err == nil
	return err
}
