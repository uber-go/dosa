package dosa

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
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
		NullBool: sql.NullBool{
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
		err = fmt.Errorf("failed to unmarshal %v into NullBool", reflect.TypeOf(unknown).Name())
	}

	i.Valid = err == nil
	return err
}
