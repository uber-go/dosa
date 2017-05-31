package dosa

import "github.com/pkg/errors"

// ErrNullValue is returned if a caller tries to call Get() on a nullable primitive value.
var ErrNullValue = errors.New("Value is null")
