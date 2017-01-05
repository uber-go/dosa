package dosa

//go:generate stringer -type=Type

// Type defines a data type for an entity field
type Type int

const (
	// TUUID is different from dosa.UUID
	TUUID Type = iota

	// String represents a string
	String

	// Int32 represents an int32
	Int32

	// Int64 represents either an int64 or a regular int
	Int64

	// Double is a float64
	Double

	// Blob is a byte array
	Blob

	// Timestamp is a time.Time
	Timestamp

	// Bool is a bool type
	Bool
)
