package dosa

import "fmt"

// ETLState is a custom type based on the entity ETL tag
type ETLState string

const (
	// EtlOn means ETL on the entity is turn ON
	EtlOn ETLState = "on"
	// EtlOff means ETL on the entity is turn OFF
	EtlOff ETLState = "off"
)

const errUnrecognizedETLState = "unrecognized ETL state"

// ToETLState converts the etl tag value (in string) to the corresponding ETLState
func ToETLState(s string) (ETLState, error) {
	st := ETLState(s)
	switch st {
	case EtlOn:
		return EtlOn, nil
	case EtlOff:
		return EtlOff, nil
	default:
		return EtlOff, fmt.Errorf("%s: %s", errUnrecognizedETLState, s)
	}
}
