package dosa

import "fmt"

type ETLState string

const (
	EtlOn ETLState = "on"
	EtlOff ETLState = "off"
)

func ToETLState(s string) (ETLState, error) {
	st := ETLState(s)
	switch st {
	case EtlOn:
		return EtlOn, nil
	case EtlOff:
		return EtlOff, nil
	default:
		return EtlOff, fmt.Errorf("undefined tag: %s", s)
	}
}
