package dosa

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestETLState(t *testing.T) {

	testCases := []struct {
		name     string
		etl      string
		expected ETLState
		err      error
	}{
		{
			name:     "ETL On",
			etl:      "on",
			expected: EtlOn,
			err:      nil,
		},
		{
			name:     "ETL Off",
			etl:      "off",
			expected: EtlOff,
			err:      nil,
		},
		{
			name:     "ETL invalid",
			etl:      "boom",
			expected: EtlOff,
			err:      fmt.Errorf("%s: boom", errUnrecognizedETLState),
		},
	}

	for _, tc := range testCases {
		t.Log(tc.name)
		s, err := ToETLState(tc.etl)
		assert.Equal(t, tc.err, err)
		assert.Equal(t, tc.expected, s)
	}
}
