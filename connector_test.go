package dosa

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestErrorIsBadArgs(t *testing.T) {
	badArgsErr := NewErrBadArgs("you can't use that!")
	assert.True(t, ErrorIsBadArgs(badArgsErr))

	regErr := errors.New("hello")
	assert.False(t, ErrorIsBadArgs(regErr))
}
