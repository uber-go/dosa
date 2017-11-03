package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var j = NewJSONEncoder()
var g = NewGobEncoder()

func TestJsonEncoder_Decode(t *testing.T) {
	var i int
	j.Decode([]byte{50, 50}, &i)
	assert.Equal(t, 22, i)
}

func TestJsonEncoder_Encode(t *testing.T) {
	m, err := j.Encode(22)
	assert.NoError(t, err)
	assert.Equal(t, []byte{50, 50}, m)
}

func TestGobEncoder_Decode(t *testing.T) {
	var i int
	g.Decode([]byte{3, 4, 0, 44}, &i)
	assert.Equal(t, 22, i)
}

func TestGobEncoder_Encode(t *testing.T) {
	m, err := g.Encode(22)
	assert.NoError(t, err)
	assert.Equal(t, []byte{3, 4, 0, 44}, m)
}
