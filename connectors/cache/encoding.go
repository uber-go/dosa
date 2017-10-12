package cache

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
)

// Encoder serializes and deserializes the data in cache
type Encoder interface {
	Encode(interface{}) ([]byte, error)
	Decode([]byte, interface{}) error
}

// NewJSONEncoder returns a json encoder
func NewJSONEncoder() Encoder {
	return &jsonEncoder{}
}

type jsonEncoder struct{}

func (j *jsonEncoder) Encode(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (j *jsonEncoder) Decode(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// NewGobEncoder returns a gob encoder
func NewGobEncoder() Encoder {
	return &gobEncoder{}
}

type gobEncoder struct{}

func (g *gobEncoder) Encode(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	e := gob.NewEncoder(&buf)
	err := e.Encode(v)
	return buf.Bytes(), err
}

func (g *gobEncoder) Decode(data []byte, v interface{}) error {
	e := gob.NewDecoder(bytes.NewBuffer(data))
	return e.Decode(v)
}
