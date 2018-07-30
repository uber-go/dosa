// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package encoding

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"time"

	"github.com/uber-go/dosa"
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

// Encode marshals an object using golang's encoding/json package
func (j *jsonEncoder) Encode(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// Decode unmarhsals bytes using golagn's encoding/json package
func (j *jsonEncoder) Decode(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// NewGobEncoder returns a gob encoder
func NewGobEncoder() GobEncoder {
	// Register non-primitive dosa types
	gob.Register(time.Time{})
	gob.Register(dosa.NewUUID())
	return GobEncoder{}
}

// GobEncoder implemented Encoder interface with gob
type GobEncoder struct{}

// Encode returns a series of bytes using golang's encoding/gob package
func (g GobEncoder) Encode(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	e := gob.NewEncoder(&buf)
	err := e.Encode(v)
	return buf.Bytes(), err
}

// Decode unmarshals bytes into an object using golang's encoding/gob package
func (g GobEncoder) Decode(data []byte, v interface{}) error {
	e := gob.NewDecoder(bytes.NewBuffer(data))
	return e.Decode(v)
}
