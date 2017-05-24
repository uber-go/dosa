// Copyright (c) 2017 Uber Technologies, Inc.
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

package random

import (
	"context"

	"math/rand"
	"time"

	"github.com/satori/go.uuid"
	"github.com/uber-go/dosa"
)

// Connector is a connector implementation for testing
type Connector struct{}

// CreateIfNotExists always succeeds
func (c *Connector) CreateIfNotExists(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	return nil
}

const maxBlobSize = 32
const maxStringSize = 64

func randomString(slen int) string {
	var validRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$%^&*()_-+={[}];:,.<>/?")
	str := make([]rune, slen)
	for i := range str {
		str[i] = validRunes[rand.Intn(len(validRunes))]
	}
	return string(str)
}

// Data generates some random data. Because our test is blackbox in a different package,
// we have to export this
func Data(ei *dosa.EntityInfo, minimumFields []string) map[string]dosa.FieldValue {
	var result = map[string]dosa.FieldValue{}
	for _, field := range minimumFields {
		var v dosa.FieldValue
		cd := ei.Def.FindColumnDefinition(field)
		switch cd.Type {
		case dosa.Int32:
			v = dosa.FieldValue(rand.Int31())
		case dosa.Int64:
			v = dosa.FieldValue(rand.Int63())
		case dosa.Bool:
			if rand.Intn(2) == 0 {
				v = dosa.FieldValue(false)
			} else {
				v = dosa.FieldValue(true)
			}
		case dosa.Blob:
			// Blobs vary in length from 1 to maxBlobSize bytes
			blen := rand.Intn(maxBlobSize-1) + 1
			blob := make([]byte, blen)
			for i := range blob {
				blob[i] = byte(rand.Intn(256))
			}
			v = dosa.FieldValue(blob)
		case dosa.String:
			slen := rand.Intn(maxStringSize) + 1
			v = dosa.FieldValue(randomString(slen))
		case dosa.Double:
			v = dosa.FieldValue(rand.Float64())
		case dosa.Timestamp:
			v = dosa.FieldValue(time.Unix(0, rand.Int63()/2))
		case dosa.TUUID:
			v = dosa.FieldValue(uuid.NewV4())
		default:
			panic("invalid type " + cd.Type.String())

		}
		result[field] = v
	}
	return result
}

// Read always returns random data of the type specified
func (c *Connector) Read(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue, minimumFields []string) (map[string]dosa.FieldValue, error) {

	return Data(ei, minimumFields), nil
}

// MultiRead returns a set of random data for each key you specify
func (c *Connector) MultiRead(ctx context.Context, ei *dosa.EntityInfo, values []map[string]dosa.FieldValue, minimumFields []string) ([]*dosa.FieldValuesOrError, error) {
	vals := make([]*dosa.FieldValuesOrError, len(values))
	for inx := range values {
		vals[inx] = &dosa.FieldValuesOrError{
			Values: Data(ei, minimumFields),
		}
	}
	return vals, nil
}

// Upsert throws away the data you upsert
func (c *Connector) Upsert(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	return nil
}

// makeErrorSlice is a handy function to make a slice of errors or nil errors
func makeErrorSlice(len int, e error) []error {
	errors := make([]error, len)
	for inx := 0; inx < len; inx = inx + 1 {
		errors[inx] = e
	}
	return errors
}

// MultiUpsert throws away all the data you upsert, returning a set of no errors
func (c *Connector) MultiUpsert(ctx context.Context, ei *dosa.EntityInfo, values []map[string]dosa.FieldValue) ([]error, error) {
	return makeErrorSlice(len(values), nil), nil
}

// Remove always returns a not found error
func (c *Connector) Remove(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	return &dosa.ErrNotFound{}
}

// MultiRemove returns a not found error for each value
func (c *Connector) MultiRemove(ctx context.Context, ei *dosa.EntityInfo, multiValues []map[string]dosa.FieldValue) ([]error, error) {
	return makeErrorSlice(len(multiValues), &dosa.ErrNotFound{}), nil
}

// Range returns a random set of data, and a random continuation token
func (c *Connector) Range(ctx context.Context, ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition, minimumFields []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	vals := make([]map[string]dosa.FieldValue, limit)
	for inx := range vals {
		vals[inx] = Data(ei, minimumFields)
	}
	return vals, randomString(32), nil
}

// Search also returns a random set of data, just like Range
func (c *Connector) Search(ctx context.Context, ei *dosa.EntityInfo, fieldPairs dosa.FieldNameValuePair, minimumFields []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	return c.Range(ctx, ei, map[string][]*dosa.Condition{}, minimumFields, token, limit)
}

// Scan also returns a random set of data, like Range and Search
func (c *Connector) Scan(ctx context.Context, ei *dosa.EntityInfo, minimumFields []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	return c.Range(ctx, ei, map[string][]*dosa.Condition{}, minimumFields, token, limit)
}

// CheckSchema always returns a slice of int32 values that match its index
func (c *Connector) CheckSchema(ctx context.Context, scope, namePrefix string, ed []*dosa.EntityDefinition) (int32, error) {
	return int32(1), nil
}

// UpsertSchema always returns a slice of int32 values that match its index
func (c *Connector) UpsertSchema(ctx context.Context, scope, namePrefix string, ed []*dosa.EntityDefinition) (*dosa.SchemaStatus, error) {
	return &dosa.SchemaStatus{Version: int32(1), Status: "ACCEPTED"}, nil
}

// CheckSchemaStatus always returns a schema status with version 1 and ACCEPTED
func (c *Connector) CheckSchemaStatus(ctx context.Context, scope, namePrefix string, version int32) (*dosa.SchemaStatus, error) {
	return &dosa.SchemaStatus{
		Version: int32(1),
		Status:  "ACCEPTED",
	}, nil
}

// CreateScope returns success
func (c *Connector) CreateScope(ctx context.Context, scope string) error {
	return nil
}

// TruncateScope returns success
func (c *Connector) TruncateScope(ctx context.Context, scope string) error {
	return nil
}

// DropScope returns success
func (c *Connector) DropScope(ctx context.Context, scope string) error {
	return nil
}

// ScopeExists is not implemented yet
func (c *Connector) ScopeExists(ctx context.Context, scope string) (bool, error) {
	return true, nil
}

// Shutdown always returns nil
func (c *Connector) Shutdown() error {
	return nil
}
