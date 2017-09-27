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

package redis_test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/redis"
	"github.com/uber-go/dosa/testentity"
	"golang.org/x/net/context"
)

var testRedisConfig = redis.Config{
	ServerSettings: redis.ServerConfig{
		Host: "localhost",
		Port: redis.RedisPort,
	},
	TTL: 5 * time.Second,
}

var rc = redis.NewConnector(testRedisConfig)

func TestUnimplementedFunctions(t *testing.T) {
	err := rc.CreateIfNotExists(context.TODO(), &dosa.EntityInfo{}, nil)
	assert.EqualError(t, err, new(redis.ErrNotImplemented).Error())

	_, err = rc.MultiRead(context.TODO(), &dosa.EntityInfo{}, nil, dosa.All())
	assert.EqualError(t, err, new(redis.ErrNotImplemented).Error())

	_, err = rc.MultiUpsert(context.TODO(), &dosa.EntityInfo{}, nil)
	assert.EqualError(t, err, new(redis.ErrNotImplemented).Error())

	err = rc.RemoveRange(context.TODO(), &dosa.EntityInfo{}, nil)
	assert.EqualError(t, err, new(redis.ErrNotImplemented).Error())

	_, err = rc.MultiRemove(context.TODO(), &dosa.EntityInfo{}, nil)
	assert.EqualError(t, err, new(redis.ErrNotImplemented).Error())

	_, _, err = rc.Range(context.TODO(), &dosa.EntityInfo{}, nil, dosa.All(), "", 1)
	assert.EqualError(t, err, new(redis.ErrNotImplemented).Error())

	_, _, err = rc.Search(context.TODO(), &dosa.EntityInfo{}, dosa.FieldNameValuePair{}, dosa.All(), "", 1)
	assert.EqualError(t, err, new(redis.ErrNotImplemented).Error())

	_, _, err = rc.Scan(context.TODO(), &dosa.EntityInfo{}, dosa.All(), "", 1)
	assert.EqualError(t, err, new(redis.ErrNotImplemented).Error())
}

func TestWriteReadKeyValue(t *testing.T) {
	if !redis.IsRunning() {
		t.Skip("Redis is not running")
	}

	table, _ := dosa.TableFromInstance(&testentity.KeyValue{})
	sr := dosa.SchemaRef{Scope: "example", NamePrefix: "example"}
	testEi := &dosa.EntityInfo{Ref: &sr, Def: &table.EntityDefinition}
	values := map[string]dosa.FieldValue{
		"k": []byte{1, 2, 3},
		"v": []byte{4, 5, 6},
	}
	err := rc.Upsert(context.TODO(), testEi, values)
	assert.NoError(t, err)

	readResponse, err := rc.Read(context.TODO(), testEi, map[string]dosa.FieldValue{"k": []byte{1, 2, 3}}, dosa.All())
	assert.NoError(t, err)
	assert.Equal(t, values, readResponse)
}

func TestWriteReadValueKey(t *testing.T) {
	if !redis.IsRunning() {
		t.Skip("Redis is not running")
	}

	table, _ := dosa.TableFromInstance(&testentity.ValueKey{})
	sr := dosa.SchemaRef{Scope: "example", NamePrefix: "example"}
	testEi := &dosa.EntityInfo{Ref: &sr, Def: &table.EntityDefinition}
	values := map[string]dosa.FieldValue{
		"k": []byte{1, 2, 3},
		"v": []byte{4, 5, 6},
	}
	err := rc.Upsert(context.TODO(), testEi, values)
	assert.NoError(t, err)

	readResponse, err := rc.Read(context.TODO(), testEi, map[string]dosa.FieldValue{"k": []byte{1, 2, 3}}, dosa.All())
	assert.NoError(t, err)
	assert.Equal(t, values, readResponse)
}

func TestWriteValidEntity(t *testing.T) {
	if !redis.IsRunning() {
		t.Skip("Redis is not running")
	}
	table, _ := dosa.TableFromInstance(&testentity.KeyValue{})
	sr := dosa.SchemaRef{Scope: "example", NamePrefix: "example"}
	testEi := &dosa.EntityInfo{Ref: &sr, Def: &table.EntityDefinition}
	err := rc.Upsert(context.TODO(), testEi, map[string]dosa.FieldValue{"k": []byte("testValue"), "v": []byte("test")})
	assert.NoError(t, err)
}

func TestWriteNilByteValue(t *testing.T) {
	if !redis.IsRunning() {
		t.Skip("Redis is not running")
	}
	table, _ := dosa.TableFromInstance(&testentity.KeyValue{})
	testEi := &dosa.EntityInfo{Ref: nil, Def: &table.EntityDefinition}
	err := rc.Upsert(context.TODO(), testEi, map[string]dosa.FieldValue{"k": []byte("testValue"), "v": []byte(nil)})
	assert.EqualError(t, err, "This entity schema and value not supported by redis. No value specified.")
}

func TestWriteNilValue(t *testing.T) {
	if !redis.IsRunning() {
		t.Skip("Redis is not running")
	}
	table, _ := dosa.TableFromInstance(&testentity.KeyValue{})
	testEi := &dosa.EntityInfo{Ref: nil, Def: &table.EntityDefinition}
	err := rc.Upsert(context.TODO(), testEi, map[string]dosa.FieldValue{"k": []byte("testValue"), "v": nil})
	assert.EqualError(t, err, "This entity schema and value not supported by redis. No value specified.")
}

func TestWriteInvalidEntityKey(t *testing.T) {
	table, _ := dosa.TableFromInstance(&testentity.TestEntity{})
	testEi := &dosa.EntityInfo{Ref: nil, Def: &table.EntityDefinition}
	err := rc.Upsert(context.TODO(), testEi, nil)
	assert.EqualError(t, err, "This entity schema and value not supported by redis. Should only have a single key.")
}

func TestWriteInvalidEntityValue(t *testing.T) {
	table, _ := dosa.TableFromInstance(&testentity.KeyValues{})
	testEi := &dosa.EntityInfo{Ref: nil, Def: &table.EntityDefinition}
	err := rc.Upsert(context.TODO(), testEi, nil)
	assert.EqualError(t, err, "This entity schema and value not supported by redis. Should have one key, one value.")
}

func TestReadNotFound(t *testing.T) {
	if !redis.IsRunning() {
		t.Skip("Redis is not running")
	}

	table, _ := dosa.TableFromInstance(&testentity.KeyValue{})
	sr := dosa.SchemaRef{Scope: "example", NamePrefix: "example"}
	testEi := &dosa.EntityInfo{Ref: &sr, Def: &table.EntityDefinition}
	// Expect a key not found error when reading something that does not exist in redis
	randBytes := make([]byte, 10)
	rand.Read(randBytes)
	_, err := rc.Read(context.TODO(), testEi, map[string]dosa.FieldValue{"k": randBytes}, dosa.All())
	assert.EqualError(t, err, new(dosa.ErrNotFound).Error())
}

func TestReadInvalidEntityKey(t *testing.T) {
	table, _ := dosa.TableFromInstance(&testentity.TestEntity{})
	testEi := &dosa.EntityInfo{Ref: nil, Def: &table.EntityDefinition}
	_, err := rc.Read(context.TODO(), testEi, nil, dosa.All())
	assert.EqualError(t, err, "This entity schema and value not supported by redis. Should only have a single key.")
}

func TestReadInvalidEntityValue(t *testing.T) {
	table, _ := dosa.TableFromInstance(&testentity.KeyValues{})
	testEi := &dosa.EntityInfo{Ref: nil, Def: &table.EntityDefinition}
	_, err := rc.Read(context.TODO(), testEi, nil, dosa.All())
	assert.EqualError(t, err, "This entity schema and value not supported by redis. Should have one key, one value.")
}

func TestRemove(t *testing.T) {
	if !redis.IsRunning() {
		t.Skip("Redis is not running")
	}

	table, _ := dosa.TableFromInstance(&testentity.KeyValue{})
	sr := dosa.SchemaRef{Scope: "example", NamePrefix: "example"}
	testEi := &dosa.EntityInfo{Ref: &sr, Def: &table.EntityDefinition}
	err := rc.Remove(context.TODO(), testEi, map[string]dosa.FieldValue{"k": []byte{1, 1}})
	assert.NoError(t, err)
}

func TestRemoveInvalidEntity(t *testing.T) {
	table, _ := dosa.TableFromInstance(&testentity.TestEntity{})
	testEi := &dosa.EntityInfo{Ref: nil, Def: &table.EntityDefinition}
	err := rc.Remove(context.TODO(), testEi, nil)
	assert.EqualError(t, err, "This entity schema and value not supported by redis. Should only have a single key.")
}

func TestEntityTypeNotBytes(t *testing.T) {
	table, err := dosa.TableFromInstance(&testentity.KeyValueNonByte{})
	testEi := &dosa.EntityInfo{Ref: nil, Def: &table.EntityDefinition}
	_, err = rc.Read(context.TODO(), testEi, nil, dosa.All())
	assert.EqualError(t, err, "This entity schema and value not supported by redis. Types should be []byte.")
}

func TestShutdownConnector(t* testing.T) {
	var rc = redis.NewConnector(testRedisConfig)
	err := rc.Shutdown()
	assert.NoError(t, err)
	err = rc.Shutdown()
	assert.NoError(t, err)
}