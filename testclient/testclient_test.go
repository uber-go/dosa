package testclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/testentity"
)

func TestTestClient(t *testing.T) {
	client, err := NewTestClient("testscope", "testprefix", &testentity.TestEntity{})
	assert.NoError(t, err)

	err = client.Initialize(context.Background())
	assert.NoError(t, err)

	uuid := dosa.NewUUID()
	testEnt := testentity.TestEntity{
		UUIDKey:  uuid,
		StrKey:   "key",
		Int64Key: 1,
		StrV:     "hello",
	}

	err = client.Upsert(context.Background(), nil, &testEnt)
	assert.NoError(t, err)

	readEnt := testentity.TestEntity{
		UUIDKey:  uuid,
		StrKey:   "key",
		Int64Key: 1,
	}

	err = client.Read(context.Background(), nil, &readEnt)
	assert.NoError(t, err)

	assert.Equal(t, "hello", readEnt.StrV)
}
