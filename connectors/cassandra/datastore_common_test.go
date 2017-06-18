package cassandra_test

import (
	"context"
	"log"
	"os"
	"testing"

	"code.uber.internal/infra/dosa-gateway/datastore/common"
	"code.uber.internal/infra/dosa-gateway/datastore/engine"
	"code.uber.internal/infra/dosa-gateway/datastore/engine/cassandra"
	"code.uber.internal/infra/dosa-gateway/datastore/mgmt"
	"code.uber.internal/infra/dosa-gateway/datastore/testutil"
	"code.uber.internal/infra/dosa-gateway/schema"
	"github.com/uber-go/dosa"
)

const (
	keyspace       = "datastore_test"
	tableName      = "testentity"
	uuidKeyField   = "uuidkeyfield"
	uuidField      = "uuidfield"
	stringKeyField = "stringkeyfield"
	stringField    = "stringfield"
	int32Field     = "int32field"
	int64KeyField  = "int64keyfield"
	int64Field     = "int64field"
	doubleField    = "doublefield"
	blobField      = "blobfield"
	timestampField = "timestampfield"
	boolField      = "boolfield"
)

var (
	testStore *cassandra.Connector
)

func initTestStore() error {
	management := mgmt.OpenManagement("127.0.0.1", cassandra.StapiPort, cassandra.EmbeddedAppID)
	testConfig := testutil.GetTestConfiguration()
	r := common.NewSimpleResolver()
	p := engine.NewSimpleProvider(testConfig.Engines)
	sh := schema.NewConnector(management, p, schema.NewMemoryStore(), nil, nil)
	var err error
	testStore, err = cassandra.NewConnector(
		map[string]cassandra.Config{
			testConfig.Engines[0].Name: testConfig.Engines[0].Cassandra,
		},
		r,
		sh,
	)
	if err != nil {
		panic(err)
	}
	return nil
}

func initTestSchema(ks string, entityInfo *dosa.EntityInfo) error {
	ctx := context.Background()
	err := testStore.CreateScope(ctx, ks)
	if err != nil {
		return err
	}
	_, err = testStore.UpsertSchema(
		ctx, entityInfo.Ref.Scope, entityInfo.Ref.NamePrefix, []*dosa.EntityDefinition{entityInfo.Def},
	)
	return err
}

func removeTestSchema(ks string) {
	ctx := context.Background()
	testStore.DropScope(ctx, ks)
}

var testEntityInfo = newTestEntityInfo(keyspace)

func newTestEntityInfo(sp string) *dosa.EntityInfo {
	return &dosa.EntityInfo{
		Ref: &dosa.SchemaRef{
			Scope:      sp,
			NamePrefix: "test.datastore",
			EntityName: "testentity",
		},
		Def: &dosa.EntityDefinition{
			Name: tableName,
			Key: &dosa.PrimaryKey{
				PartitionKeys: []string{uuidKeyField},
				ClusteringKeys: []*dosa.ClusteringKey{
					{
						Name:       stringKeyField,
						Descending: false,
					},
					{
						Name:       int64KeyField,
						Descending: true,
					},
				},
			},
			Columns: []*dosa.ColumnDefinition{
				{
					Name: uuidKeyField,
					Type: dosa.TUUID,
				},
				{
					Name: stringKeyField,
					Type: dosa.String,
				},
				{
					Name: int32Field,
					Type: dosa.Int32,
				},
				{
					Name: int64KeyField,
					Type: dosa.Int64,
				},
				{
					Name: doubleField,
					Type: dosa.Double,
				},
				{
					Name: blobField,
					Type: dosa.Blob,
				},
				{
					Name: timestampField,
					Type: dosa.Timestamp,
				},
				{
					Name: boolField,
					Type: dosa.Bool,
				},
				{
					Name: uuidField,
					Type: dosa.TUUID,
				},
				{
					Name: stringField,
					Type: dosa.String,
				},
				{
					Name: int64Field,
					Type: dosa.Int64,
				},
			},
		},
	}
}

func TestMain(m *testing.M) {

	if err := cassandra.EnsureServerUp("../../../embedded"); err != nil {
		log.Fatalf("Failed to started embedded STAPI server: %s", err.Error())
	}

	err := initTestStore()
	if err != nil {
		log.Fatal("failed to create test datastore: ", err)
	}

	err = initTestSchema(keyspace, testEntityInfo)
	if err != nil {
		log.Fatal("failed to upsert test schema: ", err)
	}

	// run the tests
	os.Exit(m.Run())

	removeTestSchema(keyspace)
}
