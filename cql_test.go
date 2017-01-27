package dosa

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCQL(t *testing.T) {
	data := []struct {
		Instance  DomainObject
		Statement string
	}{
		{
			Instance:  &SinglePrimaryKey{},
			Statement: `create table "singleprimarykey" ("primarykey" bigint, "data" text, primary key (primarykey))`,
		},
		{
			Instance:  &AllTypes{},
			Statement: `create table "alltypes" ("booltype" boolean, "int32type" int, "int64type" bigint, "doubletype" double, "stringtype" text, "blobtype" blob, "timetype" timestamp, "uuidtype" uuid, primary key (booltype))`,
		},
		// TODO: Add more test cases
	}

	for _, d := range data {
		table, err := TableFromInstance(d.Instance)
		assert.Nil(t, err) // this code does not test TableFromInstance
		statement := table.EntityDefinition.CqlCreateTable()
		assert.Equal(t, statement, d.Statement, fmt.Sprintf("Instance: %T", d.Instance))
	}
}

func BenchmarkCQL(b *testing.B) {
	table, _ := TableFromInstance(&AllTypes{})
	for i := 0; i < b.N; i++ {
		table.EntityDefinition.CqlCreateTable()
	}
}

func TestTypemapUnknown(t *testing.T) {
	assert.Equal(t, "unknown", typemap(Invalid))
}
