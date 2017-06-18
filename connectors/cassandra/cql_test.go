package cassandra_test

import (
	"testing"

	"code.uber.internal/infra/dosa-gateway/datastore/engine/cassandra"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
)

func TestInsertStmt(t *testing.T) {
	data := []struct {
		keyspace   string
		table      string
		columns    []string
		values     []interface{}
		stmt       string
		ifnotexist bool
	}{
		{
			keyspace: "ks",
			table:    "t",
			columns:  []string{"c1", "c2"},
			values:   []interface{}{"string", "sing"},
			stmt:     "INSERT INTO \"ks\".\"t\" (\"c1\", \"c2\") VALUES (?, ?);",
		},
		{
			keyspace:   "ks",
			table:      "t",
			columns:    []string{"c1", "c2"},
			values:     []interface{}{"string", "sing"},
			ifnotexist: true,
			stmt:       "INSERT INTO \"ks\".\"t\" (\"c1\", \"c2\") VALUES (?, ?) IF NOT EXISTS;",
		},
		{
			keyspace: "ks",
			table:    "t",
			columns:  []string{"c1"},
			values:   []interface{}{"string", "sing"},
			stmt:     "INSERT INTO \"ks\".\"t\" (\"c1\") VALUES (?, ?);",
		},
	}
	for _, d := range data {
		stmt, err := cassandra.InsertStmt(
			cassandra.Keyspace(d.keyspace),
			cassandra.Table(d.table),
			cassandra.Columns(d.columns),
			cassandra.Values(d.values),
			cassandra.IfNotExist(d.ifnotexist),
		)
		assert.Nil(t, err)
		assert.Equal(t, stmt, d.stmt)
	}
}

func TestSelectStmt(t *testing.T) {
	conds := []*cassandra.ColumnCondition{
		{
			Name:      "a",
			Condition: &dosa.Condition{Op: dosa.Eq, Value: 4},
		},
		{
			Name:      "a",
			Condition: &dosa.Condition{Op: dosa.Lt, Value: 5},
		},
		{
			Name:      "a",
			Condition: &dosa.Condition{Op: dosa.LtOrEq, Value: 2},
		},
		{
			Name:      "d",
			Condition: &dosa.Condition{Op: dosa.Gt, Value: 0},
		},
		{
			Name:      "a",
			Condition: &dosa.Condition{Op: dosa.GtOrEq, Value: 3},
		},
		{
			Name:      "b",
			Condition: &dosa.Condition{Op: dosa.Eq, Value: 9},
		},
		{
			Name:      "c",
			Condition: &dosa.Condition{Op: dosa.Lt, Value: 1},
		},
		{
			Name:      "c",
			Condition: &dosa.Condition{Op: dosa.Gt, Value: 0},
		},
	}
	data := []struct {
		keyspace string
		table    string
		columns  []string
		stmt     string
		limit    int
		conds    []*cassandra.ColumnCondition
	}{
		{
			keyspace: "ks",
			table:    "t",
			columns:  []string{"c1", "c2"},
			conds:    conds,
			stmt:     "SELECT \"c1\", \"c2\" FROM \"ks\".\"t\" WHERE \"a\"=? AND \"a\"<? AND \"a\"<=? AND \"d\">? AND \"a\">=? AND \"b\"=? AND \"c\"<? AND \"c\">?;",
		},
		{
			keyspace: "ks",
			table:    "t",
			columns:  []string{"c1", "c2"},
			conds:    conds,
			limit:    5,
			stmt:     "SELECT \"c1\", \"c2\" FROM \"ks\".\"t\" WHERE \"a\"=? AND \"a\"<? AND \"a\"<=? AND \"d\">? AND \"a\">=? AND \"b\"=? AND \"c\"<? AND \"c\">? LIMIT 5;",
		},
		{
			keyspace: "ks",
			table:    "t",
			columns:  []string{"c1"},
			conds:    conds,
			stmt:     "SELECT \"c1\" FROM \"ks\".\"t\" WHERE \"a\"=? AND \"a\"<? AND \"a\"<=? AND \"d\">? AND \"a\">=? AND \"b\"=? AND \"c\"<? AND \"c\">?;",
		},
		{
			keyspace: "ks",
			table:    "t",
			columns:  []string{"c1"},
			limit:    0,
			stmt:     "SELECT \"c1\" FROM \"ks\".\"t\";",
		},
	}
	for _, d := range data {
		stmt, err := cassandra.SelectStmt(
			cassandra.Keyspace(d.keyspace),
			cassandra.Table(d.table),
			cassandra.Columns(d.columns),
			cassandra.Conditions(d.conds),
			cassandra.Limit(d.limit),
		)
		assert.NoError(t, err)
		assert.Equal(t, stmt, d.stmt)
	}
}

func TestDeleteStmt(t *testing.T) {
	conds := []*cassandra.ColumnCondition{
		{
			Name:      "a",
			Condition: &dosa.Condition{Op: dosa.Eq, Value: 4},
		},
		{
			Name:      "b",
			Condition: &dosa.Condition{Op: dosa.Eq, Value: 9},
		},
		{
			Name:      "c",
			Condition: &dosa.Condition{Op: dosa.Eq, Value: 1},
		},
	}
	data := []struct {
		keyspace string
		table    string
		stmt     string
		conds    []*cassandra.ColumnCondition
	}{
		{
			keyspace: "ks",
			table:    "t",
			conds:    conds,
			stmt:     "DELETE FROM \"ks\".\"t\" WHERE \"a\"=? AND \"b\"=? AND \"c\"=?;",
		},
	}
	for _, d := range data {
		stmt, err := cassandra.DeleteStmt(
			cassandra.Keyspace(d.keyspace),
			cassandra.Table(d.table),
			cassandra.Conditions(d.conds),
		)
		assert.NoError(t, err)
		assert.Equal(t, stmt, d.stmt)
	}
}
