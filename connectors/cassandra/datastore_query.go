package cassandra

import (
	"context"
	"encoding/base64"
	"fmt"
	"sort"

	"code.uber.internal/infra/dosa-gateway/datastore/common"
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
)

// ColumnCondition represents the condition of each column
type ColumnCondition struct {
	Name      string
	Condition *dosa.Condition
}

type sortedColumnCondition []*ColumnCondition

func (list sortedColumnCondition) Len() int { return len(list) }

func (list sortedColumnCondition) Swap(i, j int) { list[i], list[j] = list[j], list[i] }

func (list sortedColumnCondition) Less(i, j int) bool {
	si := list[i]
	sj := list[j]

	if si.Name != sj.Name {
		return si.Name < sj.Name
	}

	return si.Condition.Op < sj.Condition.Op
}

func prepareConditions(columnConditions map[string][]*dosa.Condition) ([]*ColumnCondition, []interface{}, error) {
	var cc []*ColumnCondition

	for column, conds := range columnConditions {
		for _, cond := range conds {
			cc = append(cc, &ColumnCondition{
				Name:      column,
				Condition: cond})
		}
	}

	sort.Sort(sortedColumnCondition(cc))
	values := make([]interface{}, len(cc))
	for i, c := range cc {
		values[i] = c.Condition.Value
		var err error
		// specially handling for uuid is needed
		if u, ok := c.Condition.Value.(dosa.UUID); ok {
			values[i], err = gocql.ParseUUID(string(u))
			if err != nil {
				return nil, nil, errors.Wrapf(err, "invalid uuid %s", u)
			}
		}
	}
	return cc, values, nil
}

// Range querys the data based on the cluster keys
func (c *Connector) Range(ctx context.Context, ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition, fieldsToRead []string,
	token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	return c.commonQuery(ctx, ei, columnConditions, fieldsToRead, token, limit)
}

func (c *Connector) commonQuery(ctx context.Context, ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition, fieldsToRead []string,
	token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	cluster, keyspace, table, err := c.resolve(ei)
	if err != nil {
		return nil, "", err
	}

	tokenBytes, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return nil, "", common.NewErrBadToken(fmt.Sprintf("bad token: %s, err: %s", token, err.Error()))
	}

	fields := fieldsToRead
	if len(fields) == 0 {
		for _, c := range ei.Def.Columns {
			fields = append(fields, c.Name)
		}
	}

	sort.Sort(sortedFields(fields))

	conds, values, err := prepareConditions(columnConditions)
	stmt, err := SelectStmt(
		Keyspace(keyspace),
		Table(table),
		Columns(fields),
		Conditions(conds),
	)
	if err != nil {
		return nil, "", errors.Wrap(err, "failed to create cql statement")
	}

	iter := cluster.session.Query(stmt, values...).PageState(tokenBytes).PageSize(limit).Iter()
	resultSet := make([]map[string]interface{}, iter.NumRows())
	pos := 0
	for {
		// New map each iteration
		row := make(map[string]interface{})
		if !iter.MapScan(row) {
			break
		}
		resultSet[pos] = row
		pos++
	}
	if err := iter.Close(); err != nil {
		return nil, "", errors.Wrapf(err, "failed execute range query in Cassandra: %s", stmt)
	}

	res := make([]map[string]dosa.FieldValue, len(resultSet))
	for i, row := range resultSet {
		res[i] = convertToDOSATypes(ei, row)
	}

	nextToken := base64.StdEncoding.EncodeToString(iter.PageState())
	return res, nextToken, nil
}

// Scan returns the data from entire table
func (c *Connector) Scan(ctx context.Context, ei *dosa.EntityInfo, fieldsToRead []string, token string,
	limit int) ([]map[string]dosa.FieldValue, string, error) {
	return c.commonQuery(ctx, ei, nil, fieldsToRead, token, limit)
}
