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

package cassandra

import (
	"context"
	"encoding/base64"
	"sort"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
)

func prepareConditions(columnConditions map[string][]*dosa.Condition) ([]*dosa.ColumnCondition, []interface{}, error) {
	cc := dosa.NormalizeConditions(columnConditions)
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
	keyspace := c.KsMapper.Keyspace(ei.Ref.Scope, ei.Ref.NamePrefix)
	table := ei.Def.Name

	tokenBytes, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return nil, "", errors.Wrapf(err, "bad token %q", token)
	}

	fields := fieldsToRead
	if len(fields) == 0 {
		for _, c := range ei.Def.Columns {
			fields = append(fields, c.Name)
		}
	}

	sort.Strings(fields)

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

	iter := c.Session.Query(stmt, values...).PageState(tokenBytes).PageSize(limit).Iter()
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
