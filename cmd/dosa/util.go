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

package main

import (
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/pkg/errors"
	"github.com/uber-go/dosa"
)

// parseQuery parses the input query expressions
func parseQuery(exps []string) ([]*queryObj, error) {
	queries := make([]*queryObj, len(exps))
	for idx, exp := range exps {
		strs := strings.SplitN(exp, ":", 3)
		if len(strs) != 3 {
			return nil, errors.Errorf("query expression should be in the form field:op:value")
		}
		queries[idx] = newQueryObj(strs[0], strs[1], strs[2])
	}
	return queries, nil
}

// setQueryFieldValues sets the value field of queryObj
func setQueryFieldValues(queries []*queryObj, re *dosa.RegisteredEntity) ([]*queryObj, error) {
	cts := re.EntityDefinition().ColumnTypes()
	for _, query := range queries {
		// convert field name to column name, ColumnNames method will return error if field name not found
		cols, err := re.ColumnNames([]string{query.fieldName})
		if err != nil {
			return nil, err
		}
		query.colName = cols[0]
		if typ, ok := cts[cols[0]]; ok {
			fv, err := strToFieldValue(typ, query.valueStr)
			if err != nil {
				return nil, err
			}
			query.value = fv
		} else {
			return nil, errors.Errorf("cannot find the type of column %s", cols[0])
		}
	}
	return queries, nil
}

// strToFieldValue converts the 'value' of input query expression from string
// to dosa.FieldValue
func strToFieldValue(t dosa.Type, s string) (dosa.FieldValue, error) {
	switch t {
	case dosa.Int32:
		i, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, err
		}
		return dosa.FieldValue(int32(i)), nil
	case dosa.Int64:
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, err
		}
		return dosa.FieldValue(int64(i)), nil
	case dosa.Bool:
		b, err := strconv.ParseBool(s)
		if err != nil {
			return nil, err
		}
		return dosa.FieldValue(b), nil
	case dosa.String:
		return dosa.FieldValue(s), nil
	case dosa.Double:
		d, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, err
		}
		return dosa.FieldValue(d), nil
	case dosa.Timestamp:
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			// check if the input is a unix epoch timestamp
			i, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return nil, errors.Wrapf(err, "timestamp should be in form 2006-01-02T15:04:05.1Z (RFC3339Nano) or Unix epoch time in millisecond")
			}
			if i < 0 {
				return nil, errors.Errorf("timestamp should not be negative")
			}
			return dosa.FieldValue(time.Unix(0, i*int64(time.Millisecond)).UTC()), nil
		}
		return dosa.FieldValue(t), nil
	case dosa.TUUID:
		u := dosa.UUID(s)
		return dosa.FieldValue(u), nil
	case dosa.Blob:
		// TODO: support query with binary arrays
		return nil, errors.Errorf("blob query not supported for now")
	default:
		return nil, errors.Errorf("unsupported type")
	}
}

func getFields(results []map[string]dosa.FieldValue) []string {
	fieldSet := make(map[string]bool)
	for _, result := range results {
		for field := range result {
			fieldSet[field] = true
		}
	}
	var fields []string
	for field := range fieldSet {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	return fields
}

func printResults(results []map[string]dosa.FieldValue) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.Debug|tabwriter.StripEscape)
	if len(results) == 0 {
		return errors.New("Empty results")
	}
	fields := getFields(results)
	if _, err := fmt.Fprintln(w, strings.Join(fields, "\t")); err != nil {
		return errors.WithStack(err)
	}
	values := make([]string, len(fields))
	for _, result := range results {
		for idx, field := range fields {
			var value string
			if _, ok := result[field]; ok {
				value = fmt.Sprintf(
					"%s%v%s",
					[]byte{tabwriter.Escape},
					reflect.Indirect(reflect.ValueOf(result[field])),
					[]byte{tabwriter.Escape},
				)
			} else {
				value = "nil"
			}
			values[idx] = value
		}
		if _, err := fmt.Fprintln(w, strings.Join(values, "\t")); err != nil {
			return errors.WithStack(err)
		}
	}
	return w.Flush()
}
