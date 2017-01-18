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

package dosa

import (
	"fmt"
	"reflect"
	"strings"

	"time"

	"github.com/pkg/errors"
)

const (
	entityName = "Entity"
)

func parseClusteringKeys(ss []string) ([]*ClusteringKey, error) {
	clusteringKeys := make([]*ClusteringKey, len(ss))
	for i, ck := range ss {
		fields := strings.Fields(ck)
		if len(fields) > 2 || len(fields) < 1 {
			return nil, fmt.Errorf("Clustering key definition %q should look like \"name[ asc/desc]\"",
				ck)
		}
		descending := false
		if len(fields) == 2 {
			switch strings.ToLower(fields[1]) {
			case "desc":
				descending = true
			case "asc":
				descending = false
			default:
				return nil, fmt.Errorf("invalid clustering key order %q in %q", fields[1], ck)
			}

		}

		clusteringKeys[i] = &ClusteringKey{Name: fields[0], Descending: descending}
	}
	return clusteringKeys, nil
}

func parsePrimaryKey(tableName string, s string) (*PrimaryKey, error) {
	k := PrimaryKey{}

	s = strings.TrimSpace(s)
	// remove set of matching open and close parens over whole string
	if strings.HasSuffix(s, ")") && strings.HasPrefix(s, "(") {
		s = s[1 : len(s)-1]
	}

	// look for multi-component partition key notation
	if strings.HasPrefix(s, "(") {
		closeIndex := strings.Index(s, ")")
		// complex case: (a,b),c,d
		k.PartitionKeys = strings.Split(s[1:closeIndex], ",")
		for i, pk := range k.PartitionKeys {
			k.PartitionKeys[i] = strings.TrimSpace(pk)
		}
		if closeIndex < len(s)-1 {
			if s[closeIndex+1] != ',' {
				return nil, fmt.Errorf("Object %q missing comma after partition key close parenthesis", tableName)
			}
			var err error
			k.ClusteringKeys, err = parseClusteringKeys(strings.Split(s[closeIndex+2:], ","))
			if err != nil {
				return nil, err
			}

		}
	} else {
		// not using multi-component partition key syntax, so first element
		// is the partition key, remaining elements are primary keys
		// simple case: a,b,c
		fields := strings.Split(s, ",")

		k.PartitionKeys = []string{fields[0]}
		var err error
		k.ClusteringKeys, err = parseClusteringKeys(fields[1:])
		if err != nil {
			return nil, err
		}
	}

	// search for duplicates
	everything := k.PartitionKeys
	for _, ck := range k.ClusteringKeys {
		everything = append(everything, ck.Name)
	}
	seen := map[string]bool{}
	for v := range everything {
		if seen[everything[v]] {
			return nil, fmt.Errorf("Object %q has duplicate field %q in key struct tag", tableName, everything[v])
		}
		if everything[v] == "" {
			return nil, fmt.Errorf("Object %q has an empty primaryKey column", tableName)
		}

		seen[everything[v]] = true
	}

	return &k, nil
}

// TableFromInstance creates a dosa.Table from an instance
func TableFromInstance(object DomainObject) (*Table, error) {
	elem := reflect.TypeOf(object).Elem()
	name, err := NormalizeName(elem.Name())
	if err != nil {
		return nil, errors.Wrapf(err, "struct name is invalid")
	}

	t := &Table{
		StructName: elem.Name(),
		FieldNames: map[string]string{},
		EntityDefinition: EntityDefinition{
			Name:    name,
			Columns: []*ColumnDefinition{},
		},
	}
	for i := 0; i < elem.NumField(); i++ {
		structField := elem.Field(i)
		name := structField.Name
		if name == entityName {
			if t.Key, err = parseEntityTag(structField, t.StructName); err != nil {
				return nil, err
			}
		} else {
			cd, err := parseFieldTag(structField)
			if err != nil {
				return nil, errors.Wrapf(err, "Column %q had invalid type", name)
			}
			t.Columns = append(t.Columns, cd)
			t.FieldNames[cd.Name] = name
		}
	}

	// TODO: we are computing "everything" twice, maybe we can do it once?
	everything := t.Key.PartitionKeys
	for _, ck := range t.Key.ClusteringKeys {
		everything = append(everything, ck.Name)
	}
	for _, value := range everything {
		if _, ok := t.FieldNames[value]; !ok {
			return nil, fmt.Errorf("Object %q references non-existent primary key field %q",
				t.StructName, value)
		}
	}

	return t, nil
}

// parseEntityTag function parses DOSA tag on the "Entity" field
func parseEntityTag(structField reflect.StructField, structName string) (*PrimaryKey, error) {
	dosaAnnotation := structField.Tag.Get("dosa")
	if len(dosaAnnotation) == 0 {
		return nil, fmt.Errorf("dosa.Entity on object %s found without a dosa struct tag", structName)
	}
	attrs := strings.Split(dosaAnnotation, ",")
	var saved string
	var key *PrimaryKey
	for _, attr := range attrs {
		if saved != "" {
			attr = saved + "," + attr
			saved = ""
		}
		if strings.HasPrefix(attr, "primaryKey=") {
			// could hardcode the offset, but this is cleaner
			pkString := strings.SplitN(attr, "=", 2)[1]
			var err error
			// TODO: this could be better
			if strings.Count(pkString, "(") > strings.Count(pkString, ")") {
				saved = attr
				continue
			}

			key, err = parsePrimaryKey(structName, pkString)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, fmt.Errorf("Invalid annotation %q found on object %q", attr, structName)
		}
	}
	if saved != "" {
		return nil, fmt.Errorf("Object %q missing close parenthesis for primary key struct tag", structName)
	}

	return key, nil
}

// parseFieldTag function parses DOSA tag on the fields in the DOSA struct except the "Entity" field
func parseFieldTag(structField reflect.StructField) (*ColumnDefinition, error) {
	typ, err := typify(structField.Type)
	if err != nil {
		return nil, err
	}
	return &ColumnDefinition{Name: structField.Name, Type: typ}, nil
}

var (
	uuidType      = reflect.TypeOf(UUID(""))
	blobType      = reflect.TypeOf([]byte{})
	timestampType = reflect.TypeOf(time.Time{})
	int32Type     = reflect.TypeOf(int32(0))
	int64Type     = reflect.TypeOf(int64(0))
	doubleType    = reflect.TypeOf(float64(0.0))
	stringType    = reflect.TypeOf("")
	boolType      = reflect.TypeOf(true)
)

func typify(f reflect.Type) (Type, error) {
	switch f {
	case uuidType:
		return TUUID, nil
	case blobType:
		return Blob, nil
	case timestampType:
		return Timestamp, nil
	case int32Type:
		return Int32, nil
	case int64Type:
		return Int64, nil
	case doubleType:
		return Double, nil
	case stringType:
		return String, nil
	case boolType:
		return Bool, nil
	}

	return Invalid, fmt.Errorf("Invalid type %v", f)
}

func (d Table) String() string {
	// TODO: better output
	return d.Name
}
