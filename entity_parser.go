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
	"regexp"
	"strings"

	"time"

	"github.com/pkg/errors"
)

const (
	entityName = "Entity"
	dosaTagKey = "dosa"
	asc        = "asc"
	desc       = "desc"
)

var (
	primaryKeyPattern0 = regexp.MustCompile(`primaryKey\s*=\s*([^=]*)((\s+.*=)|$)`)
	primaryKeyPattern1 = regexp.MustCompile(`\(\s*\((.*)\)(.*)\)`)
	primaryKeyPattern2 = regexp.MustCompile(`\(\s*(\w*),?(.*)\)`)
	primaryKeyPattern3 = regexp.MustCompile(`^\s*(\w*)\s*$`)

	namePattern0 = regexp.MustCompile(`name\s*=\s*(\S*)`)
)

// removeTrailing func will removes all the trailing strings that match to the input.
// e.g. if the input is ' ', ',' , the method will remove all the ' ' and ',' at the
// right side of the string until it reaches to the first character not matches to the inputs.
func removeTrailingCharacters(s string, seps ...byte) string {
	set := make(map[byte]bool, len(seps))

	for _, sep := range seps {
		set[sep] = true
	}

	var i int
	for i = len(s) - 1; i >= 0; i-- {
		if !set[s[i]] {
			break
		}
	}
	return s[:i+1]
}

// parseClusteringKeys func parses the clustering key of DOSA object
func parseClusteringKeys(ckStr string) ([]*ClusteringKey, error) {
	ckStr = strings.TrimSpace(ckStr)
	cks := strings.Split(ckStr, ",")
	var clusteringKeys []*ClusteringKey
	for _, ck := range cks {
		fields := strings.Fields(ck)
		if len(fields) == 0 {
			continue
		}
		if len(fields) > 2 {
			return nil, fmt.Errorf("Clustering key definition %q should look like \"name[ asc/desc]\"",
				ck)
		}
		descending := false
		if len(fields) == 2 {
			switch strings.ToLower(fields[1]) {
			case desc:
				descending = true
			case asc:
				descending = false
			default:
				return nil, fmt.Errorf("invalid clustering key order %q in %q", fields[1], ck)
			}

		}

		name, err := NormalizeName(fields[0])
		if err != nil {
			return nil, err
		}
		clusteringKeys = append(clusteringKeys, &ClusteringKey{Name: name, Descending: descending})
	}
	return clusteringKeys, nil
}

// parsePartitionKey func parses the partition key of DOSA object
func parsePartitionKey(pkStr string) ([]string, error) {
	pkStr = strings.TrimSpace(pkStr)
	var pks []string
	var npk string
	var err error
	partitionKeys := strings.Split(pkStr, ",")
	for _, pk := range partitionKeys {
		if npk, err = NormalizeName(pk); err != nil {
			return nil, err
		}
		pks = append(pks, npk)
	}
	return pks, err
}

// parsePrimaryKey func parses the primary key of DOSA object
func parsePrimaryKey(tableName string, pkStr string) (*PrimaryKey, error) {
	// filter out "trailing comma"
	pkStr = removeTrailingCharacters(pkStr, ',', ' ')
	pkStr = strings.TrimSpace(pkStr)

	var partitionKeyStr string
	var clusteringKeyStr string

	matched := false
	// case 1: primaryKey=((PK1,PK2), PK3, PK4)
	matchs := primaryKeyPattern1.FindStringSubmatch(pkStr)
	if len(matchs) == 3 {
		matched = true
		partitionKeyStr = matchs[1]
		clusteringKeyStr = matchs[2]
	}

	// case 2: primaryKey=(PK1,PK2)
	if !matched {
		matchs = primaryKeyPattern2.FindStringSubmatch(pkStr)
		if len(matchs) == 3 {
			matched = true
			partitionKeyStr = matchs[1]
			clusteringKeyStr = matchs[2]
		}
	}

	// case 3: primaryKey=PK1 (only one primary key)
	if !matched {
		matchs = primaryKeyPattern3.FindStringSubmatch(pkStr)
		if len(matchs) == 2 {
			matched = true
			partitionKeyStr = matchs[1]
			clusteringKeyStr = ""
		}
	}

	if !matched {
		return nil, fmt.Errorf("invalid primary key: %s", pkStr)
	}

	var err error
	partitionKeys, err := parsePartitionKey(partitionKeyStr)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid primary key: %s", pkStr)
	}

	clusteringKeys, err := parseClusteringKeys(clusteringKeyStr)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid primary key: %s", pkStr)
	}

	// TODO optimize this , this is too slow
	// search for duplicates
	everything := partitionKeys
	for _, ck := range clusteringKeys {
		everything = append(everything, ck.Name)
	}
	seen := map[string]bool{}
	for v := range everything {
		if _, ok := seen[everything[v]]; ok {
			return nil, fmt.Errorf("Object %q has duplicate field %q in key struct tag", tableName, everything[v])
		}

		seen[everything[v]] = true
	}

	return &PrimaryKey{
		PartitionKeys:  partitionKeys,
		ClusteringKeys: clusteringKeys,
	}, nil
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
		ColToField: map[string]string{},
		FieldToCol: map[string]string{},
		EntityDefinition: EntityDefinition{
			Name:    name,
			Columns: []*ColumnDefinition{},
		},
	}
	for i := 0; i < elem.NumField(); i++ {
		structField := elem.Field(i)
		if len(structField.PkgPath) > 0 { // skip unexported fields
			continue
		}
		tag := strings.TrimSpace(structField.Tag.Get(dosaTagKey))
		if tag == "-" { // skip explicitly ignored fields
			continue
		}
		name := structField.Name
		if name == entityName {
			if t.EntityDefinition.Name, t.Key, err = parseEntityTag(t.StructName, tag); err != nil {
				return nil, err
			}
		} else {
			cd, err := parseFieldTag(structField, tag)
			if err != nil {
				return nil, errors.Wrapf(err, "column %q had invalid type", name)
			}
			name, _ := NormalizeName(name)
			t.Columns = append(t.Columns, cd)
			t.ColToField[cd.Name] = name
			t.FieldToCol[name] = cd.Name
		}
	}

	primaryKeyNameMatch(t)

	if err := t.EntityDefinition.EnsureValid(); err != nil {
		return nil, errors.Wrap(err, "failed to parse dosa object")
	}

	return t, nil
}

// primaryKeyNameMatch translate the primary keys to the internal column name based on the maping
// between fields and columns.
func primaryKeyNameMatch(t *Table) {
	pk := t.EntityDefinition.Key
	for i := range pk.PartitionKeys {
		if v, ok := t.FieldToCol[pk.PartitionKeys[i]]; ok {
			pk.PartitionKeys[i] = v
		}
	}

	for i := range pk.ClusteringKeys {
		if v, ok := t.FieldToCol[pk.ClusteringKeys[i].Name]; ok {
			pk.ClusteringKeys[i].Name = v
		}
	}
}

// parseNameTag functions parses DOSA "name" tag
func parseNameTag(tag, defaultName string) (string, string, error) {
	fullNameTag := ""
	name := defaultName

	matches := namePattern0.FindStringSubmatch(tag)
	if len(matches) == 2 {
		fullNameTag = matches[0]
		name = matches[1]
	}

	// filter out "trailing comma"
	name = removeTrailingCharacters(name, ',', ' ')

	var err error
	name, err = NormalizeName(name)
	if err != nil {
		return "", "", err
	}

	return fullNameTag, name, nil
}

// parseEntityTag function parses DOSA tag on the "Entity" field
func parseEntityTag(structName, dosaAnnotation string) (string, *PrimaryKey, error) {
	tag := dosaAnnotation
	// find the primaryKey
	matchs := primaryKeyPattern0.FindStringSubmatch(tag)
	if len(matchs) != 4 {
		return "", nil, fmt.Errorf("dosa.Entity on object %s with an invalid dosa struct tag", structName)
	}
	pkString := matchs[1]
	key, err := parsePrimaryKey(structName, pkString)
	if err != nil {
		return "", nil, errors.Wrapf(err, "failed to parse primary key %s for DOSA object", pkString)
	}

	toRemove := strings.TrimSuffix(matchs[0], matchs[2])
	toRemove = strings.TrimSuffix(matchs[0], matchs[3])
	tag = strings.Replace(tag, toRemove, "", 1)
	//find the name
	fullNameTag, name, err := parseNameTag(tag, structName)
	if err != nil {
		return "", nil, errors.Wrapf(err, "invalid name tag: %s", tag)
	}

	tag = strings.Replace(tag, fullNameTag, "", 1)
	if strings.TrimSpace(tag) != "" {
		return "", nil, fmt.Errorf("struct %s with an invalid dosa struct tag: %s", structName, tag)
	}

	return name, key, nil
}

// parseFieldTag function parses DOSA tag on the fields in the DOSA struct except the "Entity" field
func parseFieldTag(structField reflect.StructField, dosaAnnotation string) (*ColumnDefinition, error) {
	typ, err := typify(structField.Type)
	if err != nil {
		return nil, err
	}

	tag := dosaAnnotation
	// parse name tag
	fullNameTag, name, err := parseNameTag(tag, structField.Name)
	if err != nil {
		return nil, fmt.Errorf("invalid name tag: %s", tag)
	}

	tag = strings.Replace(tag, fullNameTag, "", 1)
	if strings.TrimSpace(tag) != "" {
		return nil, fmt.Errorf("field %s with an invalid dosa field tag: %s", structField.Name, tag)
	}

	return &ColumnDefinition{Name: name, Type: typ}, nil
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
