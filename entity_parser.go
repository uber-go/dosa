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
	"unicode"

	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
)

const (
	entityName = "Entity"
	indexName  = "Index"
	dosaTagKey = "dosa"
	asc        = "asc"
	desc       = "desc"
)

var (
	primaryKeyPattern0 = regexp.MustCompile(`primaryKey\s*=\s*([^=]*)((\s+.*=)|$)`)
	primaryKeyPattern1 = regexp.MustCompile(`\(\s*\((.*)\)(.*)\)`)
	primaryKeyPattern2 = regexp.MustCompile(`\(\s*([^,\s]+),?(.*)\)`)
	primaryKeyPattern3 = regexp.MustCompile(`^\s*([^(),\s]+)\s*$`)

	indexKeyPattern0 = regexp.MustCompile(`key\s*=\s*([^=]*)((\s+.*=)|$)`)

	namePattern0 = regexp.MustCompile(`name\s*=\s*(\S*)`)

	etlPattern0 = regexp.MustCompile(`etl\s*=\s*(\S*)`)

	ttlPattern0 = regexp.MustCompile(`ttl\s*=\s*(\S*)`)

	indexType = reflect.TypeOf((*Index)(nil)).Elem()
)

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

		clusteringKeys = append(clusteringKeys, &ClusteringKey{Name: strings.TrimSpace(fields[0]), Descending: descending})
	}
	return clusteringKeys, nil
}

// parsePartitionKey func parses the partition key of DOSA object
func parsePartitionKey(pkStr string) []string {
	pkStr = strings.TrimSpace(pkStr)
	var pks []string
	partitionKeys := strings.Split(pkStr, ",")
	for _, pk := range partitionKeys {
		npk := strings.TrimSpace(pk)
		if len(pk) > 0 {
			pks = append(pks, npk)
		}
	}
	return pks
}

// parsePrimaryKey func parses the primary key of DOSA object
func parsePrimaryKey(tableName, pkStr string) (*PrimaryKey, error) {
	// parens must be matched
	if !parensBalanced(pkStr) {
		return nil, fmt.Errorf("unmatched parentheses: %q", pkStr)
	}
	// filter out "trailing comma and space"
	pkStr = strings.TrimRight(pkStr, ", ")
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
	partitionKeys := parsePartitionKey(partitionKeyStr)
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

// TableFromInstance creates a dosa.Table from a dosa.DomainObject instance.
// Note: this method is not cheap as it does a lot of reflection to build the
// Table instances. It is recommended to only be called once and cache results.
func TableFromInstance(object DomainObject) (*Table, error) {
	elem := reflect.TypeOf(object).Elem()

	t := &Table{
		StructName: elem.Name(),
		ColToField: map[string]string{},
		FieldToCol: map[string]string{},
		EntityDefinition: EntityDefinition{
			Columns: []*ColumnDefinition{},
			Indexes: map[string]*IndexDefinition{},
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
			var err error
			if t.EntityDefinition.Name, t.TTL, t.ETL, t.Key, err = parseEntityTag(t.StructName, tag); err != nil {
				return nil, err
			}
		} else {
			// parse index fields
			if structField.Type == indexType {
				indexName, indexKey, err := parseIndexTag(structField.Name, tag)
				if err != nil {
					return nil, err
				}
				if _, exist := t.Indexes[indexName]; exist {
					return nil, errors.Errorf("index name is duplicated: %s", indexName)
				}
				t.Indexes[indexName] = &IndexDefinition{Key: indexKey}
			} else {
				cd, err := parseFieldTag(structField, tag)
				if err != nil {
					return nil, errors.Wrapf(err, "column %q had invalid type", name)
				}
				t.Columns = append(t.Columns, cd)
				t.ColToField[cd.Name] = name
				t.FieldToCol[name] = cd.Name
			}
		}
	}

	if t.Key == nil {
		return nil, errors.Errorf("cannot find dosa.Entity in object %s", t.StructName)
	}

	translateKeyName(t)

	if err := t.EnsureValid(); err != nil {
		return nil, errors.Wrap(err, "failed to parse dosa object")
	}

	return t, nil
}

// translateKeyName translate the primary keys to the internal column name based on the mapping
// between fields and columns.
func translateKeyName(t *Table) {
	pk := t.EntityDefinition.Key
	for i := range pk.PartitionKeys {
		name := pk.PartitionKeys[i]
		if v, ok := t.FieldToCol[name]; ok {
			pk.PartitionKeys[i] = v
		}
	}

	for i := range pk.ClusteringKeys {
		name := pk.ClusteringKeys[i].Name
		if v, ok := t.FieldToCol[name]; ok {
			pk.ClusteringKeys[i].Name = v
		}
	}

	for _, index := range t.Indexes {
		pk := index.Key
		for i := range pk.PartitionKeys {
			name := pk.PartitionKeys[i]
			if v, ok := t.FieldToCol[name]; ok {
				pk.PartitionKeys[i] = v
			}
		}

		for i := range pk.ClusteringKeys {
			name := pk.ClusteringKeys[i].Name
			if v, ok := t.FieldToCol[name]; ok {
				pk.ClusteringKeys[i].Name = v
			}
		}
	}
}

// parseIndexTag functions parses DOSA index tag
func parseIndexTag(indexName, dosaAnnotation string) (string, *PrimaryKey, error) {
	// index name struct must be exported in the entity,
	// otherwise it will be ignored when upserting the schema.
	if len(indexName) != 0 && unicode.IsLower([]rune(indexName)[0]) {
		expected := []rune(indexName)
		expected[0] = unicode.ToUpper(expected[0])
		return "", nil, fmt.Errorf("index name (%s) must be exported, "+
			"try (%s) instead", indexName, string(expected))
	}
	tag := dosaAnnotation
	// find the primaryKey
	matchs := indexKeyPattern0.FindStringSubmatch(tag)
	if len(matchs) != 4 {
		return "", nil, fmt.Errorf("dosa.Index %s with an invalid dosa index tag %q", indexName, tag)
	}
	pkString := matchs[1]
	key, err := parsePrimaryKey(indexName, pkString)
	if err != nil {
		return "", nil, errors.Wrapf(err, "struct %s has an invalid index key %q", indexName, pkString)
	}
	toRemove := strings.TrimSuffix(matchs[0], matchs[2])
	toRemove = strings.TrimSuffix(matchs[0], matchs[3])
	tag = strings.Replace(tag, toRemove, "", 1)

	//find the name
	fullNameTag, name, err := parseNameTag(tag, indexName)
	if err != nil {
		return "", nil, errors.Wrapf(err, "invalid name tag: %s", tag)
	}

	tag = strings.Replace(tag, fullNameTag, "", 1)
	tag = strings.TrimSpace(tag)
	if tag != "" {
		return "", nil, fmt.Errorf("index field %s with an invalid dosa index tag: %s", indexName, tag)
	}

	return name, key, nil
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
	name = strings.TrimRight(name, " ,")

	var err error
	name, err = NormalizeName(name)
	if err != nil {
		return "", "", err
	}

	return fullNameTag, name, nil
}

// parseETLTag functions parses DOSA "etl" tag
func parseETLTag(tag string) (string, ETLState, error) {
	fullETLTag := ""
	etlTag := ""
	matches := etlPattern0.FindStringSubmatch(tag)
	if len(matches) == 2 {
		fullETLTag = matches[0]
		etlTag = matches[1]
	}

	if len(matches) == 0 {
		return "", EtlOff, nil
	}

	// filter out "trailing comma"
	etlTag = strings.TrimRight(etlTag, " ,")

	var err error
	etlTag, err = NormalizeName(etlTag)
	if err != nil {
		return "", EtlOff, err
	}

	etlState, err := ToETLState(etlTag)
	if err != nil {
		return "", EtlOff, err
	}
	return fullETLTag, etlState, nil
}

// parseTTLTag functions parses DOSA "ttl" tag
func parseTTLTag(tag string) (string, time.Duration, error) {
	fullTTLTag := ""
	ttlTag := ""
	matches := ttlPattern0.FindStringSubmatch(tag)

	if len(matches) == 0 {
		return "", NoTTL(), nil
	}

	if len(matches) == 2 {
		fullTTLTag = matches[0]
		ttlTag = matches[1]
	}

	// filter out "trailing comma"
	ttlTag = strings.TrimRight(ttlTag, " ,")
	ttl, err := time.ParseDuration(ttlTag)
	if err != nil {
		return "", NoTTL(), err
	}

	if err = ValidateTTL(ttl); err != nil {
		return "", NoTTL(), err
	}

	return fullTTLTag, ttl, nil
}

// parseEntityTag function parses DOSA tag on the "Entity" field
func parseEntityTag(structName, dosaAnnotation string) (string, time.Duration, ETLState, *PrimaryKey, error) {
	tag := dosaAnnotation

	// find the primaryKey
	matchs := primaryKeyPattern0.FindStringSubmatch(tag)
	if len(matchs) != primaryKeyPattern0.NumSubexp()+1 {
		return "", NoTTL(), EtlOff, nil, fmt.Errorf("dosa.Entity on object %s with an invalid dosa struct tag %q", structName, tag)
	}
	pkString := matchs[1]

	key, err := parsePrimaryKey(structName, pkString)
	if err != nil {
		return "", NoTTL(), EtlOff, nil, errors.Wrapf(err, "struct %s has an invalid primary key %q", structName, pkString)
	}
	toRemove := strings.TrimSuffix(matchs[0], matchs[2])
	toRemove = strings.TrimSuffix(matchs[0], matchs[3])
	tag = strings.Replace(tag, toRemove, "", 1)

	// find the name
	fullNameTag, name, err := parseNameTag(tag, structName)
	if err != nil {
		return "", NoTTL(), EtlOff, nil, errors.Wrapf(err, "invalid name tag: %s", tag)
	}
	tag = strings.Replace(tag, fullNameTag, "", 1)

	// find the ETL flag
	fullETLTag, etlState, err := parseETLTag(tag)
	if err != nil {
		return "", NoTTL(), EtlOff, nil, errors.Wrapf(err, "invalid etl tag: %s", tag)
	}
	tag = strings.Replace(tag, fullETLTag, "", 1)

	// find the ttl flag
	fullTTLTag, ttl, err := parseTTLTag(tag)
	if err != nil {
		return "", NoTTL(), EtlOff, nil, errors.Wrapf(err, "invalid ttl tag: %s", tag)
	}
	tag = strings.Replace(tag, fullTTLTag, "", 1)

	tag = strings.TrimSpace(tag)
	if tag != "" {
		return "", NoTTL(), EtlOff, nil, fmt.Errorf("struct %s with an invalid dosa struct tag: %s", structName, tag)
	}

	return name, ttl, etlState, key, nil
}

// parseFieldTag function parses DOSA tag on the fields in the DOSA struct except the "Entity" field
func parseFieldTag(structField reflect.StructField, dosaAnnotation string) (*ColumnDefinition, error) {
	typ, isPointer, err := typify(structField.Type)
	if err != nil {
		return nil, err
	}
	return parseField(typ, isPointer, structField.Name, dosaAnnotation)
}

func parseField(typ Type, isPointer bool, name string, tag string) (*ColumnDefinition, error) {
	// parse name tag
	fullNameTag, name, err := parseNameTag(tag, name)
	if err != nil {
		return nil, fmt.Errorf("invalid name tag: %s", tag)
	}

	tag = strings.Replace(tag, fullNameTag, "", 1)
	if strings.TrimSpace(tag) != "" {
		return nil, fmt.Errorf("field %s with an invalid dosa field tag: %s", name, tag)
	}

	return &ColumnDefinition{Name: name, IsPointer: isPointer, Type: typ}, nil
}

func parensBalanced(s string) bool {
	// This is effectively pushing left parens on the stack, and popping them when
	// a right paren is seen. Since the stack only ever contains the same character,
	// we don't actually need the stack -- only its size.
	var ssize uint
	for i := 0; i < len(s); i++ {
		if s[i] == '(' {
			ssize++
		} else if s[i] == ')' {
			if ssize == 0 {
				// Extra right paren
				return false
			}
			ssize--
		}
	}
	// Stack must be empty
	return ssize == 0
}

var (
	uuidType       = reflect.TypeOf(uuid.UUID{})
	blobType       = reflect.TypeOf([]byte{})
	timestampType  = reflect.TypeOf(time.Time{})
	int32Type      = reflect.TypeOf(int32(0))
	int64Type      = reflect.TypeOf(int64(0))
	doubleType     = reflect.TypeOf(float64(0.0))
	stringType     = reflect.TypeOf("")
	boolType       = reflect.TypeOf(true)
	nullBoolType   = reflect.TypeOf((*bool)(nil))
	nullInt32Type  = reflect.TypeOf((*int32)(nil))
	nullInt64Type  = reflect.TypeOf((*int64)(nil))
	nullDoubleType = reflect.TypeOf((*float64)(nil))
	nullStringType = reflect.TypeOf((*string)(nil))
	nullUUIDType   = reflect.TypeOf((*uuid.UUID)(nil))
	nullTimeType   = reflect.TypeOf((*time.Time)(nil))
)

func typify(f reflect.Type) (Type, bool, error) {
	switch f {
	case uuidType:
		return TUUID, false, nil
	case blobType:
		return Blob, false, nil
	case timestampType:
		return Timestamp, false, nil
	case int32Type:
		return Int32, false, nil
	case int64Type:
		return Int64, false, nil
	case doubleType:
		return Double, false, nil
	case stringType:
		return String, false, nil
	case boolType:
		return Bool, false, nil
	case nullUUIDType:
		return TUUID, true, nil
	case nullTimeType:
		return Timestamp, true, nil
	case nullInt32Type:
		return Int32, true, nil
	case nullInt64Type:
		return Int64, true, nil
	case nullDoubleType:
		return Double, true, nil
	case nullStringType:
		return String, true, nil
	case nullBoolType:
		return Bool, true, nil
	}

	return Invalid, true, fmt.Errorf("Invalid type %v", f)
}

func (d Table) String() string {
	return d.Name + " " + d.Key.String()
}
