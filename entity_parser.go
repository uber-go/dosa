// Copyright (c) 2019 Uber Technologies, Inc.
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
	"unicode"

	"github.com/pkg/errors"
)

const (
	entityName = "Entity"
	indexName  = "Index"
	dosaTagKey = "dosa"
	asc        = "asc"
	desc       = "desc"
)

var (
	indexType = reflect.TypeOf((*Index)(nil)).Elem()
	tagsKey   = []string{"primaryKey", "key", "name", "columns", "etl", "ttl"}
)

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
				indexName, indexKey, indexColumns, err := parseIndexTag(structField.Name, tag)
				if err != nil {
					return nil, err
				}
				if _, exist := t.Indexes[indexName]; exist {
					return nil, errors.Errorf("index name is duplicated: %s", indexName)
				}
				t.Indexes[indexName] = &IndexDefinition{Key: indexKey, Columns: indexColumns}
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

// parseEntityTag function parses DOSA tag on the "Entity" field
func parseEntityTag(structName, dosaAnnotation string) (string, time.Duration, ETLState, *PrimaryKey, error) {
	tagsMap, err := getTags(dosaAnnotation)
	if err != nil {
		return "", NoTTL(), EtlOff, nil, errors.Wrapf(err, "dosa.Entity on object %s with an invalid annotation %q", structName, dosaAnnotation)
	}
	if len(tagsMap) == 0 && dosaAnnotation != "" {
		return "", NoTTL(), EtlOff, nil, errors.Wrapf(err, "dosa.Entity on object %s with an invalid annotation %q", structName, dosaAnnotation)
	}

	name, err := parseNameTag(structName, tagsMap)
	if err != nil {
		return "", NoTTL(), EtlOff, nil, errors.Wrapf(err, "struct %s has invalid name tag %q", structName, dosaAnnotation)
	}

	ttl, err := parseTTLTag(tagsMap)
	if err != nil {
		return "", NoTTL(), EtlOff, nil, errors.Wrapf(err, "struct %s has invalid ttl tag %q", structName, dosaAnnotation)
	}

	etlState, err := parseETLTag(tagsMap)
	if err != nil {
		return "", NoTTL(), EtlOff, nil, errors.Wrapf(err, "struct %s has invalid etl tag %q", structName, dosaAnnotation)
	}

	var primaryKey *PrimaryKey
	if val, ok := tagsMap["primaryKey"]; ok {
		primaryKey, err = parsePrimaryKey(val)
		if err != nil {
			return "", NoTTL(), EtlOff, nil, errors.Wrapf(err, "struct %s has invalid primaryKey tag %q", structName, dosaAnnotation)
		}
	}
	return name, ttl, etlState, primaryKey, nil
}

// parseIndexTag functions parses DOSA index tag
func parseIndexTag(indexName, dosaAnnotation string) (string, *PrimaryKey, []string, error) {
	tagsMap, err := getTags(dosaAnnotation)
	if err != nil {
		return "", nil, nil, errors.Wrapf(err, "dosa.Index on object %s with an invalid annotation %q", indexName, dosaAnnotation)
	}
	if len(tagsMap) == 0 && dosaAnnotation != "" {
		return "", nil, nil, errors.Wrapf(err, "dosa.Index on object %s with an invalid annotation %q", indexName, dosaAnnotation)
	}

	// index name struct must be exported in the entity,
	// otherwise it will be ignored when upserting the schema.
	if len(indexName) != 0 && unicode.IsLower([]rune(indexName)[0]) {
		expected := []rune(indexName)
		expected[0] = unicode.ToUpper(expected[0])
		return "", nil, nil, fmt.Errorf("index name (%s) must be exported, try (%s) instead", indexName, string(expected))
	}

	name, err := parseNameTag(indexName, tagsMap)
	if err != nil {
		return "", nil, nil, errors.Wrapf(err, "index %s has invalid name tag %q", indexName, dosaAnnotation)
	}

	columns, err := parseColumnsTag(tagsMap)
	if err != nil {
		return "", nil, nil, errors.Wrapf(err, "index %s has invalid columns tag %q", indexName, dosaAnnotation)
	}

	var key *PrimaryKey
	if val, ok := tagsMap["key"]; ok {
		key, err = parsePrimaryKey(val)
		if err != nil {
			return "", nil, nil, errors.Wrapf(err, "index %s has invalid key tag %q", indexName, dosaAnnotation)
		}
	}

	return name, key, columns, nil
}

// parseSinglePrimaryKey func parses the single parimary key of DOSA object
func parseSinglePrimaryKey(s string) (*PrimaryKey, error) {
	s = strings.TrimSpace(s)
	spaceIdx := strings.Index(s, " ")
	if spaceIdx != -1 {
		return nil, fmt.Errorf("invalid partition key: %s", s)
	}
	commaIdx := strings.Index(s, ",")
	if commaIdx != -1 {
		return nil, fmt.Errorf("invalid partition key: %s", s)
	}
	return &PrimaryKey{
		PartitionKeys: []string{s},
	}, nil
}

// parseCompoundPartitionKeys func parses the compund partition key of DOSA object
func parseCompoundPartitionKeys(s string) ([]string, error) {
	s = strings.TrimSpace(strings.Trim(s, "()"))
	fields := strings.Split(s, ",")
	pks := []string{}
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			return nil, fmt.Errorf("invalid partition key %s", s)
		}
		pks = append(pks, field)
	}
	return pks, nil
}

// parseClusteringKeys func parses the clustering key of DOSA object
func parseClusteringKeys(s string) ([]*ClusteringKey, error) {
	s = strings.TrimSpace(strings.Trim(s, "()"))
	fields := strings.Split(s, ",")
	ckFields := []string{}
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			return nil, fmt.Errorf("invalid clustering key %s", s)
		}
		ckFields = append(ckFields, field)
	}

	cks := []*ClusteringKey{}
	for _, ckField := range ckFields {
		fields := strings.Fields(ckField)
		if len(fields) == 0 {
			continue
		}
		if len(fields) > 2 {
			return nil, fmt.Errorf("Clustering key definition %q should look like \"name[ asc/desc]\"",
				ckField)
		}
		descending := false
		if len(fields) == 2 {
			switch strings.ToLower(fields[1]) {
			case "desc":
				descending = true
			case "asc":
				descending = false
			default:
				return nil, fmt.Errorf("invalid clustering key order %q in %q", fields[1], ckField)
			}
		}

		name := strings.TrimSpace(fields[0])
		cks = append(cks, &ClusteringKey{Name: name, Descending: descending})
	}

	return cks, nil
}

// parsePrimaryKey func parses the primary key of DOSA object
func parsePrimaryKey(s string) (*PrimaryKey, error) {
	annotation := s
	// single partition key, e.g. key=UUID
	if s[0] != '(' {
		return parseSinglePrimaryKey(s)
	}

	// Trim outermost parenthesis
	if s[0] == '(' && s[len(s)-1] == ')' {
		s = strings.TrimSpace(s[1 : len(s)-1])
	} else {
		return nil, fmt.Errorf("invalid primary key: %s", annotation)
	}
	if s == "" {
		return nil, fmt.Errorf("invalid primary key: %s", annotation)
	}
	// single partition key, e.g. key=(UUID) key=(UUID1, UUID2, UUID3)
	if s[0] != '(' {
		commaPos := strings.Index(s, ",")
		if commaPos == -1 {
			return parseSinglePrimaryKey(s)
		}
		pkFields := []string{strings.TrimSpace(s[:commaPos])}
		cks, err := parseClusteringKeys(strings.TrimSpace(s[commaPos+1:]))
		if err != nil {
			return nil, errors.Wrapf(err, "invalid primary key: %s", annotation)
		}
		return &PrimaryKey{
			PartitionKeys:  pkFields,
			ClusteringKeys: cks,
		}, nil
	}

	// find partition keys
	i := skipParens(0, s)
	pkFields, err := parseCompoundPartitionKeys(s[:i+1])
	if err != nil {
		return nil, errors.Wrapf(err, "invalid primary key: %s", annotation)
	}
	if i+1 == len(s) {
		return &PrimaryKey{
			PartitionKeys: pkFields,
		}, nil
	}
	if i+1 < len(s) && s[i+1] != ',' {
		return nil, fmt.Errorf("invalid primary key: %s", annotation)
	}

	// find clustering keys
	s = strings.TrimSpace(s[i+2:])
	cks, err := parseClusteringKeys(strings.TrimSpace(s))
	if err != nil {
		if err != nil {
			return nil, errors.Wrapf(err, "invalid primary key: %s", annotation)
		}
	}

	// check duplicate fields
	allFields := pkFields
	for _, ck := range cks {
		allFields = append(allFields, ck.Name)
	}
	seen := map[string]bool{}
	for _, field := range allFields {
		if _, ok := seen[field]; ok {
			return nil, fmt.Errorf("duplicate field %q in key tag", field)
		}
		seen[field] = true
	}

	return &PrimaryKey{
		PartitionKeys:  pkFields,
		ClusteringKeys: cks,
	}, nil
}

// parseNameTag functions parses DOSA "name" tag
func parseNameTag(defaultName string, tagsMap map[string]string) (string, error) {
	name := defaultName
	if val, ok := tagsMap["name"]; ok {
		name = val
	}

	name, err := NormalizeName(name)
	if err != nil {
		return "", err
	}
	return name, nil
}

// parseColumnsTag parses the "columns" tag of a dosa.Index in the entity. It returns
// the matched section of the tag string and a list of the selected fields.
func parseColumnsTag(tagsMap map[string]string) ([]string, error) {
	if _, ok := tagsMap["columns"]; !ok {
		return nil, nil
	}
	s := strings.TrimSpace(tagsMap["columns"])
	if s[0] != '(' || s[len(s)-1] != ')' {
		return nil, fmt.Errorf("invalid columns tag %s", s)
	}
	s = s[1 : len(s)-1]
	fields := strings.Split(s, ",")
	columns := []string{}
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			return nil, fmt.Errorf("invalid columns tag %s", s)
		}
		field, err := NormalizeName(field)
		if err != nil {
			return nil, err
		}
		columns = append(columns, field)
	}
	// check duplicate fields in columns
	seen := map[string]bool{}
	for _, c := range columns {
		if _, ok := seen[c]; ok {
			return nil, fmt.Errorf("duplicate field %s in columns tag", c)
		}
		seen[c] = true
	}

	return columns, nil
}

// parseTTLTag functions parses DOSA "ttl" tag
func parseTTLTag(tagsMap map[string]string) (time.Duration, error) {
	if _, ok := tagsMap["ttl"]; !ok {
		return NoTTL(), nil
	}
	ttl, err := time.ParseDuration(tagsMap["ttl"])
	if err != nil {
		return NoTTL(), err
	}
	if err = ValidateTTL(ttl); err != nil {
		return NoTTL(), err
	}
	return ttl, nil
}

// parseETLTag functions parses DOSA "etl" tag
func parseETLTag(tagsMap map[string]string) (ETLState, error) {
	if _, ok := tagsMap["etl"]; !ok {
		return EtlOff, nil
	}
	etlTag, err := NormalizeName(tagsMap["etl"])
	if err != nil {
		return EtlOff, err
	}
	etlState, err := ToETLState(etlTag)
	if err != nil {
		return EtlOff, err
	}
	return etlState, nil
}

// getTags get tags splited and return map of the key=val pairs
func getTags(s string) (map[string]string, error) {
	s = strings.TrimSpace(s)
	if !parensBalanced(s) {
		return nil, fmt.Errorf("unmatched parentheses:%q", s)
	}

	tagsMap := make(map[string]string)

	i := 0
	start := 0
	for ; i < len(s); i++ {
		if s[i] == '(' {
			i = skipParens(i, s)
			if i+1 != len(s) && s[i+1] != ',' {
				return nil, fmt.Errorf("illegal delimiter %c in %s", s[i+1], s)
			}
		}
		if s[i] == ',' || i+1 == len(s) {
			tag := s[start:i]
			if i+1 == len(s) {
				tag = s[start:]
			}
			tag = strings.TrimSpace(tag)
			validTag := false

			for _, key := range tagsKey {
				if strings.HasPrefix(tag, key) {
					if _, exist := tagsMap[key]; exist {
						return nil, fmt.Errorf("duplicate tag %q in %s", key, s)
					}
					idx := len(key)
					for ; idx < len(tag); idx++ {
						if tag[idx] != ' ' && tag[idx] != '=' {
							break
						}
					}
					val := tag[idx:]
					if val == "" {
						return nil, fmt.Errorf("invalid %s tag", key)
					}
					tagsMap[key] = val
					validTag = true
					break
				}
			}
			// report that this tag has no prefix in tagsKey
			if !validTag {
				return nil, fmt.Errorf("invalid dosa annotation %s", tag)
			}
			start = i + 1
		}
	}

	return tagsMap, nil
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
	tagsMap, err := getTags(tag)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid dosa field tag: %s", tag)
	}
	if len(tagsMap) == 0 && tag != "" {
		return nil, fmt.Errorf("invalid dosa field tag: %s", tag)
	}

	name, err = parseNameTag(name, tagsMap)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid dosa field tag: %s", tag)
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

// skipParens start from i to the matching close parenthesis position
func skipParens(i int, s string) int {
	var count uint
	for ; i < len(s); i++ {
		if s[i] == '(' {
			count++
		} else if s[i] == ')' {
			count--
		}
		if count == 0 {
			break
		}
	}
	return i
}

var (
	uuidType       = reflect.TypeOf(UUID(""))
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
	nullUUIDType   = reflect.TypeOf((*UUID)(nil))
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
