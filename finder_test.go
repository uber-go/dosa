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
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnparseableGoCode(t *testing.T) {
	const tmpdir = ".testgen"
	defer os.RemoveAll(tmpdir)
	if err := os.Mkdir(tmpdir, 0770); err != nil {
		t.Fatalf("can't create %s: %s", tmpdir, err)
	}
	if err := ioutil.WriteFile(tmpdir+"/broken.go", []byte("package broken\nfunc broken\n"), 0644); err != nil {
		t.Fatalf("can't create %s/broken.go: %s", tmpdir, err)
	}
	entities, errs, err := findEntities([]string{tmpdir}, []string{})
	assert.Nil(t, entities)
	assert.Nil(t, errs)
	assert.Contains(t, err.Error(), "expected '('")
}

func TestNonExistentDirectory(t *testing.T) {
	const nonExistentDirectory = "ThisDirectoryBetterNotExist"
	entities, errs, err := findEntities([]string{nonExistentDirectory}, []string{})
	assert.Nil(t, entities)
	assert.Nil(t, errs)
	assert.Contains(t, err.Error(), nonExistentDirectory)
}

func TestParser(t *testing.T) {
	entities, errs, err := findEntities([]string{"."}, []string{})
	assert.NoError(t, err)
	expectedEntities := map[string]DomainObject{
		"singleprimarykeynoparen":       &SinglePrimaryKeyNoParen{},
		"singleprimarykey":              &SinglePrimaryKey{},
		"singlepartitionkey":            &SinglePartitionKey{},
		"primarykeywithsecondaryrange":  &PrimaryKeyWithSecondaryRange{},
		"primarykeywithdescendingrange": &PrimaryKeyWithDescendingRange{},
		"multicomponentprimarykey":      &MultiComponentPrimaryKey{},
		"nameinprimarykey":              &NameInPrimaryKey{},
		"noetltag":                      &NoETLTag{},
		"etltagoff":                     &ETLTagOff{},
		"etltagon":                      &ETLTagOn{},
		"etlinprimarykey":               &ETLInPrimaryKey{},
		"nullabletype":                  &NullableType{},
		"alltypes":                      &AllTypes{},
		"unexportedfieldtype":           &UnexportedFieldType{},
		"ignoretagtype":                 &IgnoreTagType{},
		"badcolnamebutrenamed":          &BadColNameButRenamed{},
		"singleindexnoparen":            &SingleIndexNoParen{},
		"multipleindexes":               &MultipleIndexes{},
		"complexindexes":                &ComplexIndexes{},
		"scopemetadata":                 &ScopeMetadata{},
		"indexeswithcolumnstag":         &IndexesWithColumnsTag{},
		"indexeswithdefuncttag":         &IndexesWithDefunctTag{},
	}
	entitiesExcludedForTest := map[string]interface{}{
		"clienttestentity1":      struct{}{}, // skip, see https://jira.uberinternal.com/browse/DOSA-788
		"clienttestentity2":      struct{}{}, // skip, same as above
		"registrytestvalid":      struct{}{}, // skip, same as above
		"allfieldtypes":          struct{}{},
		"alltypesscantestentity": struct{}{},
	}

	assert.Equal(t, len(expectedEntities)+len(entitiesExcludedForTest), len(entities), fmt.Sprintf("%s", entities))
	// TODO(jzhan): remove the hard-coded number of errors.
	assert.Equal(t, 24, len(errs), fmt.Sprintf("%v", errs))

	for _, entity := range entities {
		if _, ok := entitiesExcludedForTest[entity.Name]; ok {
			continue
		}
		inst, ok := expectedEntities[entity.Name]
		fmt.Printf("checking %s\n", entity.Name)
		assert.True(t, ok)
		e, err := TableFromInstance(inst)
		assert.NoError(t, err)
		assert.Equal(t, e, entity)
	}
}

func TestExclusion(t *testing.T) {
	entities, errs, err := findEntities([]string{"."}, []string{"*_test.go"})
	// We expect to find only ScopeMetadata
	assert.Equal(t, 1, len(entities))
	assert.Equal(t, "ScopeMetadata", entities[0].StructName)
	assert.Equal(t, 0, len(errs))
	assert.Nil(t, err)
}

func TestFindEntitiesInOtherPkg(t *testing.T) {
	entities, warnings, err := findEntities([]string{"testentity"}, []string{})
	assert.NoError(t, err)
	assert.Equal(t, 6, len(entities))
	assert.Empty(t, warnings)
}

func BenchmarkFinder(b *testing.B) {
	for i := 0; i < b.N; i++ {
		findEntities([]string{"."}, []string{})
	}
}

func TestFindEntityByName(t *testing.T) {
	validName := "SinglePrimaryKey"
	invalidName := "BadEntityName"

	// happy case
	entity, err := FindEntityByName(".", validName)
	assert.NotNil(t, entity)
	assert.NoError(t, err)
	assert.Equal(t, validName, entity.StructName)

	// bad case
	entity, err = FindEntityByName(".", invalidName)
	assert.Nil(t, entity)
	assert.Error(t, err)
}

func TestStringToDosaType(t *testing.T) {
	data := []struct {
		inType    string
		pkg       string
		expected  Type
		isPointer bool
	}{
		// Tests without package name
		{"string", "", String, false},
		{"[]byte", "", Blob, false},
		{"bool", "", Bool, false},
		{"int32", "", Int32, false},
		{"int64", "", Int64, false},
		{"float64", "", Double, false},
		{"time.Time", "", Timestamp, false},
		{"UUID", "", TUUID, false},

		{"*string", "", String, true},
		{"*bool", "", Bool, true},
		{"*int32", "", Int32, true},
		{"*int64", "", Int64, true},
		{"*float64", "", Double, true},
		{"*time.Time", "", Timestamp, true},
		{"*UUID", "", TUUID, true},

		// Tests with package name that doesn't end with dot.
		{"dosa.UUID", "dosa", TUUID, false},
		{"*dosa.UUID", "dosa", TUUID, true},

		// Tests with package name that ends with dot.
		{"dosav2.UUID", "dosav2.", TUUID, false},
		{"*dosav2.UUID", "dosav2.", TUUID, true},

		{"unknown", "", Invalid, false},
	}

	for _, tc := range data {
		actual, isPointer := stringToDosaType(tc.inType, tc.pkg)
		assert.Equal(t, isPointer, tc.isPointer)
		assert.Equal(t, tc.expected, actual,
			fmt.Sprintf("stringToDosaType(%q, %q) != %d -- actual: %d", tc.inType, tc.pkg, tc.expected, actual))
	}
}
