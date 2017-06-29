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
	entities, errs, err := FindEntities([]string{tmpdir}, []string{})
	assert.Nil(t, entities)
	assert.Nil(t, errs)
	assert.Contains(t, err.Error(), "expected '('")
}

func TestNonExistentDirectory(t *testing.T) {
	const nonExistentDirectory = "ThisDirectoryBetterNotExist"
	entities, errs, err := FindEntities([]string{nonExistentDirectory}, []string{})
	assert.Nil(t, entities)
	assert.Nil(t, errs)
	assert.Contains(t, err.Error(), nonExistentDirectory)
}

func TestParser(t *testing.T) {
	entities, errs, err := FindEntities([]string{"."}, []string{})

	assert.Equal(t, 18, len(entities), fmt.Sprintf("%s", entities))
	assert.Equal(t, 19, len(errs), fmt.Sprintf("%v", errs))
	assert.Nil(t, err)

	for _, entity := range entities {
		var e *Table
		switch entity.Name {
		case "singleprimarykeynoparen":
			e, _ = TableFromInstance(&SinglePrimaryKeyNoParen{})
		case "singleprimarykey":
			e, _ = TableFromInstance(&SinglePrimaryKey{})
		case "singlepartitionkey":
			e, _ = TableFromInstance(&SinglePartitionKey{})
		case "primarykeywithsecondaryrange":
			e, _ = TableFromInstance(&PrimaryKeyWithSecondaryRange{})
		case "primarykeywithdescendingrange":
			e, _ = TableFromInstance(&PrimaryKeyWithDescendingRange{})
		case "multicomponentprimarykey":
			e, _ = TableFromInstance(&MultiComponentPrimaryKey{})
		case "nullabletype":
			e, _ = TableFromInstance(&NullableType{})
		case "alltypes":
			e, _ = TableFromInstance(&AllTypes{})
		case "unexportedfieldtype":
			e, _ = TableFromInstance(&UnexportedFieldType{})
		case "ignoretagtype":
			e, _ = TableFromInstance(&IgnoreTagType{})
		case "badcolnamebutrenamed":
			e, _ = TableFromInstance(&BadColNameButRenamed{})
		case "clienttestentity1": // skip, see https://jira.uberinternal.com/browse/DOSA-788
			continue
		case "clienttestentity2": // skip, same as above
			continue
		case "registrytestvalid": // skip, same as above
			continue
		case "alltypesscantestentity": // skipping test entity defined in scan_test.go
			continue
		case "singleindexnoparen":
			e, _ = TableFromInstance(&SingleIndexNoParen{})
		case "multipleindexes":
			e, _ = TableFromInstance(&MultipleIndexes{})
		case "complexindexes":
			e, _ = TableFromInstance(&ComplexIndexes{})
		default:
			t.Errorf("entity %s not expected", entity.Name)
			continue
		}

		assert.Equal(t, e, entity)
	}
}

func TestExclusion(t *testing.T) {
	entities, errs, err := FindEntities([]string{"."}, []string{"*_test.go"})
	assert.Equal(t, 0, len(entities))
	assert.Equal(t, 0, len(errs))
	assert.Nil(t, err)
}

func TestFindEntitiesInOtherPkg(t *testing.T) {
	entities, warnings, err := FindEntities([]string{"testentity"}, []string{})
	assert.NoError(t, err)
	assert.Equal(t, 4, len(entities))
	assert.Empty(t, warnings)
}

func BenchmarkFinder(b *testing.B) {
	for i := 0; i < b.N; i++ {
		FindEntities([]string{"."}, []string{})
	}
}

func TestStringToDosaType(t *testing.T) {
	data := []struct {
		inType   string
		pkg      string
		expected Type
	}{
		// Tests without package name
		{"string", "", String},
		{"[]byte", "", Blob},
		{"bool", "", Bool},
		{"int32", "", Int32},
		{"int64", "", Int64},
		{"float64", "", Double},
		{"time.Time", "", Timestamp},
		{"UUID", "", TUUID},
		{"NullString", "", TNullString},
		{"NullInt64", "", TNullInt64},
		{"NullFloat64", "", TNullFloat64},
		{"NullBool", "", TNullBool},
		{"NullTime", "", TNullTime},
		{"NullUUID", "", TNullUUID},

		// Tests with package name that doesn't end with dot.
		{"dosa.UUID", "dosa", TUUID},
		{"dosa.NullString", "dosa", TNullString},
		{"dosa.NullInt64", "dosa", TNullInt64},
		{"dosa.NullFloat64", "dosa", TNullFloat64},
		{"dosa.NullBool", "dosa", TNullBool},
		{"dosa.NullTime", "dosa", TNullTime},
		{"dosa.NullUUID", "dosa", TNullUUID},

		// Tests with package name that ends with dot.
		{"dosav2.UUID", "dosav2.", TUUID},
		{"dosav2.NullString", "dosav2.", TNullString},
		{"dosav2.NullInt64", "dosav2.", TNullInt64},
		{"dosav2.NullFloat64", "dosav2.", TNullFloat64},
		{"dosav2.NullBool", "dosav2.", TNullBool},
		{"dosav2.NullTime", "dosav2.", TNullTime},
		{"dosav2.NullUUID", "dosav2.", TNullUUID},

		{"unknown", "", Invalid},
	}

	for _, tc := range data {
		actual := stringToDosaType(tc.inType, tc.pkg)
		assert.Equal(t, tc.expected, actual,
			fmt.Sprintf("stringToDosaType(%q, %q) != %d -- actual: %d", tc.inType, tc.pkg, tc.expected, actual))
	}
}
