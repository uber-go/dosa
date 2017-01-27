package dosa

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

func TestUnparseableGoCode(t *testing.T) {
	assert := assert.New(t)
	const tmpdir = ".testgen"
	defer os.RemoveAll(tmpdir)
	if err := os.Mkdir(tmpdir, 0770); err != nil {
		t.Fatalf("can't create %s: %s", tmpdir, err)
	}
	if err := ioutil.WriteFile(tmpdir+"/broken.go", []byte("package broken\nfunc broken\n"), 0644); err != nil {
		t.Fatalf("can't create %s/broken.go: %s", tmpdir, err)
	}
	entities, errs, err := FindEntities(tmpdir)
	assert.Nil(entities)
	assert.Nil(errs)
	assert.Contains(err.Error(), "expected '('")
}

func TestNonExistentDirectory(t *testing.T) {
	const nonExistentDirectory = "ThisDirectoryBetterNotExist"
	entities, errs, err := FindEntities(nonExistentDirectory)
	assert := assert.New(t)
	assert.Nil(entities)
	assert.Nil(errs)
	assert.Contains(err.Error(), nonExistentDirectory)
}

func TestParser(t *testing.T) {
	entities, errs, err := FindEntities(".")
	assert := assert.New(t)

	assert.Equal(10, len(entities), fmt.Sprintf("%s", entities))
	assert.Equal(11, len(errs), fmt.Sprintf("%s", err))
	assert.Nil(err)

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
		case "alltypes":
			e, _ = TableFromInstance(&AllTypes{})
		case "unexportedfieldtype":
			e, _ = TableFromInstance(&UnexportedFieldType{})
		case "ignoretagtype":
			e, _ = TableFromInstance(&IgnoreTagType{})
		case "badnamebutrenamed":
			e, _ = TableFromInstance(&BadNameButRenamed{})
		default:
			t.Errorf("entity %s not expected", entity.Name)
			continue
		}
		assert.Equal(e, entity)
	}
}

func BenchmarkFinder(b *testing.B) {
	for i := 0; i < b.N; i++ {
		FindEntities(".")
	}
}
