package dosa

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParser(t *testing.T) {
	entities, err := FindEntities(".")
	assert := assert.New(t)

	assert.Equal(10, len(entities), fmt.Sprintf("%s", entities))
	assert.Equal(11, len(err), fmt.Sprintf("%s", err))

EntityList:
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
			continue EntityList
		}
		assert.Equal(e, &entity)
	}
}

func BenchmarkFinder(b *testing.B) {
	for i := 0; i < b.N; i++ {
		FindEntities(".")
	}
}
