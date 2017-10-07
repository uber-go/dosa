package schemaadapter

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/dosa"
	"github.com/uber-go/dosa/connectors/base"
	"github.com/uber-go/dosa/mocks"
	"golang.org/x/net/context"
)

var (
	testEiName     = "testing"
	testKeyValueEi = &dosa.EntityInfo{
		Def: &dosa.EntityDefinition{
			Name: testEiName,
			Key: &dosa.PrimaryKey{
				PartitionKeys: []string{"key"},
			},
			Columns: []*dosa.ColumnDefinition{
				{Name: "value", Type: dosa.Blob},
				{Name: "key", Type: dosa.Blob},
			},
		},
	}
	testEi = &dosa.EntityInfo{
		Def: &dosa.EntityDefinition{
			Name: testEiName,
			Key: &dosa.PrimaryKey{
				PartitionKeys: []string{"u", "b"},
			},
			Columns: []*dosa.ColumnDefinition{
				{Name: "u", Type: dosa.Blob},
				{Name: "b", Type: dosa.Blob},
				{Name: "e", Type: dosa.Blob},
				{Name: "r", Type: dosa.Blob},
			},
		},
	}
)

func TestNewConnector(t *testing.T) {
	c := NewConnector(&base.Connector{})
	assert.NotNil(t, c)
}

func TestUpsert(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockDownstreamConnector := mocks.NewMockConnector(ctrl)
	baseConnector := base.NewConnector(mockDownstreamConnector)
	schemaConnector := NewConnector(baseConnector)

	values := map[string]dosa.FieldValue{
		"u": 1,
		"b": 3.0,
		"e": map[string][]string{"b": []string{"e", "r"}},
		"r": "2",
	}
	expectedValues := map[string]dosa.FieldValue{
		"key": []byte("{\"b\":3,\"u\":1}"),
		// Should be serialized alphabetically
		"value": []byte("{\"b\":3,\"e\":{\"b\":[\"e\",\"r\"]},\"r\":\"2\",\"u\":1}"),
	}

	mockDownstreamConnector.EXPECT().
		Upsert(context.TODO(), testKeyValueEi, expectedValues).
		Return(nil).
		Times(1)
	err := schemaConnector.Upsert(context.TODO(), testEi, values)
	assert.NoError(t, err)
}

func TestUpsertError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockDownstreamConnector := mocks.NewMockConnector(ctrl)
	baseConnector := base.NewConnector(mockDownstreamConnector)
	schemaConnector := NewConnector(baseConnector)

	values := map[string]dosa.FieldValue{
		"u": 1,
		"b": 3.0,
		"e": map[string][]string{"b": []string{"e", "r"}},
		"r": "2",
	}
	expectedValues := map[string]dosa.FieldValue{
		"key": []byte("{\"b\":3,\"u\":1}"),
		// Should be serialized alphabetically
		"value": []byte("{\"b\":3,\"e\":{\"b\":[\"e\",\"r\"]},\"r\":\"2\",\"u\":1}"),
	}

	mockDownstreamConnector.EXPECT().
		Upsert(context.TODO(), testKeyValueEi, expectedValues).
		Return(assert.AnError).
		Times(1)
	err := schemaConnector.Upsert(context.TODO(), testEi, values)
	assert.Error(t, err)

}
func TestRemove(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockDownstreamConnector := mocks.NewMockConnector(ctrl)
	baseConnector := base.NewConnector(mockDownstreamConnector)
	schemaConnector := NewConnector(baseConnector)

	values := map[string]dosa.FieldValue{
		"c": 3.0,
		"d": map[string][]string{"e": []string{"f", "g"}},
	}
	expectedValues := map[string]dosa.FieldValue{
		"key": []byte("{\"c\":3,\"d\":{\"e\":[\"f\",\"g\"]}}"),
	}

	mockDownstreamConnector.EXPECT().
		Remove(context.TODO(), testKeyValueEi, expectedValues).
		Return(nil).
		Times(1)
	err := schemaConnector.Remove(context.TODO(), testEi, values)
	assert.NoError(t, err)
}

func TestRemoveError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockDownstreamConnector := mocks.NewMockConnector(ctrl)
	baseConnector := base.NewConnector(mockDownstreamConnector)
	schemaConnector := NewConnector(baseConnector)

	values := map[string]dosa.FieldValue{
		"c": 3.0,
		"d": map[string][]string{"e": []string{"f", "g"}},
	}
	expectedValues := map[string]dosa.FieldValue{
		"key": []byte("{\"c\":3,\"d\":{\"e\":[\"f\",\"g\"]}}"),
	}

	mockDownstreamConnector.EXPECT().
		Remove(context.TODO(), testKeyValueEi, expectedValues).
		Return(assert.AnError).
		Times(1)
	err := schemaConnector.Remove(context.TODO(), testEi, values)
	assert.Error(t, err)
}

func TestRange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockDownstreamConnector := mocks.NewMockConnector(ctrl)
	baseConnector := base.NewConnector(mockDownstreamConnector)
	schemaConnector := NewConnector(baseConnector)

	conditions := map[string][]*dosa.Condition{
		"b": []*dosa.Condition{{dosa.LtOrEq, "uber"}, {dosa.Eq, []byte("uber")}},
		"e": []*dosa.Condition{},
	}
	minFields := []string{"u", "b", "e", "r"}
	token := "uber"
	limit := 54321

	expectedValues := map[string]dosa.FieldValue{
		"key": []byte("{\"conditions\":{\"b\":[{\"Op\":3,\"Value\":\"uber\"},{\"Op\":1,\"Value\":\"dWJlcg==\"}],\"e\":[]},\"limit\":54321,\"token\":\"uber\"}"),
	}

	readResponse := map[string]dosa.FieldValue{
		"value": []byte("{\"rows\":[{\"r\":11},{\"e\":22},{\"b\":33,\"u\":44}],\"tokenNext\":\"uberr\"}"),
	}
	mockDownstreamConnector.EXPECT().
		Read(
			context.TODO(),
			testKeyValueEi,
			expectedValues,
			minFields).
		Return(readResponse, nil).
		Times(1)

	row1 := map[string]dosa.FieldValue{"r": float64(11)}
	row2 := map[string]dosa.FieldValue{"e": float64(22)}
	row3 := map[string]dosa.FieldValue{"b": float64(33), "u": float64(44)}

	rows, tn, err := schemaConnector.Range(context.TODO(), testEi, conditions, minFields, token, limit)
	assert.NoError(t, err)
	assert.Equal(t, "uberr", tn)
	assert.Equal(t, []map[string]dosa.FieldValue{row1, row2, row3}, rows)
}

func TestRangeError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockDownstreamConnector := mocks.NewMockConnector(ctrl)
	baseConnector := base.NewConnector(mockDownstreamConnector)
	schemaConnector := NewConnector(baseConnector)

	conditions := map[string][]*dosa.Condition{
		"b": []*dosa.Condition{{dosa.LtOrEq, "uber"}, {dosa.Eq, []byte("uber")}},
		"e": []*dosa.Condition{},
	}
	minFields := []string{"u", "b", "e", "r"}
	token := "uber"
	limit := 54321

	expectedValues := map[string]dosa.FieldValue{
		"key": []byte("{\"conditions\":{\"b\":[{\"Op\":3,\"Value\":\"uber\"},{\"Op\":1,\"Value\":\"dWJlcg==\"}],\"e\":[]},\"limit\":54321,\"token\":\"uber\"}"),
	}

	mockDownstreamConnector.EXPECT().
		Read(
			context.TODO(),
			testKeyValueEi,
			expectedValues,
			minFields).
		Return(nil, assert.AnError).
		Times(1)

	_, _, err := schemaConnector.Range(context.TODO(), testEi, conditions, minFields, token, limit)
	assert.Error(t, err)
}

func TestScan(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockDownstreamConnector := mocks.NewMockConnector(ctrl)
	baseConnector := base.NewConnector(mockDownstreamConnector)
	schemaConnector := NewConnector(baseConnector)

	minFields := []string{"u", "b", "e", "r"}
	token := "uber"
	limit := 54321

	expectedValues := map[string]dosa.FieldValue{
		"key": []byte("{\"limit\":54321,\"token\":\"uber\"}"),
	}

	readResponse := map[string]dosa.FieldValue{
		"value": []byte("{\"rows\":[{\"r\":11},{\"e\":22},{\"b\":33,\"u\":44}],\"tokenNext\":\"uberr\"}"),
	}
	mockDownstreamConnector.EXPECT().
		Read(
			context.TODO(),
			testKeyValueEi,
			expectedValues,
			minFields).
		Return(readResponse, nil).
		Times(1)

	row1 := map[string]dosa.FieldValue{"r": float64(11)}
	row2 := map[string]dosa.FieldValue{"e": float64(22)}
	row3 := map[string]dosa.FieldValue{"b": float64(33), "u": float64(44)}

	rows, tn, err := schemaConnector.Scan(context.TODO(), testEi, minFields, token, limit)
	assert.NoError(t, err)
	assert.Equal(t, "uberr", tn)
	assert.Equal(t, []map[string]dosa.FieldValue{row1, row2, row3}, rows)

}

func TestScanError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockDownstreamConnector := mocks.NewMockConnector(ctrl)
	baseConnector := base.NewConnector(mockDownstreamConnector)
	schemaConnector := NewConnector(baseConnector)

	minFields := []string{"u", "b", "e", "r"}
	token := "uber"
	limit := 54321

	expectedValues := map[string]dosa.FieldValue{
		"key": []byte("{\"limit\":54321,\"token\":\"uber\"}"),
	}

	mockDownstreamConnector.EXPECT().
		Read(
			context.TODO(),
			testKeyValueEi,
			expectedValues,
			minFields).
		Return(nil, assert.AnError).
		Times(1)

	_, _, err := schemaConnector.Scan(context.TODO(), testEi, minFields, token, limit)
	assert.Error(t, err)

}
func TestRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockDownstreamConnector := mocks.NewMockConnector(ctrl)
	baseConnector := base.NewConnector(mockDownstreamConnector)
	schemaConnector := NewConnector(baseConnector)

	values := map[string]dosa.FieldValue{
		"u": 1,
		"b": 3.0,
	}
	minFields := []string{"u", "b", "e", "r"}

	expectedValues := map[string]dosa.FieldValue{
		"key": []byte("{\"b\":3,\"u\":1}"),
	}

	readResponse := map[string]dosa.FieldValue{
		"value": []byte("{\"b\":3,\"e\":{\"b\":[\"e\",\"r\"]},\"r\":\"2\",\"u\":1}"),
	}

	mockDownstreamConnector.EXPECT().
		Read(context.TODO(), testKeyValueEi, expectedValues, minFields).
		Return(readResponse, nil).
		Times(1)
	result, err := schemaConnector.Read(context.TODO(), testEi, values, minFields)
	assert.NoError(t, err)
	assert.Equal(t, map[string]dosa.FieldValue{
		"u": float64(1),
		"b": 3.0,
		"e": map[string]interface{}{"b": []interface{}{"e", "r"}},
		"r": "2",
	}, result)
}

func TestReadError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockDownstreamConnector := mocks.NewMockConnector(ctrl)
	baseConnector := base.NewConnector(mockDownstreamConnector)
	schemaConnector := NewConnector(baseConnector)

	values := map[string]dosa.FieldValue{
		"u": 1,
		"b": 3.0,
	}
	minFields := []string{"u", "b", "e", "r"}

	expectedValues := map[string]dosa.FieldValue{
		"key": []byte("{\"b\":3,\"u\":1}"),
	}

	mockDownstreamConnector.EXPECT().
		Read(context.TODO(), testKeyValueEi, expectedValues, minFields).
		Return(nil, assert.AnError).
		Times(1)
	_, err := schemaConnector.Read(context.TODO(), testEi, values, minFields)
	assert.Error(t, err)

}
