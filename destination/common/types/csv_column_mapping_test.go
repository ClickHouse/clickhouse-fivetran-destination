package types

import (
	"reflect"
	"testing"
	"time"

	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/stretchr/testify/assert"
)

func TestMakeCSVColumnMappingEqual(t *testing.T) {
	mapping, err := MakeCSVColumns(
		map[string]*DriverColumn{
			"col1": {Name: "col1", DatabaseType: "Int32", ScanType: reflect.TypeOf(int32(0)), Index: 0},
			"col2": {Name: "col2", DatabaseType: "String", ScanType: reflect.TypeOf(""), Index: 1},
			"col3": {Name: "col3", DatabaseType: "DateTime", ScanType: reflect.TypeOf(time.Time{}), Index: 2},
		},
		[]string{"col1", "col2", "col3"},
		map[string]*pb.Column{
			"col1": {Name: "col1", Type: pb.DataType_INT},
			"col2": {Name: "col2", Type: pb.DataType_STRING, PrimaryKey: true},
			"col3": {Name: "col3", Type: pb.DataType_NAIVE_DATETIME},
		})
	assert.NoError(t, err)
	assert.Equal(t, []*CSVColumn{
		{Index: 0, TableIndex: 0, Name: "col1", Type: pb.DataType_INT, IsPrimaryKey: false},
		{Index: 1, TableIndex: 1, Name: "col2", Type: pb.DataType_STRING, IsPrimaryKey: true},
		{Index: 2, TableIndex: 2, Name: "col3", Type: pb.DataType_NAIVE_DATETIME, IsPrimaryKey: false},
	}, mapping)
}

//func TestMakeCSVColumnMappingDifferentOrder(t *testing.T) {
//	mapping, err := MakeCSVColumns(
//		map[string]uint{"col1": 0, "col2": 1, "col3": 2},
//		[]string{"col3", "col1", "col2"},
//		&pb.Table{Columns: []*pb.Column{
//			{Name: "col1", Type: pb.DataType_INT},
//			{Name: "col2", Type: pb.DataType_STRING, PrimaryKey: true},
//			{Name: "col3", Type: pb.DataType_NAIVE_DATETIME},
//		}})
//	assert.NoError(t, err)
//	assert.Equal(t, []*CSVColumn{
//		{Index: 0, TableIndex: 2, Name: "col3", Type: pb.DataType_STRING, IsPrimaryKey: true},
//		{Index: 1, TableIndex: 0, Name: "col1", Type: pb.DataType_INT, IsPrimaryKey: false},
//		{Index: 2, TableIndex: 1, Name: "col2", Type: pb.DataType_NAIVE_DATETIME, IsPrimaryKey: false},
//	}, mapping)
//}

//func TestMakeCSVColumnMappingEmptyHeader(t *testing.T) {
//	dbColIndexMap := map[string]uint{"col1": 0}
//	_, err := MakeCSVColumns(dbColIndexMap, []string{})
//	assert.ErrorContains(t, err, "input file header is empty")
//	_, err = MakeCSVColumns(dbColIndexMap, nil)
//	assert.ErrorContains(t, err, "input file header is empty")
//}
//
//func TestMakeCSVColumnMappingCountMismatch(t *testing.T) {
//	dbColIndexMap := map[string]uint{"col1": 0, "col2": 1}
//	_, err := MakeCSVColumns(dbColIndexMap, []string{"col1"})
//	assert.ErrorContains(t, err, "columns count in ClickHouse table (2) does not match the input file (1)")
//	_, err = MakeCSVColumns(dbColIndexMap, []string{"col1", "col2", "col3"})
//	assert.ErrorContains(t, err, "columns count in ClickHouse table (2) does not match the input file (3)")
//}
//
//func TestMakeCSVColumnMappingMissingColumn(t *testing.T) {
//	dbColIndexMap := map[string]uint{"col1": 0, "col2": 1}
//	_, err := MakeCSVColumns(dbColIndexMap, []string{"col2", "col3"})
//	assert.ErrorContains(t, err, "column col3 was not found in the input file. ClickHouse columns: col1, col2; input file columns: col2, col3")
//}
