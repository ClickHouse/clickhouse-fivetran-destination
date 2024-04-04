package types

import (
	"reflect"
	"testing"

	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/stretchr/testify/assert"
)

func TestMakeCSVColumnMappingEqual(t *testing.T) {
	mapping, err := MakeCSVColumns([]string{"col1", "col2", "col3"}, dbCols, fivetranColumnsMap)
	assert.NoError(t, err)
	assert.Equal(t, &CSVColumns{
		All: []*CSVColumn{
			{Index: 0, TableIndex: 0, Name: "col1", Type: pb.DataType_INT, IsPrimaryKey: false},
			{Index: 1, TableIndex: 1, Name: "col2", Type: pb.DataType_STRING, IsPrimaryKey: true},
			{Index: 2, TableIndex: 2, Name: "col3", Type: pb.DataType_NAIVE_DATETIME, IsPrimaryKey: false}},
		PrimaryKeys: []*CSVColumn{
			{Index: 1, TableIndex: 1, Name: "col2", Type: pb.DataType_STRING, IsPrimaryKey: true}},
	}, mapping)
}

func TestMakeCSVColumnMappingDifferentOrder(t *testing.T) {
	mapping, err := MakeCSVColumns([]string{"col2", "col1", "col3"}, dbCols, fivetranColumnsMap)
	assert.NoError(t, err)
	assert.Equal(t, &CSVColumns{
		All: []*CSVColumn{
			{Index: 0, TableIndex: 1, Name: "col2", Type: pb.DataType_STRING, IsPrimaryKey: true},
			{Index: 1, TableIndex: 0, Name: "col1", Type: pb.DataType_INT, IsPrimaryKey: false},
			{Index: 2, TableIndex: 2, Name: "col3", Type: pb.DataType_NAIVE_DATETIME, IsPrimaryKey: false}},
		PrimaryKeys: []*CSVColumn{
			{Index: 0, TableIndex: 1, Name: "col2", Type: pb.DataType_STRING, IsPrimaryKey: true}},
	}, mapping)
}

func TestMakeCSVColumnMappingSingleColumn(t *testing.T) {
	mapping, err := MakeCSVColumns(
		[]string{"foo"},
		&DriverColumns{
			Mapping: map[string]*DriverColumn{"foo": {Name: "col1", DatabaseType: "String", ScanType: reflect.TypeOf(""), Index: 0}},
			Columns: []*DriverColumn{{Name: "foo", DatabaseType: "String", ScanType: reflect.TypeOf(""), Index: 0}}},
		map[string]*pb.Column{"foo": fivetranCol2})
	assert.NoError(t, err)
	assert.Equal(t, &CSVColumns{
		All:         []*CSVColumn{{Index: 0, TableIndex: 0, Name: "foo", Type: pb.DataType_STRING, IsPrimaryKey: true}},
		PrimaryKeys: []*CSVColumn{{Index: 0, TableIndex: 0, Name: "foo", Type: pb.DataType_STRING, IsPrimaryKey: true}},
	}, mapping)
}

func TestMakeCSVColumnMappingEmptyHeader(t *testing.T) {
	_, err := MakeCSVColumns([]string{}, dbCols, fivetranColumnsMap)
	assert.ErrorContains(t, err, "input file header is empty")
	_, err = MakeCSVColumns(nil, dbCols, fivetranColumnsMap)
	assert.ErrorContains(t, err, "input file header is empty")
}

func TestMakeCSVColumnMappingCountMismatch(t *testing.T) {
	_, err := MakeCSVColumns([]string{"col1"}, dbCols, fivetranColumnsMap)
	assert.ErrorContains(t, err, "columns count in ClickHouse table (3) does not match the input file (1)")
	_, err = MakeCSVColumns([]string{"col1", "col2"}, dbCols, fivetranColumnsMap)
	assert.ErrorContains(t, err, "columns count in ClickHouse table (3) does not match the input file (2)")
}

var (
	dbCol1 = &DriverColumn{Name: "col1", DatabaseType: "Int32", ScanType: scanTypeNullableInt32, Index: 0}
	dbCol2 = &DriverColumn{Name: "col2", DatabaseType: "String", ScanType: scanTypeString, Index: 1}
	dbCol3 = &DriverColumn{Name: "col3", DatabaseType: "DateTime", ScanType: scanTypeNullableTime, Index: 2}
	dbCols = &DriverColumns{
		Mapping: map[string]*DriverColumn{"col1": dbCol1, "col2": dbCol2, "col3": dbCol3},
		Columns: []*DriverColumn{dbCol1, dbCol2, dbCol3}}

	fivetranCol1       = &pb.Column{Name: "col1", Type: pb.DataType_INT}
	fivetranCol2       = &pb.Column{Name: "col2", Type: pb.DataType_STRING, PrimaryKey: true}
	fivetranCol3       = &pb.Column{Name: "col3", Type: pb.DataType_NAIVE_DATETIME}
	fivetranColumnsMap = map[string]*pb.Column{"col1": fivetranCol1, "col2": fivetranCol2, "col3": fivetranCol3}
)
