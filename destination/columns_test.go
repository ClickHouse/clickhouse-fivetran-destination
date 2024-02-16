package main

import (
	"testing"

	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/stretchr/testify/assert"
)

func TestMakeTableDescription(t *testing.T) {
	description := MakeTableDescription([]*ColumnDefinition{})
	assert.Equal(t, description, &TableDescription{})

	col1 := &ColumnDefinition{Name: "id", Type: "Int32", IsPrimaryKey: true}
	col2 := &ColumnDefinition{Name: "name", Type: "String", IsPrimaryKey: false}
	col3 := &ColumnDefinition{Name: "age", Type: "Int32", IsPrimaryKey: false}

	description = MakeTableDescription([]*ColumnDefinition{col1, col2, col3})
	assert.Equal(t, description, &TableDescription{
		Mapping:     map[string]*ColumnDefinition{"id": col1, "name": col2, "age": col3},
		Columns:     []*ColumnDefinition{col1, col2, col3},
		PrimaryKeys: []string{"id"},
	})
}

func TestToFivetranColumns(t *testing.T) {
	description := MakeTableDescription([]*ColumnDefinition{
		{Name: "b", Type: "Bool", IsPrimaryKey: true},
		{Name: "i16", Type: "Int16", IsPrimaryKey: false},
		{Name: "i32", Type: "Int32", IsPrimaryKey: false},
		{Name: "i64", Type: "Int64", IsPrimaryKey: false},
		{Name: "f32", Type: "Float32", IsPrimaryKey: false},
		{Name: "f64", Type: "Float64", IsPrimaryKey: false},
		{Name: "dec", Type: "Decimal(10, 4)", IsPrimaryKey: false, DecimalParams: &DecimalParams{Precision: 10, Scale: 4}},
		{Name: "d", Type: "Date", IsPrimaryKey: false},
		{Name: "dt", Type: "DateTime", IsPrimaryKey: false},
		{Name: "dt_utc", Type: "DateTime64(9, 'UTC')", IsPrimaryKey: true},
		{Name: "str", Type: "String", IsPrimaryKey: false},
		{Name: "j", Type: "String", IsPrimaryKey: false, Comment: "JSON"},
		{Name: "x", Type: "String", IsPrimaryKey: false, Comment: "XML"},
		{Name: "bin", Type: "String", IsPrimaryKey: false, Comment: "BINARY"},
	})
	columns, err := ToFivetranColumns(description)
	assert.NoError(t, err)
	assert.Equal(t, columns, []*pb.Column{
		{Name: "b", Type: pb.DataType_BOOLEAN, PrimaryKey: true},
		{Name: "i16", Type: pb.DataType_SHORT, PrimaryKey: false},
		{Name: "i32", Type: pb.DataType_INT, PrimaryKey: false},
		{Name: "i64", Type: pb.DataType_LONG, PrimaryKey: false},
		{Name: "f32", Type: pb.DataType_FLOAT, PrimaryKey: false},
		{Name: "f64", Type: pb.DataType_DOUBLE, PrimaryKey: false},
		{Name: "dec", Type: pb.DataType_DECIMAL, PrimaryKey: false, Decimal: &pb.DecimalParams{Precision: 10, Scale: 4}},
		{Name: "d", Type: pb.DataType_NAIVE_DATE, PrimaryKey: false},
		{Name: "dt", Type: pb.DataType_NAIVE_DATETIME, PrimaryKey: false},
		{Name: "dt_utc", Type: pb.DataType_UTC_DATETIME, PrimaryKey: true},
		{Name: "str", Type: pb.DataType_STRING, PrimaryKey: false},
		{Name: "j", Type: pb.DataType_JSON, PrimaryKey: false},
		{Name: "x", Type: pb.DataType_XML, PrimaryKey: false},
		{Name: "bin", Type: pb.DataType_BINARY, PrimaryKey: false},
	})

	columns, err = ToFivetranColumns(nil)
	assert.NoError(t, err)
	assert.Equal(t, columns, []*pb.Column{})

	columns, err = ToFivetranColumns(&TableDescription{})
	assert.NoError(t, err)
	assert.Equal(t, columns, []*pb.Column{})

	_, err = ToFivetranColumns(&TableDescription{Columns: []*ColumnDefinition{{Name: "a", Type: "Array(String)"}}})
	assert.ErrorContains(t, err, "can't map type Array(String) to Fivetran types")
}

func TestToClickHouseColumns(t *testing.T) {
	table := &pb.Table{
		Columns: []*pb.Column{
			{Name: "b", Type: pb.DataType_BOOLEAN, PrimaryKey: true},
			{Name: "i16", Type: pb.DataType_SHORT, PrimaryKey: false},
			{Name: "i32", Type: pb.DataType_INT, PrimaryKey: false},
			{Name: "i64", Type: pb.DataType_LONG, PrimaryKey: false},
			{Name: "f32", Type: pb.DataType_FLOAT, PrimaryKey: false},
			{Name: "f64", Type: pb.DataType_DOUBLE, PrimaryKey: false},
			{Name: "dec", Type: pb.DataType_DECIMAL, PrimaryKey: false, Decimal: &pb.DecimalParams{Precision: 10, Scale: 4}},
			{Name: "d", Type: pb.DataType_NAIVE_DATE, PrimaryKey: false},
			{Name: "dt", Type: pb.DataType_NAIVE_DATETIME, PrimaryKey: false},
			{Name: "dt_utc", Type: pb.DataType_UTC_DATETIME, PrimaryKey: true},
			{Name: "str", Type: pb.DataType_STRING, PrimaryKey: false},
			{Name: "j", Type: pb.DataType_JSON, PrimaryKey: false},
			{Name: "x", Type: pb.DataType_XML, PrimaryKey: false},
			{Name: "bin", Type: pb.DataType_BINARY, PrimaryKey: false},
		},
	}

	// PK fields - not nullable
	boolCol := &ColumnDefinition{Name: "b", Type: "Bool", IsPrimaryKey: true}
	utcCol := &ColumnDefinition{Name: "dt_utc", Type: "DateTime64(9, 'UTC')", IsPrimaryKey: true}
	// The rest of the fields are nullable
	i16Col := &ColumnDefinition{Name: "i16", Type: "Nullable(Int16)", IsPrimaryKey: false}
	i32Col := &ColumnDefinition{Name: "i32", Type: "Nullable(Int32)", IsPrimaryKey: false}
	i64Col := &ColumnDefinition{Name: "i64", Type: "Nullable(Int64)", IsPrimaryKey: false}
	f32Col := &ColumnDefinition{Name: "f32", Type: "Nullable(Float32)", IsPrimaryKey: false}
	f64Col := &ColumnDefinition{Name: "f64", Type: "Nullable(Float64)", IsPrimaryKey: false}
	decimalCol := &ColumnDefinition{Name: "dec", Type: "Nullable(Decimal(10, 4))", IsPrimaryKey: false}
	dateCol := &ColumnDefinition{Name: "d", Type: "Nullable(Date)", IsPrimaryKey: false}
	datetimeCol := &ColumnDefinition{Name: "dt", Type: "Nullable(DateTime)", IsPrimaryKey: false}
	strCol := &ColumnDefinition{Name: "str", Type: "Nullable(String)", IsPrimaryKey: false}
	jsonCol := &ColumnDefinition{Name: "j", Type: "Nullable(String)", IsPrimaryKey: false, Comment: "JSON"}
	xmlCol := &ColumnDefinition{Name: "x", Type: "Nullable(String)", IsPrimaryKey: false, Comment: "XML"}
	binaryCol := &ColumnDefinition{Name: "bin", Type: "Nullable(String)", IsPrimaryKey: false, Comment: "BINARY"}

	description, err := ToClickHouseColumns(table)
	assert.NoError(t, err)
	assert.Equal(t, description, &TableDescription{
		Mapping: map[string]*ColumnDefinition{
			"b":      boolCol,
			"i16":    i16Col,
			"i32":    i32Col,
			"i64":    i64Col,
			"f32":    f32Col,
			"f64":    f64Col,
			"dec":    decimalCol,
			"d":      dateCol,
			"dt":     datetimeCol,
			"dt_utc": utcCol,
			"str":    strCol,
			"j":      jsonCol,
			"x":      xmlCol,
			"bin":    binaryCol,
		},
		Columns: []*ColumnDefinition{
			boolCol, i16Col, i32Col, i64Col,
			f32Col, f64Col, decimalCol,
			dateCol, datetimeCol, utcCol,
			strCol, jsonCol, xmlCol, binaryCol,
		},
		PrimaryKeys: []string{"b", "dt_utc"},
	})

	_, err = ToClickHouseColumns(nil)
	assert.ErrorContains(t, err, "no columns in Fivetran table definition")

	_, err = ToClickHouseColumns(&pb.Table{})
	assert.ErrorContains(t, err, "no columns in Fivetran table definition")

	_, err = ToClickHouseColumns(&pb.Table{Columns: []*pb.Column{{Name: "a", Type: pb.DataType_UNSPECIFIED}}})
	assert.ErrorContains(t, err, "unknown datatype UNSPECIFIED")
}

func TestGetAlterTableOpsModify(t *testing.T) {
	int64Type := "Int64"
	emptyComment := ""
	curCol1 := &ColumnDefinition{Name: "qaz", Type: "Int32", IsPrimaryKey: true}
	curCol2 := &ColumnDefinition{Name: "qux", Type: "String", IsPrimaryKey: false}
	alterCol1 := &ColumnDefinition{Name: "qaz", Type: "Int32", IsPrimaryKey: true}
	alterCol2 := &ColumnDefinition{Name: "qux", Type: "Int64", IsPrimaryKey: false}
	ops := GetAlterTableOps(
		&TableDescription{
			Mapping: map[string]*ColumnDefinition{"qaz": curCol1, "qux": curCol2},
			Columns: []*ColumnDefinition{curCol1, curCol2},
		},
		&TableDescription{
			Mapping: map[string]*ColumnDefinition{"qaz": alterCol1, "qux": alterCol2},
			Columns: []*ColumnDefinition{alterCol1, alterCol2},
		})
	assert.Equal(t, ops, []*AlterTableOp{{Op: Modify, Column: "qux", Type: &int64Type, Comment: &emptyComment}})
}

func TestGetAlterTableOpsAdd(t *testing.T) {
	int64Type := "Int64"
	emptyComment := ""
	curCol1 := &ColumnDefinition{Name: "qaz", Type: "Int32", IsPrimaryKey: true}
	curCol2 := &ColumnDefinition{Name: "qux", Type: "String", IsPrimaryKey: false}
	alterCol1 := &ColumnDefinition{Name: "qaz", Type: "Int32", IsPrimaryKey: true}
	alterCol2 := &ColumnDefinition{Name: "qux", Type: "String", IsPrimaryKey: false}
	alterCol3 := &ColumnDefinition{Name: "zaq", Type: "Int64", IsPrimaryKey: false}
	ops := GetAlterTableOps(
		&TableDescription{
			Mapping: map[string]*ColumnDefinition{"qaz": curCol1, "qux": curCol2},
			Columns: []*ColumnDefinition{curCol1, curCol2},
		},
		&TableDescription{
			Mapping: map[string]*ColumnDefinition{"qaz": alterCol1, "qux": alterCol2, "zaq": alterCol3},
			Columns: []*ColumnDefinition{alterCol1, alterCol2, alterCol3},
		})
	assert.Equal(t, ops, []*AlterTableOp{{Op: Add, Column: "zaq", Type: &int64Type, Comment: &emptyComment}})

}

func TestGetAlterTableOpsDrop(t *testing.T) {
	curCol1 := &ColumnDefinition{Name: "qaz", Type: "Int32", IsPrimaryKey: true}
	curCol2 := &ColumnDefinition{Name: "qux", Type: "String", IsPrimaryKey: false}
	alterCol1 := &ColumnDefinition{Name: "qaz", Type: "Int32", IsPrimaryKey: true}
	ops := GetAlterTableOps(
		&TableDescription{
			Mapping: map[string]*ColumnDefinition{"qaz": curCol1, "qux": curCol2},
			Columns: []*ColumnDefinition{curCol1, curCol2},
		},
		&TableDescription{
			Mapping: map[string]*ColumnDefinition{"qaz": alterCol1},
			Columns: []*ColumnDefinition{alterCol1},
		})
	assert.Equal(t, ops, []*AlterTableOp{{Op: Drop, Column: "qux", Type: nil, Comment: nil}})
}

func TestGetAlterTableOpsCombined(t *testing.T) {
	int64Type := "Int64"
	int16Type := "Int16"
	emptyComment := ""
	curCol1 := &ColumnDefinition{Name: "qaz", Type: "Int32", IsPrimaryKey: true}
	curCol2 := &ColumnDefinition{Name: "qux", Type: "String", IsPrimaryKey: false}
	alterCol1 := &ColumnDefinition{Name: "qux", Type: "Int64", IsPrimaryKey: false}
	alterCol2 := &ColumnDefinition{Name: "zaq", Type: "Int16", IsPrimaryKey: false}
	ops := GetAlterTableOps(
		&TableDescription{
			Mapping: map[string]*ColumnDefinition{"qaz": curCol1, "qux": curCol2},
			Columns: []*ColumnDefinition{curCol1, curCol2},
		},
		&TableDescription{
			Mapping: map[string]*ColumnDefinition{"qux": alterCol1, "zaq": alterCol2},
			Columns: []*ColumnDefinition{alterCol1, alterCol2},
		})
	assert.Equal(t, ops, []*AlterTableOp{
		{Op: Modify, Column: "qux", Type: &int64Type, Comment: &emptyComment},
		{Op: Add, Column: "zaq", Type: &int16Type, Comment: &emptyComment},
		{Op: Drop, Column: "qaz", Type: nil, Comment: nil},
	})
}

func TestGetAlterTableOpsEqualTables(t *testing.T) {
	// Make sure they all are different pointers
	curCol1 := &ColumnDefinition{Name: "qaz", Type: "Int32", IsPrimaryKey: true}
	curCol2 := &ColumnDefinition{Name: "qux", Type: "String", IsPrimaryKey: false}
	alterCol1 := &ColumnDefinition{Name: "qaz", Type: "Int32", IsPrimaryKey: true}
	alterCol2 := &ColumnDefinition{Name: "qux", Type: "String", IsPrimaryKey: false}
	td1 := &TableDescription{
		Mapping: map[string]*ColumnDefinition{"qaz": curCol1, "qux": curCol2},
		Columns: []*ColumnDefinition{curCol1, curCol2},
	}
	td2 := &TableDescription{
		Mapping: map[string]*ColumnDefinition{"qaz": alterCol1, "qux": alterCol2},
		Columns: []*ColumnDefinition{alterCol1, alterCol2},
	}
	ops := GetAlterTableOps(td1, td2)
	assert.Equal(t, ops, []*AlterTableOp{})
}

func TestGetAlterTableOpsWithComments(t *testing.T) {
	strType := "String"
	emptyComment := ""
	xmlComment := "XML"
	binaryComment := "BINARY"
	curCol1 := &ColumnDefinition{Name: "s1", Type: strType, IsPrimaryKey: true, Comment: emptyComment}
	curCol2 := &ColumnDefinition{Name: "s2", Type: strType, IsPrimaryKey: true, Comment: emptyComment}
	curCol3 := &ColumnDefinition{Name: "s3", Type: strType, IsPrimaryKey: false, Comment: xmlComment}
	curCol4 := &ColumnDefinition{Name: "s4", Type: strType, IsPrimaryKey: false, Comment: binaryComment}
	curCol5 := &ColumnDefinition{Name: "s5", Type: strType, IsPrimaryKey: false, Comment: emptyComment}
	alterCol1 := &ColumnDefinition{Name: "s1", Type: strType, IsPrimaryKey: true, Comment: binaryComment}
	alterCol2 := &ColumnDefinition{Name: "s2", Type: strType, IsPrimaryKey: true, Comment: xmlComment}
	alterCol3 := &ColumnDefinition{Name: "s3", Type: strType, IsPrimaryKey: false, Comment: emptyComment}
	alterCol4 := &ColumnDefinition{Name: "s4", Type: strType, IsPrimaryKey: false, Comment: emptyComment}
	alterCol5 := &ColumnDefinition{Name: "s10", Type: strType, IsPrimaryKey: false, Comment: emptyComment}
	alterCol6 := &ColumnDefinition{Name: "s11", Type: strType, IsPrimaryKey: false, Comment: binaryComment}
	alterCol7 := &ColumnDefinition{Name: "s12", Type: strType, IsPrimaryKey: false, Comment: xmlComment}
	ops := GetAlterTableOps(
		&TableDescription{
			Mapping: map[string]*ColumnDefinition{
				"s1": curCol1, "s2": curCol2, "s3": curCol3, "s4": curCol4, "s5": curCol5,
			},
			Columns: []*ColumnDefinition{curCol1, curCol2, curCol3, curCol4, curCol5},
		},
		&TableDescription{
			Mapping: map[string]*ColumnDefinition{
				"s1": alterCol1, "s2": alterCol2, "s3": alterCol3, "s4": alterCol4,
				"s10": alterCol5, "s11": alterCol6, "s12": alterCol7,
			},
			Columns: []*ColumnDefinition{alterCol1, alterCol2, alterCol3, alterCol4, alterCol5, alterCol6, alterCol7},
		})
	assert.Equal(t, ops, []*AlterTableOp{
		{Op: Modify, Column: "s1", Type: &strType, Comment: &binaryComment},
		{Op: Modify, Column: "s2", Type: &strType, Comment: &xmlComment},
		{Op: Modify, Column: "s3", Type: &strType, Comment: &emptyComment},
		{Op: Modify, Column: "s4", Type: &strType, Comment: &emptyComment},
		{Op: Add, Column: "s10", Type: &strType, Comment: &emptyComment},
		{Op: Add, Column: "s11", Type: &strType, Comment: &binaryComment},
		{Op: Add, Column: "s12", Type: &strType, Comment: &xmlComment},
		{Op: Drop, Column: "s5", Type: nil, Comment: nil},
	})
}
