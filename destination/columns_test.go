package main

import (
	"testing"

	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/stretchr/testify/assert"
)

func TestMakeTableDescription(t *testing.T) {
	description, err := MakeTableDescription([]*ColumnDefinition{})
	assert.ErrorContains(t, err, "expected non-empty list of column definitions")

	description, err = MakeTableDescription([]*ColumnDefinition{
		{Name: "id", Type: "Int32", IsPrimaryKey: true},
		{Name: "name", Type: "String", IsPrimaryKey: false},
		{Name: "age", Type: "Int32", IsPrimaryKey: false},
	})
	assert.NoError(t, err)
	assert.Equal(t, description, &TableDescription{
		Mapping: map[string]string{"id": "Int32", "name": "String", "age": "Int32"},
		Columns: []*ColumnDefinition{
			{Name: "id", Type: "Int32", IsPrimaryKey: true},
			{Name: "name", Type: "String", IsPrimaryKey: false},
			{Name: "age", Type: "Int32", IsPrimaryKey: false},
		},
		PrimaryKeys: []string{"id"},
	})
}

func TestToFivetranColumns(t *testing.T) {
	description, _ := MakeTableDescription([]*ColumnDefinition{
		{Name: "b", Type: "Bool", IsPrimaryKey: true},
		{Name: "i16", Type: "Int16", IsPrimaryKey: false},
		{Name: "i32", Type: "Int32", IsPrimaryKey: false},
		{Name: "i64", Type: "Int64", IsPrimaryKey: false},
		{Name: "f32", Type: "Float32", IsPrimaryKey: false},
		{Name: "f64", Type: "Float64", IsPrimaryKey: false},
		{Name: "dec", Type: "Decimal(10, 4)", IsPrimaryKey: false},
		{Name: "d", Type: "Date", IsPrimaryKey: false},
		{Name: "dt", Type: "DateTime", IsPrimaryKey: false},
		{Name: "dt_utc", Type: "DateTime64(9, 'UTC')", IsPrimaryKey: true},
		{Name: "json", Type: "JSON", IsPrimaryKey: false},
		{Name: "json_obj", Type: "Object('json')", IsPrimaryKey: false},
		{Name: "str", Type: "String", IsPrimaryKey: false},
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
		{Name: "json", Type: pb.DataType_JSON, PrimaryKey: false},
		{Name: "json_obj", Type: pb.DataType_JSON, PrimaryKey: false},
		{Name: "str", Type: pb.DataType_STRING, PrimaryKey: false},
	})

	_, err = ToFivetranColumns(nil)
	assert.ErrorContains(t, err, "no columns in table description")

	_, err = ToFivetranColumns(&TableDescription{})
	assert.ErrorContains(t, err, "no columns in table description")

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
			{Name: "json", Type: pb.DataType_JSON, PrimaryKey: false},
			{Name: "str", Type: pb.DataType_STRING, PrimaryKey: false},
		},
	}
	description, err := ToClickHouseColumns(table)
	assert.NoError(t, err)
	assert.Equal(t, description, &TableDescription{
		Mapping: map[string]string{
			"b":      "Bool",                 // PK field - not nullable
			"dt_utc": "DateTime64(9, 'UTC')", // PK field - not nullable
			"json":   "JSON",                 // JSON can't be Nullable in CH by design
			"i16":    "Nullable(Int16)",
			"i32":    "Nullable(Int32)",
			"i64":    "Nullable(Int64)",
			"f32":    "Nullable(Float32)",
			"f64":    "Nullable(Float64)",
			"dec":    "Nullable(Decimal(10, 4))",
			"d":      "Nullable(Date)",
			"dt":     "Nullable(DateTime)",
			"str":    "Nullable(String)",
		},
		Columns: []*ColumnDefinition{
			{Name: "b", Type: "Bool", IsPrimaryKey: true},
			{Name: "i16", Type: "Nullable(Int16)", IsPrimaryKey: false},
			{Name: "i32", Type: "Nullable(Int32)", IsPrimaryKey: false},
			{Name: "i64", Type: "Nullable(Int64)", IsPrimaryKey: false},
			{Name: "f32", Type: "Nullable(Float32)", IsPrimaryKey: false},
			{Name: "f64", Type: "Nullable(Float64)", IsPrimaryKey: false},
			{Name: "dec", Type: "Nullable(Decimal(10, 4))", IsPrimaryKey: false},
			{Name: "d", Type: "Nullable(Date)", IsPrimaryKey: false},
			{Name: "dt", Type: "Nullable(DateTime)", IsPrimaryKey: false},
			{Name: "dt_utc", Type: "DateTime64(9, 'UTC')", IsPrimaryKey: true},
			{Name: "json", Type: "JSON", IsPrimaryKey: false},
			{Name: "str", Type: "Nullable(String)", IsPrimaryKey: false},
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

func TestGetAlterTableOps(t *testing.T) {
	int64Type := "Int64"
	int16Type := "Int16"
	ops := GetAlterTableOps(
		&TableDescription{
			Mapping: map[string]string{"qaz": "Int32", "qux": "String"},
			Columns: []*ColumnDefinition{
				{Name: "qaz", Type: "Int32", IsPrimaryKey: true},
				{Name: "qux", Type: "String", IsPrimaryKey: false},
			},
		},
		&TableDescription{
			Mapping: map[string]string{"qaz": "Int32", "qux": "Int64"},
			Columns: []*ColumnDefinition{
				{Name: "qaz", Type: "Int32", IsPrimaryKey: true},
				{Name: "qux", Type: "Int64", IsPrimaryKey: false},
			},
		})
	assert.Equal(t, ops, []*AlterTableOp{{Op: Modify, Column: "qux", Type: &int64Type}})

	ops = GetAlterTableOps(
		&TableDescription{
			Mapping: map[string]string{"qaz": "Int32", "qux": "String"},
			Columns: []*ColumnDefinition{
				{Name: "qaz", Type: "Int32", IsPrimaryKey: true},
				{Name: "qux", Type: "String", IsPrimaryKey: false},
			},
		},
		&TableDescription{
			Mapping: map[string]string{"qaz": "Int32", "qux": "String", "zaq": "Int64"},
			Columns: []*ColumnDefinition{
				{Name: "qaz", Type: "Int32", IsPrimaryKey: true},
				{Name: "qux", Type: "String", IsPrimaryKey: false},
				{Name: "zaq", Type: "Int64", IsPrimaryKey: false},
			}})
	assert.Equal(t, ops, []*AlterTableOp{{Op: Add, Column: "zaq", Type: &int64Type}})

	ops = GetAlterTableOps(
		&TableDescription{
			Mapping: map[string]string{"qaz": "Int32", "qux": "String"},
			Columns: []*ColumnDefinition{
				{Name: "qaz", Type: "Int32", IsPrimaryKey: true},
				{Name: "qux", Type: "String", IsPrimaryKey: false},
			},
		},
		&TableDescription{
			Mapping: map[string]string{"qaz": "Int32"},
			Columns: []*ColumnDefinition{
				{Name: "qaz", Type: "Int32", IsPrimaryKey: true},
			}})
	assert.Equal(t, ops, []*AlterTableOp{{Op: Drop, Column: "qux"}})

	ops = GetAlterTableOps(
		&TableDescription{
			Mapping: map[string]string{"qaz": "Int32", "qux": "String"},
			Columns: []*ColumnDefinition{
				{Name: "qaz", Type: "Int32", IsPrimaryKey: true},
				{Name: "qux", Type: "String", IsPrimaryKey: false},
			},
		},
		&TableDescription{
			Mapping: map[string]string{"qux": "Int64", "zaq": "Int16"},
			Columns: []*ColumnDefinition{
				{Name: "qux", Type: "Int64", IsPrimaryKey: false},
				{Name: "zaq", Type: "Int16", IsPrimaryKey: false},
			}})
	assert.Equal(t, ops, []*AlterTableOp{
		{Op: Modify, Column: "qux", Type: &int64Type},
		{Op: Add, Column: "zaq", Type: &int16Type},
		{Op: Drop, Column: "qaz"},
	})

	// Equal tables
	td := &TableDescription{
		Mapping: map[string]string{"qaz": "Int32", "qux": "String"},
		Columns: []*ColumnDefinition{
			{Name: "qaz", Type: "Int32", IsPrimaryKey: true},
			{Name: "qux", Type: "String", IsPrimaryKey: false},
		},
	}
	ops = GetAlterTableOps(td, td)
	assert.Equal(t, ops, []*AlterTableOp{})
}
