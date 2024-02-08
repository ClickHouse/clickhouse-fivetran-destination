package main

import (
	"testing"

	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/stretchr/testify/assert"
)

func TestRemoveNullable(t *testing.T) {
	assert.Equal(t, "Int32", RemoveNullable("Nullable(Int32)"))
	assert.Equal(t, "Decimal(10, 2)", RemoveNullable("Nullable(Decimal(10, 2))"))
	assert.Equal(t, "String", RemoveNullable("String"))
}

func TestGetFivetranDataType(t *testing.T) {
	args := []struct {
		string
		pb.DataType
	}{
		{"Bool", pb.DataType_BOOLEAN},
		{"Nullable(Bool)", pb.DataType_BOOLEAN},
		{"Int32", pb.DataType_INT},
		{"Nullable(Int32)", pb.DataType_INT},
		{"Int64", pb.DataType_LONG},
		{"Nullable(Int64)", pb.DataType_LONG},
		{"Float32", pb.DataType_FLOAT},
		{"Nullable(Float32)", pb.DataType_FLOAT},
		{"Float64", pb.DataType_DOUBLE},
		{"Nullable(Float64)", pb.DataType_DOUBLE},
		{"Date", pb.DataType_NAIVE_DATE},
		{"Nullable(Date)", pb.DataType_NAIVE_DATE},
		{"DateTime", pb.DataType_NAIVE_DATETIME},
		{"Nullable(DateTime)", pb.DataType_NAIVE_DATETIME},
		{"DateTime64(9, 'UTC')", pb.DataType_UTC_DATETIME},
		{"Nullable(DateTime64(9, 'UTC'))", pb.DataType_UTC_DATETIME},
		{"String", pb.DataType_STRING},
		{"Nullable(String)", pb.DataType_STRING},
		{"JSON", pb.DataType_JSON}, // JSON can't be nullable in CH
		{"Object('json')", pb.DataType_JSON},
	}
	for _, arg := range args {
		dataType, decimalParams, err := GetFivetranDataType(arg.string)
		assert.NoError(t, err, "Error for CH type %s and Fivetran type %s should be nil", arg.string, arg.DataType.String())
		assert.Nil(t, decimalParams, "Decimal params for CH type %s and Fivetran type %s should be nil", arg.string, arg.DataType.String())
		assert.Equal(t, arg.DataType, dataType, "Fivetran type for CH type %s should be %s", arg.string, arg.DataType.String())
	}

	dataType, _, err := GetFivetranDataType("Array(String)")
	assert.ErrorContains(t, err, "can't map type Array(String) to Fivetran types")
	assert.Equal(t, pb.DataType_UNSPECIFIED, dataType)
}

func TestGetFivetranDataTypeDecimals(t *testing.T) {
	dataType, decimalParams, err := GetFivetranDataType("Decimal(10, 2)")
	assert.NoError(t, err)
	assert.Equal(t, &pb.DecimalParams{Precision: 10, Scale: 2}, decimalParams)
	assert.Equal(t, pb.DataType_DECIMAL, dataType)

	_, _, err = GetFivetranDataType("Decimal(2)")
	assert.ErrorContains(t, err, "invalid decimal type Decimal(2), expected two parameters - precision and scale")

	_, _, err = GetFivetranDataType("Decimal(x, 2)")
	assert.ErrorContains(t, err, "can't parse precision")

	_, _, err = GetFivetranDataType("Decimal(-1, 2)")
	assert.ErrorContains(t, err, "invalid decimal type Decimal(-1, 2): precision can't be negative")

	_, _, err = GetFivetranDataType("Decimal(2, x)")
	assert.ErrorContains(t, err, "can't parse scale")

	_, _, err = GetFivetranDataType("Decimal(2, -1)")
	assert.ErrorContains(t, err, "invalid decimal type Decimal(2, -1): scale can't be negative")
}

func TestGetClickHouseDataType(t *testing.T) {
	// Everything is nullable unless it's a PK, Fivetran metadata field or JSON
	args := []struct {
		pb.DataType
		string
	}{
		{pb.DataType_BOOLEAN, "Nullable(Bool)"},
		{pb.DataType_SHORT, "Nullable(Int16)"},
		{pb.DataType_INT, "Nullable(Int32)"},
		{pb.DataType_LONG, "Nullable(Int64)"},
		{pb.DataType_FLOAT, "Nullable(Float32)"},
		{pb.DataType_DOUBLE, "Nullable(Float64)"},
		{pb.DataType_DECIMAL, "Nullable(Decimal)"}, // just `Decimal` if DecimalParams are not set
		{pb.DataType_STRING, "Nullable(String)"},
		{pb.DataType_NAIVE_DATE, "Nullable(Date)"},
		{pb.DataType_NAIVE_DATETIME, "Nullable(DateTime)"},
		{pb.DataType_UTC_DATETIME, "Nullable(DateTime64(9, 'UTC'))"},
		{pb.DataType_JSON, "JSON"}, // JSON can't be nullable in CH
		// Unclear CH mapping, may be removed
		{pb.DataType_BINARY, "Nullable(String)"},
		{pb.DataType_XML, "Nullable(String)"},
	}
	for _, arg := range args {
		colType, err := GetClickHouseDataType(&pb.Column{Type: arg.DataType})
		assert.NoError(t, err, "Error for Fivetran type %s and CH type %s should be nil", arg.DataType.String(), arg.string)
		assert.Equal(t, arg.string, colType, "CH type for Fivetran type %s should be %s", arg.DataType.String(), arg.string)
	}

	_, err := GetClickHouseDataType(&pb.Column{Type: pb.DataType_UNSPECIFIED})
	assert.ErrorContains(t, err, "unknown datatype UNSPECIFIED")
}

func TestGetClickHouseDataTypeFivetranMetadata(t *testing.T) {
	// Fivetran metadata columns have known types and those are not nullable
	colType, err := GetClickHouseDataType(&pb.Column{Name: "_fivetran_id"})
	assert.NoError(t, err)
	assert.Equal(t, "String", colType)
	colType, err = GetClickHouseDataType(&pb.Column{Name: "_fivetran_synced"})
	assert.NoError(t, err)
	assert.Equal(t, "DateTime64(9, 'UTC')", colType)
	colType, err = GetClickHouseDataType(&pb.Column{Name: "_fivetran_deleted"})
	assert.NoError(t, err)
	assert.Equal(t, "Bool", colType)
}

func TestGetClickHouseDataTypePrimaryKeys(t *testing.T) {
	// Primary keys are not nullable
	args := []struct {
		pb.DataType
		string
	}{
		{pb.DataType_BOOLEAN, "Bool"},
		{pb.DataType_SHORT, "Int16"},
		{pb.DataType_INT, "Int32"},
		{pb.DataType_LONG, "Int64"},
		{pb.DataType_FLOAT, "Float32"},
		{pb.DataType_DOUBLE, "Float64"},
		{pb.DataType_DECIMAL, "Decimal"}, // just `Decimal` if DecimalParams are not set
		{pb.DataType_STRING, "String"},
		{pb.DataType_NAIVE_DATE, "Date"},
		{pb.DataType_NAIVE_DATETIME, "DateTime"},
		{pb.DataType_UTC_DATETIME, "DateTime64(9, 'UTC')"},
		{pb.DataType_JSON, "JSON"},
		// Unclear CH mapping, may be removed
		{pb.DataType_BINARY, "String"},
		{pb.DataType_XML, "String"},
	}
	for _, arg := range args {
		colType, err := GetClickHouseDataType(&pb.Column{Type: arg.DataType, PrimaryKey: true})
		assert.NoError(t, err, "Error for PK Fivetran type %s and CH type %s should be nil", arg.DataType.String(), arg.string)
		assert.Equal(t, arg.string, colType, "PK CH type for Fivetran type %s should be %s", arg.DataType.String(), arg.string)
	}
}

func TestGetClickHouseDataTypeDecimals(t *testing.T) {
	dataType, err := GetClickHouseDataType(&pb.Column{Type: pb.DataType_DECIMAL, Decimal: &pb.DecimalParams{
		Precision: 4,
		Scale:     2,
	}})
	assert.NoError(t, err)
	assert.Equal(t, "Nullable(Decimal(4, 2))", dataType)

	dataType, err = GetClickHouseDataType(&pb.Column{Type: pb.DataType_DECIMAL, Decimal: &pb.DecimalParams{
		Precision: 76,
		Scale:     76,
	}})
	assert.NoError(t, err)
	assert.Equal(t, "Nullable(Decimal(76, 76))", dataType)

	// Precision and scale normalization
	dataType, err = GetClickHouseDataType(&pb.Column{Type: pb.DataType_DECIMAL, Decimal: &pb.DecimalParams{
		Precision: 5,
		Scale:     76,
	}})
	assert.NoError(t, err)
	assert.Equal(t, "Nullable(Decimal(5, 5))", dataType)

	dataType, err = GetClickHouseDataType(&pb.Column{Type: pb.DataType_DECIMAL, Decimal: &pb.DecimalParams{
		Precision: 200,
		Scale:     5,
	}})
	assert.NoError(t, err)
	assert.Equal(t, "Nullable(Decimal(76, 5))", dataType)

	dataType, err = GetClickHouseDataType(&pb.Column{Type: pb.DataType_DECIMAL, Decimal: &pb.DecimalParams{
		Precision: 200,
		Scale:     200,
	}})
	assert.NoError(t, err)
	assert.Equal(t, "Nullable(Decimal(76, 76))", dataType)

	// PK Decimal is not nullable
	dataType, err = GetClickHouseDataType(&pb.Column{
		Type:       pb.DataType_DECIMAL,
		PrimaryKey: true,
		Decimal: &pb.DecimalParams{
			Precision: 4,
			Scale:     2,
		}})
	assert.NoError(t, err)
	assert.Equal(t, "Decimal(4, 2)", dataType)
}
