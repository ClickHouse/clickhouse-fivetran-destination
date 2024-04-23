package data_types

import (
	"testing"

	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/stretchr/testify/assert"
)

func TestGetClickHouseDataType(t *testing.T) {
	// Everything is nullable unless it's a PK or Fivetran metadata field
	args := []struct {
		pb.DataType
		ClickHouseType
	}{
		{pb.DataType_BOOLEAN, ClickHouseType{Type: "Nullable(Bool)"}},
		{pb.DataType_SHORT, ClickHouseType{Type: "Nullable(Int16)"}},
		{pb.DataType_INT, ClickHouseType{Type: "Nullable(Int32)"}},
		{pb.DataType_LONG, ClickHouseType{Type: "Nullable(Int64)"}},
		{pb.DataType_FLOAT, ClickHouseType{Type: "Nullable(Float32)"}},
		{pb.DataType_DOUBLE, ClickHouseType{Type: "Nullable(Float64)"}},
		{pb.DataType_DECIMAL, ClickHouseType{Type: "Nullable(Decimal)"}}, // just `Decimal` if DecimalParams are not set
		{pb.DataType_STRING, ClickHouseType{Type: "Nullable(String)"}},
		{pb.DataType_NAIVE_DATE, ClickHouseType{Type: "Nullable(Date32)"}},
		{pb.DataType_NAIVE_DATETIME, ClickHouseType{Type: "Nullable(DateTime64(0, 'UTC'))"}},
		{pb.DataType_UTC_DATETIME, ClickHouseType{Type: "Nullable(DateTime64(9, 'UTC'))"}},
		{pb.DataType_JSON, ClickHouseType{Type: "Nullable(String)", Comment: "JSON"}},
		{pb.DataType_BINARY, ClickHouseType{Type: "Nullable(String)", Comment: "BINARY"}},
		{pb.DataType_XML, ClickHouseType{Type: "Nullable(String)", Comment: "XML"}},
	}
	for _, arg := range args {
		chType, err := ToClickHouseDataType(&pb.Column{Type: arg.DataType})
		assert.NoError(t, err,
			"Error for Fivetran type %s and CH type %s should be nil",
			arg.DataType.String(), arg.ClickHouseType.Type)
		assert.Equal(t, arg.ClickHouseType.Type, chType.Type,
			"CH type for Fivetran type %s should be %s",
			arg.DataType.String(), arg.ClickHouseType.Type)
		assert.Equal(t, arg.ClickHouseType.Comment, chType.Comment,
			"CH type comment for Fivetran type %s should be %s",
			arg.DataType.String(), arg.ClickHouseType.Comment)
	}

	_, err := ToClickHouseDataType(&pb.Column{Type: pb.DataType_UNSPECIFIED})
	assert.ErrorContains(t, err, "unknown datatype UNSPECIFIED")
}

func TestGetClickHouseDataTypeFivetranMetadata(t *testing.T) {
	// Fivetran metadata columns have known types and those are not nullable
	chType, err := ToClickHouseDataType(&pb.Column{Name: "_fivetran_id"})
	assert.NoError(t, err)
	assert.Equal(t, "String", chType.Type)
	assert.Equal(t, "", chType.Comment)
	chType, err = ToClickHouseDataType(&pb.Column{Name: "_fivetran_synced"})
	assert.NoError(t, err)
	assert.Equal(t, "DateTime64(9, 'UTC')", chType.Type)
	assert.Equal(t, "", chType.Comment)
	chType, err = ToClickHouseDataType(&pb.Column{Name: "_fivetran_deleted"})
	assert.NoError(t, err)
	assert.Equal(t, "Bool", chType.Type)
	assert.Equal(t, "", chType.Comment)
}

func TestGetClickHouseDataTypePrimaryKeys(t *testing.T) {
	// Primary keys are not nullable
	args := []struct {
		pb.DataType
		ClickHouseType
	}{
		{pb.DataType_BOOLEAN, ClickHouseType{Type: "Bool"}},
		{pb.DataType_SHORT, ClickHouseType{Type: "Int16"}},
		{pb.DataType_INT, ClickHouseType{Type: "Int32"}},
		{pb.DataType_LONG, ClickHouseType{Type: "Int64"}},
		{pb.DataType_FLOAT, ClickHouseType{Type: "Float32"}},
		{pb.DataType_DOUBLE, ClickHouseType{Type: "Float64"}},
		{pb.DataType_DECIMAL, ClickHouseType{Type: "Decimal"}}, // just `Decimal` if DecimalParams are not set
		{pb.DataType_STRING, ClickHouseType{Type: "String"}},
		{pb.DataType_NAIVE_DATE, ClickHouseType{Type: "Date32"}},
		{pb.DataType_NAIVE_DATETIME, ClickHouseType{Type: "DateTime64(0, 'UTC')"}},
		{pb.DataType_UTC_DATETIME, ClickHouseType{Type: "DateTime64(9, 'UTC')"}},
		{pb.DataType_JSON, ClickHouseType{Type: "String", Comment: "JSON"}},
		{pb.DataType_BINARY, ClickHouseType{Type: "String", Comment: "BINARY"}},
		{pb.DataType_XML, ClickHouseType{Type: "String", Comment: "XML"}},
	}
	for _, arg := range args {
		chType, err := ToClickHouseDataType(&pb.Column{Type: arg.DataType, PrimaryKey: true})
		assert.NoError(t, err,
			"Error for PK Fivetran type %s and CH type %s should be nil",
			arg.DataType.String(), arg.ClickHouseType.Type)
		assert.Equal(t, arg.ClickHouseType.Type, chType.Type,
			"PK CH type for Fivetran type %s should be %s",
			arg.DataType.String(), arg.ClickHouseType.Type)
		assert.Equal(t, arg.ClickHouseType.Comment, chType.Comment,
			"PK CH type comment for Fivetran type %s should be %s",
			arg.DataType.String(), arg.ClickHouseType.Comment)
	}
}

func TestGetClickHouseDataTypeDecimals(t *testing.T) {
	dataType, err := ToClickHouseDataType(&pb.Column{Type: pb.DataType_DECIMAL, Decimal: &pb.DecimalParams{
		Precision: 4,
		Scale:     2,
	}})
	assert.NoError(t, err)
	assert.Equal(t, ClickHouseType{Type: "Nullable(Decimal(4, 2))"}, dataType)

	dataType, err = ToClickHouseDataType(&pb.Column{Type: pb.DataType_DECIMAL, Decimal: &pb.DecimalParams{
		Precision: 76,
		Scale:     76,
	}})
	assert.NoError(t, err)
	assert.Equal(t, ClickHouseType{Type: "Nullable(Decimal(76, 76))"}, dataType)

	// Precision and scale normalization
	dataType, err = ToClickHouseDataType(&pb.Column{Type: pb.DataType_DECIMAL, Decimal: &pb.DecimalParams{
		Precision: 5,
		Scale:     76,
	}})
	assert.NoError(t, err)
	assert.Equal(t, ClickHouseType{Type: "Nullable(Decimal(5, 5))"}, dataType)

	dataType, err = ToClickHouseDataType(&pb.Column{Type: pb.DataType_DECIMAL, Decimal: &pb.DecimalParams{
		Precision: 200,
		Scale:     5,
	}})
	assert.NoError(t, err)
	assert.Equal(t, ClickHouseType{Type: "Nullable(Decimal(76, 5))"}, dataType)

	dataType, err = ToClickHouseDataType(&pb.Column{Type: pb.DataType_DECIMAL, Decimal: &pb.DecimalParams{
		Precision: 200,
		Scale:     200,
	}})
	assert.NoError(t, err)
	assert.Equal(t, ClickHouseType{Type: "Nullable(Decimal(76, 76))"}, dataType)

	// PK Decimal is not nullable
	dataType, err = ToClickHouseDataType(&pb.Column{
		Type:       pb.DataType_DECIMAL,
		PrimaryKey: true,
		Decimal: &pb.DecimalParams{
			Precision: 4,
			Scale:     2,
		}})
	assert.NoError(t, err)
	assert.Equal(t, ClickHouseType{Type: "Decimal(4, 2)"}, dataType)
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
		{"Date32", pb.DataType_NAIVE_DATE},
		{"Nullable(Date32)", pb.DataType_NAIVE_DATE},
		{"DateTime64(0, 'UTC')", pb.DataType_NAIVE_DATETIME},
		{"Nullable(DateTime64(0, 'UTC'))", pb.DataType_NAIVE_DATETIME},
		{"DateTime64(9, 'UTC')", pb.DataType_UTC_DATETIME},
		{"Nullable(DateTime64(9, 'UTC'))", pb.DataType_UTC_DATETIME},
		{"String", pb.DataType_STRING},
		{"Nullable(String)", pb.DataType_STRING},
	}
	for _, arg := range args {
		dataType, decimalParams, err := ToFivetranDataType(arg.string, "", nil)
		assert.NoError(t, err,
			"Error for CH type %s and Fivetran type %s should be nil",
			arg.string, arg.DataType.String())
		assert.Nil(t, decimalParams,
			"Decimal params for CH type %s and Fivetran type %s should be nil",
			arg.string, arg.DataType.String())
		assert.Equal(t, arg.DataType, dataType,
			"Fivetran type for CH type %s should be %s",
			arg.string, arg.DataType.String())
	}

	dataType, decimalParams, err := ToFivetranDataType("Decimal(10, 2)", "", &pb.DecimalParams{Precision: 10, Scale: 2})
	assert.NoError(t, err)
	assert.Equal(t, &pb.DecimalParams{Precision: 10, Scale: 2}, decimalParams)
	assert.Equal(t, pb.DataType_DECIMAL, dataType)

	dataType, _, err = ToFivetranDataType("Array(String)", "", nil)
	assert.ErrorContains(t, err, "can't map type Array(String) to Fivetran types")
	assert.Equal(t, pb.DataType_UNSPECIFIED, dataType)
}

func TestGetFivetranDataTypeWithComments(t *testing.T) {
	args := []struct {
		Type         string
		Comment      string
		FivetranType pb.DataType
	}{
		{"String", "", pb.DataType_STRING},
		{"Nullable(String)", "", pb.DataType_STRING},
		{"String", "XML", pb.DataType_XML},
		{"Nullable(String)", "XML", pb.DataType_XML},
		{"String", "BINARY", pb.DataType_BINARY},
		{"Nullable(String)", "BINARY", pb.DataType_BINARY},
		{"String", "JSON", pb.DataType_JSON},
		{"Nullable(String)", "JSON", pb.DataType_JSON},
	}
	for _, arg := range args {
		dataType, decimalParams, err := ToFivetranDataType(arg.Type, arg.Comment, nil)
		assert.NoError(t, err,
			"Error for CH type %s, comment %s, and Fivetran type %s should be nil",
			arg.Type, arg.Comment, arg.FivetranType.String())
		assert.Nil(t, decimalParams,
			"Decimal params for CH type %s, comment %s, and Fivetran type %s should be nil",
			arg.Type, arg.Comment, arg.FivetranType.String())
		assert.Equal(t, arg.FivetranType, dataType,
			"Fivetran type for CH type %s and comment %s should be %s",
			arg.Type, arg.Comment, arg.FivetranType.String())
	}
}
