package service

import (
	"testing"

	"fivetran.com/fivetran_sdk/destination/common/types"
	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/stretchr/testify/assert"
)

func TestToFivetranColumns(t *testing.T) {
	description := types.MakeTableDescription([]*types.ColumnDefinition{
		{Name: "b", Type: "Bool", IsPrimaryKey: true},
		{Name: "i16", Type: "Int16", IsPrimaryKey: false},
		{Name: "i32", Type: "Int32", IsPrimaryKey: false},
		{Name: "i64", Type: "Int64", IsPrimaryKey: false},
		{Name: "f32", Type: "Float32", IsPrimaryKey: false},
		{Name: "f64", Type: "Float64", IsPrimaryKey: false},
		{Name: "dec", Type: "Decimal(10, 4)", IsPrimaryKey: false, DecimalParams: &pb.DecimalParams{Precision: 10, Scale: 4}},
		{Name: "d", Type: "Date", IsPrimaryKey: false},
		{Name: "dt", Type: "DateTime", IsPrimaryKey: false},
		{Name: "dt_utc", Type: "DateTime64(9, 'UTC')", IsPrimaryKey: true},
		{Name: "str", Type: "String", IsPrimaryKey: false},
		{Name: "j", Type: "String", IsPrimaryKey: false, Comment: "JSON"},
		{Name: "x", Type: "String", IsPrimaryKey: false, Comment: "XML"},
		{Name: "bin", Type: "String", IsPrimaryKey: false, Comment: "BINARY"},
	})
	cols, err := ToFivetran(description)
	assert.NoError(t, err)
	assert.Equal(t, cols, []*pb.Column{
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

	cols, err = ToFivetran(nil)
	assert.NoError(t, err)
	assert.Equal(t, cols, []*pb.Column{})

	cols, err = ToFivetran(&types.TableDescription{})
	assert.NoError(t, err)
	assert.Equal(t, cols, []*pb.Column{})

	_, err = ToFivetran(&types.TableDescription{Columns: []*types.ColumnDefinition{{Name: "a", Type: "Array(String)"}}})
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
	boolCol := &types.ColumnDefinition{Name: "b", Type: "Bool", IsPrimaryKey: true}
	utcCol := &types.ColumnDefinition{Name: "dt_utc", Type: "DateTime64(9, 'UTC')", IsPrimaryKey: true}
	// The rest of the fields are nullable
	i16Col := &types.ColumnDefinition{Name: "i16", Type: "Nullable(Int16)", IsPrimaryKey: false}
	i32Col := &types.ColumnDefinition{Name: "i32", Type: "Nullable(Int32)", IsPrimaryKey: false}
	i64Col := &types.ColumnDefinition{Name: "i64", Type: "Nullable(Int64)", IsPrimaryKey: false}
	f32Col := &types.ColumnDefinition{Name: "f32", Type: "Nullable(Float32)", IsPrimaryKey: false}
	f64Col := &types.ColumnDefinition{Name: "f64", Type: "Nullable(Float64)", IsPrimaryKey: false}
	decimalCol := &types.ColumnDefinition{Name: "dec", Type: "Nullable(Decimal(10, 4))", IsPrimaryKey: false}
	dateCol := &types.ColumnDefinition{Name: "d", Type: "Nullable(Date)", IsPrimaryKey: false}
	datetimeCol := &types.ColumnDefinition{Name: "dt", Type: "Nullable(DateTime)", IsPrimaryKey: false}
	strCol := &types.ColumnDefinition{Name: "str", Type: "Nullable(String)", IsPrimaryKey: false}
	jsonCol := &types.ColumnDefinition{Name: "j", Type: "Nullable(String)", IsPrimaryKey: false, Comment: "JSON"}
	xmlCol := &types.ColumnDefinition{Name: "x", Type: "Nullable(String)", IsPrimaryKey: false, Comment: "XML"}
	binaryCol := &types.ColumnDefinition{Name: "bin", Type: "Nullable(String)", IsPrimaryKey: false, Comment: "BINARY"}

	description, err := ToClickHouse(table)
	assert.NoError(t, err)
	assert.Equal(t, description, &types.TableDescription{
		Mapping: map[string]*types.ColumnDefinition{
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
		Columns: []*types.ColumnDefinition{
			boolCol, i16Col, i32Col, i64Col,
			f32Col, f64Col, decimalCol,
			dateCol, datetimeCol, utcCol,
			strCol, jsonCol, xmlCol, binaryCol,
		},
		PrimaryKeys: []string{"b", "dt_utc"},
	})

	_, err = ToClickHouse(nil)
	assert.ErrorContains(t, err, "no columns in Fivetran table definition")

	_, err = ToClickHouse(&pb.Table{})
	assert.ErrorContains(t, err, "no columns in Fivetran table definition")

	_, err = ToClickHouse(&pb.Table{Columns: []*pb.Column{{Name: "a", Type: pb.DataType_UNSPECIFIED}}})
	assert.ErrorContains(t, err, "unknown datatype UNSPECIFIED")
}

func TestGetPrimaryKeysAndMetadataColumns(t *testing.T) {
	pkCols, err := GetPrimaryKeysAndMetadataColumns(&pb.Table{Columns: []*pb.Column{
		{Name: "i16", Type: pb.DataType_SHORT, PrimaryKey: false},
		{Name: "i32", Type: pb.DataType_INT, PrimaryKey: false},
		{Name: "str", Type: pb.DataType_STRING, PrimaryKey: true},
		{Name: "_fivetran_synced", Type: pb.DataType_UTC_DATETIME, PrimaryKey: false},
		{Name: "_fivetran_deleted", Type: pb.DataType_BOOLEAN, PrimaryKey: false},
	}})
	assert.NoError(t, err)
	assert.Equal(t, pkCols, &types.PrimaryKeysAndMetadataColumns{
		PrimaryKeys: []*types.PrimaryKeyColumn{
			{Index: 2, Name: "str", Type: pb.DataType_STRING},
		},
		FivetranSyncedIdx:  3,
		FivetranDeletedIdx: 4,
	})

	pkCols, err = GetPrimaryKeysAndMetadataColumns(nil)
	assert.ErrorContains(t, err, "no columns in Fivetran table definition")

	pkCols, err = GetPrimaryKeysAndMetadataColumns(&pb.Table{})
	assert.ErrorContains(t, err, "no columns in Fivetran table definition")

	pkCols, err = GetPrimaryKeysAndMetadataColumns(&pb.Table{Columns: []*pb.Column{}})
	assert.ErrorContains(t, err, "no columns in Fivetran table definition")

	pkCols, err = GetPrimaryKeysAndMetadataColumns(&pb.Table{Columns: []*pb.Column{
		{Name: "a", Type: pb.DataType_STRING},
	}})
	assert.ErrorContains(t, err, "no primary keys found")

	pkCols, err = GetPrimaryKeysAndMetadataColumns(&pb.Table{Columns: []*pb.Column{
		{Name: "a", Type: pb.DataType_STRING, PrimaryKey: true},
		{Name: "_fivetran_synced", Type: pb.DataType_UTC_DATETIME, PrimaryKey: false},
	}})
	assert.ErrorContains(t, err, "no _fivetran_deleted column found")

	pkCols, err = GetPrimaryKeysAndMetadataColumns(&pb.Table{Columns: []*pb.Column{
		{Name: "a", Type: pb.DataType_STRING, PrimaryKey: true},
		{Name: "_fivetran_deleted", Type: pb.DataType_BOOLEAN, PrimaryKey: false},
	}})
	assert.ErrorContains(t, err, "no _fivetran_synced column found")
}
