package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func TestColumnTypesToEmptyRows(t *testing.T) {
	conn, err := GetClickHouseConnection(context.Background(), config)
	assert.NoError(t, err)
	defer conn.Close()

	tableName := fmt.Sprintf("test_empty_rows_gen_%s", strings.ReplaceAll(uuid.New().String(), "-", "_"))
	err = conn.Exec(context.Background(), GetDDL(tableName))
	assert.NoError(t, err)

	rows, err := conn.Query(context.Background(), fmt.Sprintf("SELECT * FROM %s WHERE false", tableName))
	assert.NoError(t, err)
	defer rows.Close()

	columnTypes := rows.ColumnTypes()
	emptyRows := ColumnTypesToEmptyRows(columnTypes, 10)
	assert.NoError(t, err)
	assert.Equal(t, 10, len(emptyRows))

	b := new(bool)
	i16 := new(int16)
	i32 := new(int32)
	i64 := new(int64)
	f32 := new(float32)
	f64 := new(float64)
	dec := new(decimal.Decimal)
	d := new(time.Time)
	s := new(string)
	j := new(map[string]interface{})

	/// Non-nullable is *Type, nullable is **Type (see ddl)
	for i, row := range emptyRows {
		assert.Equal(t, 23, len(row))

		// Boolean
		assert.IsType(t, b, row[0], "Expected idx 0 of row %d to be *bool", i)
		assert.IsType(t, &b, row[1], "Expected idx 1 of row %d to be **bool", i)

		// Int16
		assert.IsType(t, i16, row[2], "Expected idx 2 of row %d to be *int16", i)
		assert.IsType(t, &i16, row[3], "Expected idx 3 of row %d to be **int16", i)

		// Int32
		assert.IsType(t, i32, row[4], "Expected idx 4 of row %d to be *int32", i)
		assert.IsType(t, &i32, row[5], "Expected idx 5 of row %d to be **int32", i)

		// Int64
		assert.IsType(t, i64, row[6], "Expected idx 6 of row %d to be *int64", i)
		assert.IsType(t, &i64, row[7], "Expected idx 7 of row %d to be **int64", i)

		// Float32
		assert.IsType(t, f32, row[8], "Expected idx 8 of row %d to be *float32", i)
		assert.IsType(t, &f32, row[9], "Expected idx 9 of row %d to be **float32", i)

		// Float64
		assert.IsType(t, f64, row[10], "Expected idx 10 of row %d to be *float64", i)
		assert.IsType(t, &f64, row[11], "Expected idx 11 of row %d to be **float64", i)

		// Decimal
		assert.IsType(t, dec, row[12], "Expected idx 12 of row %d to be *decimal.Decimal", i)
		assert.IsType(t, &dec, row[13], "Expected idx 13 of row %d to be **decimal.Decimal", i)

		// Date
		assert.IsType(t, d, row[14], "Expected idx 14 of row %d to be *time.Time", i)
		assert.IsType(t, &d, row[15], "Expected idx 15 of row %d to be **time.Time", i)

		// DateTime
		assert.IsType(t, d, row[16], "Expected idx 16 of row %d to be *time.Time", i)
		assert.IsType(t, &d, row[17], "Expected idx 17 of row %d to be **time.Time", i)

		// DateTime64(9, 'UTC')
		assert.IsType(t, d, row[18], "Expected idx 18 of row %d to be *time.Time", i)
		assert.IsType(t, &d, row[19], "Expected idx 19 of row %d to be **time.Time", i)

		// String
		assert.IsType(t, s, row[20], "Expected idx 20 of row %d to be *string", i)
		assert.IsType(t, &s, row[21], "Expected idx 21 of row %d to be **string", i)

		// JSON is not nullable only
		assert.IsType(t, j, row[22], "Expected idx 22 of row %d to be *map[string]interface{}", i)
	}
}

func TestGetCSVRowMappingKey(t *testing.T) {
	row := CSVRow{"true", "false", "42", "100.5", "2021-03-04T22:44:22.123456789Z", "2023-05-07T18:22:44", "2019-12-15", "test"}

	// Serialization of a single PK to a mapping key
	singlePrimaryKeyArgs := []struct {
		*PrimaryKeyColumn
		string
	}{
		{&PrimaryKeyColumn{Index: 0}, "true"},
		{&PrimaryKeyColumn{Index: 1}, "false"},
		{&PrimaryKeyColumn{Index: 2}, "42"},
		{&PrimaryKeyColumn{Index: 3}, "100.5"},
		{&PrimaryKeyColumn{Index: 4}, "2021-03-04T22:44:22.123456789Z"},
		{&PrimaryKeyColumn{Index: 5}, "2023-05-07T18:22:44"},
		{&PrimaryKeyColumn{Index: 6}, "2019-12-15"},
		{&PrimaryKeyColumn{Index: 7}, "test"},
	}
	for i, arg := range singlePrimaryKeyArgs {
		key, err := GetCSVRowMappingKey(row, []*PrimaryKeyColumn{arg.PrimaryKeyColumn})
		assert.NoError(t, err, "Expected no error for idx %d with key %s", i, arg.string)
		assert.Equal(t, arg.string, key, "Expected key to be %s for idx %d", arg.string, i)
	}

	// Serialization of multiple PKs to a mapping key
	multiplePrimaryKeyArgs := []struct {
		pkCols []*PrimaryKeyColumn
		key    string
	}{
		{pkCols: []*PrimaryKeyColumn{{Index: 0}, {Index: 1}}, key: "true_false"},
		{pkCols: []*PrimaryKeyColumn{{Index: 2}, {Index: 3}}, key: "42_100.5"},
		{pkCols: []*PrimaryKeyColumn{{Index: 4}, {Index: 5}}, key: "2021-03-04T22:44:22.123456789Z_2023-05-07T18:22:44"},
		{pkCols: []*PrimaryKeyColumn{{Index: 6}, {Index: 7}}, key: "2019-12-15_test"},
	}
	for i, arg := range multiplePrimaryKeyArgs {
		key, err := GetCSVRowMappingKey(row, arg.pkCols)
		assert.NoError(t, err, "Expected no error for idx %d with key %s", i, arg.key)
		assert.Equal(t, arg.key, key, "Expected key to be %s for idx %d", arg.key, i)
	}

	_, err := GetCSVRowMappingKey(row, nil)
	assert.ErrorContains(t, err, "expected non-empty list of primary keys columns")
}

func TestGetDatabaseRowMappingKey(t *testing.T) {
	//row := CSVRow{
	//	"true", "42", "43", "44", "100.5", "200.5", "47.47",
	//	"2021-03-04T22:44:22.123456789Z", "2023-05-07T18:22:44", "2019-12-15",
	//	"test", "{\"foo\": \"bar\"}",
	//	"binaryStr", "xmlStr"}

	//{&PrimaryKeyColumn{Name: "b", Type: pb.DataType_BOOLEAN, Index: 0}, "true"},
	//{&PrimaryKeyColumn{Name: "i16", Type: pb.DataType_SHORT, Index: 1}, "42"},
	//{&PrimaryKeyColumn{Name: "i32", Type: pb.DataType_INT, Index: 2}, "43"},
	//{&PrimaryKeyColumn{Name: "i64", Type: pb.DataType_LONG, Index: 3}, "44"},
	//{&PrimaryKeyColumn{Name: "f32", Type: pb.DataType_FLOAT, Index: 4}, "100.5"},
	//{&PrimaryKeyColumn{Name: "f64", Type: pb.DataType_DOUBLE, Index: 5}, "200.5"},
	//{&PrimaryKeyColumn{Name: "dec", Type: pb.DataType_DECIMAL, Index: 6}, "47.47"},
	//{&PrimaryKeyColumn{Name: "utc_datetime", Type: pb.DataType_UTC_DATETIME, Index: 7}, "2021-03-04T22:44:22.123456789Z"},
	//{&PrimaryKeyColumn{Name: "naive_datetime", Type: pb.DataType_NAIVE_DATETIME, Index: 8}, "2023-05-07T18:22:44"},
	//{&PrimaryKeyColumn{Name: "naive_date", Type: pb.DataType_NAIVE_DATE, Index: 9}, "2019-12-15"},
	//{&PrimaryKeyColumn{Name: "str", Type: pb.DataType_STRING, Index: 10}, "test"},
	//{&PrimaryKeyColumn{Name: "json", Type: pb.DataType_JSON, Index: 11}, "{\"foo\": \"bar\"}"},
	//// Unclear CH mapping: may be removed
	//{&PrimaryKeyColumn{Name: "binary", Type: pb.DataType_BINARY, Index: 12}, "binaryStr"},
	//{&PrimaryKeyColumn{Name: "xml", Type: pb.DataType_XML, Index: 13}, "xmlStr"},
}

var config = map[string]string{
	"host":     "localhost",
	"port":     "9000",
	"username": "default",
	"password": "",
}

func GetDDL(tableName string) string {
	return fmt.Sprintf(`
		CREATE OR REPLACE TABLE %s (
			b Bool,
			nb Nullable(Bool),
			i16 Int16,
			ni16 Nullable(Int16),
			i32 Int32,
			ni32 Nullable(Int32),
			i64 Int64,
			ni64 Nullable(Int64),
			f32 Float32,
			nf32 Nullable(Float32),
			f64 Float64,
			nf64 Nullable(Float64),
			dd Decimal(4, 2),
			ndd Nullable(Decimal(4, 2)),
			d Date,
			nd Nullable(Date),
			dt DateTime,
			ndt Nullable(DateTime),
			dt64 DateTime64(9, 'UTC'),
			ndt64 Nullable(DateTime64(9, 'UTC')),
			s String,
			ns Nullable(String),
			j JSON,
		) ENGINE Memory`, tableName)
}
