package db

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"fivetran.com/fivetran_sdk/destination/common/types"
	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestColumnTypesToEmptyRows(t *testing.T) {
	conn, err := GetClickHouseConnection(map[string]string{
		"host":     "localhost:9000",
		"username": "default",
		"local":    "true",
	})
	require.NoError(t, err)
	defer conn.Close()

	tableName := fmt.Sprintf("test_empty_rows_gen_%s", strings.ReplaceAll(uuid.New().String(), "-", "_"))
	err = conn.Exec(context.Background(), fmt.Sprintf(`
		CREATE OR REPLACE TABLE %s (
			b     Bool,
			nb    Nullable(Bool),
			i16   Int16,
			ni16  Nullable(Int16),
			i32   Int32,
			ni32  Nullable(Int32),
			i64   Int64,
			ni64  Nullable(Int64),
			f32   Float32,
			nf32  Nullable(Float32),
			f64   Float64,
			nf64  Nullable(Float64),
			dd    Decimal(4, 2),
			ndd   Nullable(Decimal(4, 2)),
			d     Date,
			nd    Nullable(Date),
			dt    DateTime,
			ndt   Nullable(DateTime),
			dt64  DateTime64(9, 'UTC'),
			ndt64 Nullable(DateTime64(9, 'UTC')),
			s     String,
			ns    Nullable(String)
		) ENGINE Memory`, tableName))
	assert.NoError(t, err)

	rows, err := conn.Query(context.Background(), fmt.Sprintf("SELECT * FROM %s WHERE false", tableName))
	assert.NoError(t, err)
	defer rows.Close()

	columnTypes := rows.ColumnTypes()
	emptyRows := ColumnTypesToEmptyScanRows(columnTypes, 10)
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

	/// Non-nullable is *Type, nullable is **Type (see ddl)
	for i, row := range emptyRows {
		assert.Equal(t, 22, len(row))

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
	}
}

func TestGetDatabaseRowMappingKey(t *testing.T) {
	conn, err := GetClickHouseConnection(map[string]string{
		"host":     "localhost:9000",
		"username": "default",
		"local":    "true",
	})
	require.NoError(t, err)
	defer conn.Close()

	// Create a table with all possible destination types and a single record
	tableName := fmt.Sprintf("test_get_db_row_key_%s", strings.ReplaceAll(uuid.New().String(), "-", "_"))
	// dt64_nanos/micros/millis does not refer to the precision of the type itself, but to the contents
	err = conn.Exec(context.Background(), fmt.Sprintf(`
		CREATE OR REPLACE TABLE %s (
			b           Bool,
			i16         Int16,
			i32         Int32,
			i64         Int64,
			f32         Float32,
			f64         Float64,
			dd          Decimal(4, 2),
			dt64_nanos  DateTime64(9, 'UTC'),
			dt64_micros DateTime64(9, 'UTC'),
			dt64_millis DateTime64(9, 'UTC'),
			dt64        DateTime64(9, 'UTC'),
			dt          DateTime,
			d           Date,
			s           String
		) ENGINE Memory`, tableName))
	assert.NoError(t, err)

	batch, err := conn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s", tableName))
	assert.NoError(t, err)
	err = batch.Append(
		true,
		int16(42),
		int32(43),
		int64(44),
		float32(100.5),
		float64(200.5),
		decimal.NewFromFloat(47.47),
		time.Date(2021, 3, 4, 22, 44, 22, 123456789, time.UTC),
		time.Date(2021, 3, 4, 22, 44, 22, 123456000, time.UTC),
		time.Date(2021, 3, 4, 22, 44, 22, 123000000, time.UTC),
		time.Date(2021, 3, 4, 22, 44, 22, 0, time.UTC),
		time.Date(2023, 5, 7, 18, 22, 44, 0, time.UTC),
		time.Date(2019, 12, 15, 0, 0, 0, 0, time.UTC),
		"test",
	)
	assert.NoError(t, err)
	err = batch.Send()
	assert.NoError(t, err)

	// Introspect the table definition and create a single empty "proto" row to scan into
	rows, err := conn.Query(context.Background(), fmt.Sprintf("SELECT * FROM %s WHERE false", tableName))
	assert.NoError(t, err)

	columnTypes := rows.ColumnTypes()
	dbRow := ColumnTypesToEmptyScanRows(columnTypes, 1)[0]
	rows.Close()

	// Scan into that "proto" row
	rows, err = conn.Query(context.Background(), fmt.Sprintf("SELECT * FROM %s LIMIT 1", tableName))
	assert.NoError(t, err)
	rows.Next()
	err = rows.Scan(dbRow...)
	assert.NoError(t, err)

	// Serialization of a single PK to a mapping key (assuming one column is defined as a PK in Fivetran)
	singlePrimaryKeyArgs := []struct {
		*types.PrimaryKeyColumn
		string
	}{
		{&types.PrimaryKeyColumn{Name: "b", Type: pb.DataType_BOOLEAN, Index: 0}, "b:true"},
		{&types.PrimaryKeyColumn{Name: "i16", Type: pb.DataType_SHORT, Index: 1}, "i16:42"},
		{&types.PrimaryKeyColumn{Name: "i32", Type: pb.DataType_INT, Index: 2}, "i32:43"},
		{&types.PrimaryKeyColumn{Name: "i64", Type: pb.DataType_LONG, Index: 3}, "i64:44"},
		{&types.PrimaryKeyColumn{Name: "f32", Type: pb.DataType_FLOAT, Index: 4}, "f32:100.5"},
		{&types.PrimaryKeyColumn{Name: "f64", Type: pb.DataType_DOUBLE, Index: 5}, "f64:200.5"},
		{&types.PrimaryKeyColumn{Name: "dec", Type: pb.DataType_DECIMAL, Index: 6}, "dec:47.47"},
		{&types.PrimaryKeyColumn{Name: "utc_datetime_nanos", Type: pb.DataType_UTC_DATETIME, Index: 7}, "utc_datetime_nanos:1614897862123456789"},
		{&types.PrimaryKeyColumn{Name: "utc_datetime_micros", Type: pb.DataType_UTC_DATETIME, Index: 8}, "utc_datetime_micros:1614897862123456000"},
		{&types.PrimaryKeyColumn{Name: "utc_datetime_millis", Type: pb.DataType_UTC_DATETIME, Index: 9}, "utc_datetime_millis:1614897862123000000"},
		{&types.PrimaryKeyColumn{Name: "utc_datetime", Type: pb.DataType_UTC_DATETIME, Index: 10}, "utc_datetime:1614897862000000000"},
		{&types.PrimaryKeyColumn{Name: "naive_datetime", Type: pb.DataType_NAIVE_DATETIME, Index: 11}, "naive_datetime:2023-05-07T18:22:44"},
		{&types.PrimaryKeyColumn{Name: "naive_date", Type: pb.DataType_NAIVE_DATE, Index: 12}, "naive_date:2019-12-15"},
		{&types.PrimaryKeyColumn{Name: "str", Type: pb.DataType_STRING, Index: 13}, "str:test"},
	}
	for _, arg := range singlePrimaryKeyArgs {
		key, err := GetDatabaseRowMappingKey(dbRow, []*types.PrimaryKeyColumn{arg.PrimaryKeyColumn})
		assert.NoError(t, err, "Expected no error for key %s", arg.string)
		assert.Equal(t, arg.string, key, "Expected key to be %s", arg.string)
	}

	// Serialization of multiple PKs to a mapping key (assuming two columns are defined as PKs in Fivetran)
	multiplePrimaryKeyArgs := []struct {
		pkCols []*types.PrimaryKeyColumn
		key    string
	}{
		{pkCols: []*types.PrimaryKeyColumn{
			{Name: "b", Type: pb.DataType_BOOLEAN, Index: 0},
			{Name: "i16", Type: pb.DataType_SHORT, Index: 1},
		}, key: "b:true,i16:42"},
		{pkCols: []*types.PrimaryKeyColumn{
			{Name: "i32", Type: pb.DataType_INT, Index: 2},
			{Name: "i64", Type: pb.DataType_LONG, Index: 3},
		}, key: "i32:43,i64:44"},
		{pkCols: []*types.PrimaryKeyColumn{
			{Name: "f32", Type: pb.DataType_FLOAT, Index: 4},
			{Name: "f64", Type: pb.DataType_DOUBLE, Index: 5},
		}, key: "f32:100.5,f64:200.5"},
		{pkCols: []*types.PrimaryKeyColumn{
			{Name: "dec", Type: pb.DataType_DECIMAL, Index: 6},
			{Name: "utc_datetime", Type: pb.DataType_UTC_DATETIME, Index: 7},
		}, key: "dec:47.47,utc_datetime:1614897862123456789"},
		{pkCols: []*types.PrimaryKeyColumn{
			{Name: "utc_datetime_nanos", Type: pb.DataType_UTC_DATETIME, Index: 7},
			{Name: "utc_datetime_micros", Type: pb.DataType_UTC_DATETIME, Index: 8},
			{Name: "utc_datetime_millis", Type: pb.DataType_UTC_DATETIME, Index: 9},
		}, key: "utc_datetime_nanos:1614897862123456789,utc_datetime_micros:1614897862123456000,utc_datetime_millis:1614897862123000000"},
		{pkCols: []*types.PrimaryKeyColumn{
			{Name: "naive_datetime", Type: pb.DataType_NAIVE_DATETIME, Index: 11},
			{Name: "naive_date", Type: pb.DataType_NAIVE_DATE, Index: 12},
			{Name: "str", Type: pb.DataType_STRING, Index: 13},
		}, key: "naive_datetime:2023-05-07T18:22:44,naive_date:2019-12-15,str:test"},
	}
	for i, arg := range multiplePrimaryKeyArgs {
		key, err := GetDatabaseRowMappingKey(dbRow, arg.pkCols)
		assert.NoError(t, err, "Expected no error for idx %d with key %s", i, arg.key)
		assert.Equal(t, arg.key, key, "Expected key to be %s for idx %d", arg.key, i)
	}

	_, err = GetDatabaseRowMappingKey(dbRow, nil)
	assert.ErrorContains(t, err, "expected non-empty list of primary keys columns")
}
