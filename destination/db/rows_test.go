package db

import (
	"testing"
	"time"

	"fivetran.com/fivetran_sdk/destination/common/types"
	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func TestGetCSVRowMappingKey(t *testing.T) {
	row := []string{"true", "false", "42", "100.5", "2021-03-04T22:44:22.123456789Z", "2023-05-07T18:22:44", "2019-12-15", "test"}

	// Serialization of a single PK to a mapping key (assuming one column is defined as a PK in Fivetran)
	singlePrimaryKeyArgs := []struct {
		col *types.CSVColumn
		key string
	}{
		{&types.CSVColumn{Name: "b1", Type: pb.DataType_BOOLEAN, Index: 0}, "b1:true"},
		{&types.CSVColumn{Name: "b2", Type: pb.DataType_BOOLEAN, Index: 1}, "b2:false"},
		{&types.CSVColumn{Name: "i32", Type: pb.DataType_INT, Index: 2}, "i32:42"},
		{&types.CSVColumn{Name: "f32", Type: pb.DataType_FLOAT, Index: 3}, "f32:100.5"},
		{&types.CSVColumn{Name: "dt_utc", Type: pb.DataType_UTC_DATETIME, Index: 4}, "dt_utc:1614897862123456789"},
		{&types.CSVColumn{Name: "dt", Type: pb.DataType_NAIVE_DATETIME, Index: 5}, "dt:2023-05-07T18:22:44"},
		{&types.CSVColumn{Name: "d", Type: pb.DataType_NAIVE_DATE, Index: 6}, "d:2019-12-15"},
		{&types.CSVColumn{Name: "s", Type: pb.DataType_STRING, Index: 7}, "s:test"},
	}
	for i, arg := range singlePrimaryKeyArgs {
		csvColumns := &types.CSVColumns{All: []*types.CSVColumn{arg.col}, PrimaryKeys: []*types.CSVColumn{arg.col}}
		key, err := GetCSVRowMappingKey(row, csvColumns, true)
		assert.NoError(t, err, "Expected no error for idx %d with key %s", i, arg.key)
		assert.Equal(t, arg.key, key, "Expected key to be %s for idx %d", arg.key, i)
	}

	// Serialization of multiple PKs to a mapping key (assuming two columns are defined as PKs in Fivetran)
	multiplePrimaryKeyArgs := []struct {
		csvCols []*types.CSVColumn
		key     string
	}{
		{csvCols: []*types.CSVColumn{
			{Name: "b1", Type: pb.DataType_BOOLEAN, Index: 0},
			{Name: "b2", Type: pb.DataType_BOOLEAN, Index: 1},
		}, key: "b1:true,b2:false"},
		{csvCols: []*types.CSVColumn{
			{Name: "i32", Type: pb.DataType_INT, Index: 2},
			{Name: "f32", Type: pb.DataType_FLOAT, Index: 3},
		}, key: "i32:42,f32:100.5"},
		{csvCols: []*types.CSVColumn{
			{Name: "dt_utc", Type: pb.DataType_UTC_DATETIME, Index: 4},
			{Name: "dt", Type: pb.DataType_NAIVE_DATETIME, Index: 5},
		}, key: "dt_utc:1614897862123456789,dt:2023-05-07T18:22:44"},
		{csvCols: []*types.CSVColumn{
			{Name: "d", Type: pb.DataType_NAIVE_DATE, Index: 6},
			{Name: "s", Type: pb.DataType_STRING, Index: 7},
		}, key: "d:2019-12-15,s:test"},
	}
	for i, arg := range multiplePrimaryKeyArgs {
		csvColumns := &types.CSVColumns{All: arg.csvCols, PrimaryKeys: arg.csvCols}
		key, err := GetCSVRowMappingKey(row, csvColumns, true)
		assert.NoError(t, err, "Expected no error for idx %d with key %s", i, arg.key)
		assert.Equal(t, arg.key, key, "Expected key to be %s for idx %d", arg.key, i)
	}

	_, err := GetCSVRowMappingKey(row, nil, true)
	assert.ErrorContains(t, err, "expected non-empty list of primary keys columns")
}

func TestGetCSVRowMappingKeyArbitraryUTCPrecision(t *testing.T) {
	args := []struct {
		row []string
		key string
	}{
		{row: []string{"42", "2021-03-04T22:44:22.123456789Z", "test"}, key: "dt_utc:1614897862123456789"},
		{row: []string{"42", "2021-03-04T22:44:22.123456Z", "test"}, key: "dt_utc:1614897862123456000"},
		{row: []string{"42", "2021-03-04T22:44:22.123Z", "test"}, key: "dt_utc:1614897862123000000"},
		{row: []string{"42", "2021-03-04T22:44:22Z", "test"}, key: "dt_utc:1614897862000000000"},
		{row: []string{"42", "2021-03-04T22:44:22.001Z", "test"}, key: "dt_utc:1614897862001000000"},
		{row: []string{"42", "2021-03-04T22:44:22.000001Z", "test"}, key: "dt_utc:1614897862000001000"},
		{row: []string{"42", "2021-03-04T22:44:22.000000001Z", "test"}, key: "dt_utc:1614897862000000001"},
		{row: []string{"42", "2021-03-04T22:44:22.100000000Z", "test"}, key: "dt_utc:1614897862100000000"},
	}
	csvCol := &types.CSVColumn{Name: "dt_utc", Type: pb.DataType_UTC_DATETIME, Index: 1}
	csvColumns := &types.CSVColumns{All: []*types.CSVColumn{csvCol}, PrimaryKeys: []*types.CSVColumn{csvCol}}
	for i, arg := range args {
		key, err := GetCSVRowMappingKey(arg.row, csvColumns, true)
		assert.NoError(t, err, "Expected no error for idx %d with key %s", i, arg.key)
		assert.Equal(t, arg.key, key, "Expected key to be %s for idx %d", arg.key, i)
	}
}

func TestToInsertRowValidation(t *testing.T) {
	_, err := ToInsertRow(nil, nil, "")
	assert.ErrorContains(t, err, "nullStr can't be empty")
	_, err = ToInsertRow(nil, nil, "foobar")
	assert.ErrorContains(t, err, "table can't be nil")

	csvColumns := &types.CSVColumns{
		All: []*types.CSVColumn{
			{Name: "id", Type: pb.DataType_LONG, Index: 0},
			{Name: "name", Type: pb.DataType_STRING, Index: 1}},
		PrimaryKeys: []*types.CSVColumn{
			{Name: "id", Type: pb.DataType_LONG, Index: 0}},
	}

	_, err = ToInsertRow(nil, csvColumns, "foobar")
	assert.ErrorContains(t, err, "expected 2 columns, but CSV row contains 0")
	_, err = ToInsertRow([]string{"1", "2", "3"}, csvColumns, "foobar")
	assert.ErrorContains(t, err, "expected 2 columns, but CSV row contains 3")
	_, err = ToInsertRow([]string{"foo", "2"}, csvColumns, "foobar")
	assert.ErrorContains(t, err, "can't parse value foo as int64 for column id")
}

func TestToInsertRow(t *testing.T) {
	colBoolean := &types.CSVColumn{Name: "boolean", Type: pb.DataType_BOOLEAN, Index: 0, TableIndex: 0}
	colShort := &types.CSVColumn{Name: "short", Type: pb.DataType_SHORT, Index: 1, TableIndex: 1}
	colInt := &types.CSVColumn{Name: "int", Type: pb.DataType_INT, Index: 2, TableIndex: 2}
	colLong := &types.CSVColumn{Name: "long", Type: pb.DataType_LONG, Index: 3, TableIndex: 3}
	colFloat := &types.CSVColumn{Name: "float", Type: pb.DataType_FLOAT, Index: 4, TableIndex: 4}
	colDouble := &types.CSVColumn{Name: "double", Type: pb.DataType_DOUBLE, Index: 5, TableIndex: 5}
	colDecimal := &types.CSVColumn{Name: "decimal", Type: pb.DataType_DECIMAL, Index: 6, TableIndex: 6}
	colUTCDateTimeNanos := &types.CSVColumn{Name: "utc_datetime_nanos", Type: pb.DataType_UTC_DATETIME, Index: 7, TableIndex: 7}
	colUTCDateTimeMicros := &types.CSVColumn{Name: "utc_datetime_micros", Type: pb.DataType_UTC_DATETIME, Index: 8, TableIndex: 8}
	colUTCDateTimeMillis := &types.CSVColumn{Name: "utc_datetime_millis", Type: pb.DataType_UTC_DATETIME, Index: 9, TableIndex: 9}
	colUTCDateTime := &types.CSVColumn{Name: "utc_datetime", Type: pb.DataType_UTC_DATETIME, Index: 10, TableIndex: 10}
	colNaiveDatetime := &types.CSVColumn{Name: "naive_datetime", Type: pb.DataType_NAIVE_DATETIME, Index: 11, TableIndex: 11}
	colNaiveDate := &types.CSVColumn{Name: "naive_date", Type: pb.DataType_NAIVE_DATE, Index: 12, TableIndex: 12}
	colStr := &types.CSVColumn{Name: "str", Type: pb.DataType_STRING, Index: 13, TableIndex: 13}
	colJSON := &types.CSVColumn{Name: "json", Type: pb.DataType_JSON, Index: 14, TableIndex: 14}
	colBinary := &types.CSVColumn{Name: "binary", Type: pb.DataType_BINARY, Index: 15, TableIndex: 15}
	colXML := &types.CSVColumn{Name: "xml", Type: pb.DataType_XML, Index: 16, TableIndex: 16}
	csvCols := &types.CSVColumns{
		All: []*types.CSVColumn{
			colBoolean, colShort, colInt, colLong, colFloat, colDouble, colDecimal,
			colUTCDateTimeNanos, colUTCDateTimeMicros, colUTCDateTimeMillis,
			colUTCDateTime, colNaiveDatetime, colNaiveDate,
			colStr, colJSON, colBinary, colXML},
		PrimaryKeys: []*types.CSVColumn{colLong},
	}

	row, err := ToInsertRow([]string{
		"false", "42", "43", "44", "100.5", "200.55", "47.47",
		"2022-03-05T04:45:12.123456789Z",
		"2022-03-05T04:45:12.123456Z",
		"2022-03-05T04:45:12.123Z",
		"2022-03-05T04:45:12Z",
		"2022-03-05T04:45:11",
		"2022-03-05",
		"foo", "{\"foo\": \"bar\"}", "0x42", "<foo>bar</foo>",
	}, csvCols, "my-null-str")
	assert.NoError(t, err)
	assert.Equal(t, []any{
		false, int16(42), int32(43), int64(44), float64(100.5), float64(200.55), decimal.NewFromFloat32(47.47),
		time.Date(2022, 3, 5, 4, 45, 12, 123456789, time.UTC),
		time.Date(2022, 3, 5, 4, 45, 12, 123456000, time.UTC),
		time.Date(2022, 3, 5, 4, 45, 12, 123000000, time.UTC),
		time.Date(2022, 3, 5, 4, 45, 12, 0, time.UTC),
		time.Date(2022, 3, 5, 4, 45, 11, 0, time.UTC),
		time.Date(2022, 3, 5, 0, 0, 0, 0, time.UTC),
		"foo", "{\"foo\": \"bar\"}", "0x42", "<foo>bar</foo>",
	}, row)

	row, err = ToInsertRow([]string{
		"true", "32767", "2147483647", "9223372036854775807", "-100.5", "-200.55", "-100.55",
		"2023-05-06T02:12:15.023456789Z",
		"2023-05-06T02:12:15.023456Z",
		"2023-05-06T02:12:15.023Z",
		"2023-05-06T02:12:15Z",
		"2023-05-06T02:12:14",
		"2023-05-06",
		"qaz", "{\"qaz\": \"qux\"}", "0xFF", "<x/>",
	}, csvCols, "my-null-str")
	assert.NoError(t, err)
	assert.Equal(t, []any{
		true, int16(32767), int32(2147483647), int64(9223372036854775807), float64(-100.5), float64(-200.55), decimal.NewFromFloat32(-100.55),
		time.Date(2023, 5, 6, 2, 12, 15, 23456789, time.UTC),
		time.Date(2023, 5, 6, 2, 12, 15, 23456000, time.UTC),
		time.Date(2023, 5, 6, 2, 12, 15, 23000000, time.UTC),
		time.Date(2023, 5, 6, 2, 12, 15, 0, time.UTC),
		time.Date(2023, 5, 6, 2, 12, 14, 0, time.UTC),
		time.Date(2023, 5, 6, 0, 0, 0, 0, time.UTC),
		"qaz", "{\"qaz\": \"qux\"}", "0xFF", "<x/>",
	}, row)
}

func TestToInsertRowNullStr(t *testing.T) {
	colID := &types.CSVColumn{Name: "id", Type: pb.DataType_LONG, Index: 0, TableIndex: 0}
	colName := &types.CSVColumn{Name: "name", Type: pb.DataType_STRING, Index: 1, TableIndex: 1}
	colIsDeleted := &types.CSVColumn{Name: "is_deleted", Type: pb.DataType_BOOLEAN, Index: 2, TableIndex: 2}
	colSomeJSONField := &types.CSVColumn{Name: "some_json_field", Type: pb.DataType_JSON, Index: 3, TableIndex: 3}
	csvCols := &types.CSVColumns{
		All:         []*types.CSVColumn{colID, colName, colIsDeleted, colSomeJSONField},
		PrimaryKeys: []*types.CSVColumn{colID},
	}

	row, err := ToInsertRow([]string{
		"my-null-str", "foo", "true", `{"foo": "bar"}`,
	}, csvCols, "my-null-str")
	assert.Equal(t, []any{nil, "foo", true, `{"foo": "bar"}`}, row)
	assert.NoError(t, err)

	row, err = ToInsertRow([]string{
		"42", "my-null-str", "false", "my-null-str",
	}, csvCols, "my-null-str")
	assert.NoError(t, err)
	assert.Equal(t, []any{int64(42), nil, false, nil}, row)

	row, err = ToInsertRow([]string{
		"43", "bar", "my-null-str", `{"foo": "bar"}`,
	}, csvCols, "my-null-str")
	assert.NoError(t, err)
	assert.Equal(t, []any{int64(43), "bar", nil, `{"foo": "bar"}`}, row)
}

func TestToUpdatedRowValidation(t *testing.T) {
	_, err := ToUpdatedRow(nil, nil, nil, "", "")
	assert.ErrorContains(t, err, "unmodifiedStr can't be empty")
	_, err = ToUpdatedRow(nil, nil, nil, "", "bar")
	assert.ErrorContains(t, err, "nullStr can't be empty")
	_, err = ToUpdatedRow(nil, nil, nil, "foo", "bar")
	assert.ErrorContains(t, err, "CSV columns can't be empty")

	colID := &types.CSVColumn{Name: "id", Type: pb.DataType_LONG, Index: 0, TableIndex: 0}
	colName := &types.CSVColumn{Name: "name", Type: pb.DataType_STRING, Index: 1, TableIndex: 1}
	csvCols := &types.CSVColumns{
		All:         []*types.CSVColumn{colID, colName},
		PrimaryKeys: []*types.CSVColumn{colID},
	}

	_, err = ToUpdatedRow(nil, nil, csvCols, "foo", "bar")
	assert.ErrorContains(t, err, "expected CSV, table definition and ClickHouse row to contain the same number of columns, but got 0, 2 and 0")
	_, err = ToUpdatedRow(nil, []any{int64(42), "foo"}, csvCols, "foo", "bar")
	assert.ErrorContains(t, err, "expected CSV, table definition and ClickHouse row to contain the same number of columns, but got 0, 2 and 2")
	// CSV columns count mismatch
	_, err = ToUpdatedRow([]string{"42"}, []any{int64(42), "foo"}, csvCols, "foo", "bar")
	assert.ErrorContains(t, err, "expected CSV, table definition and ClickHouse row to contain the same number of columns, but got 1, 2 and 2")
	// DB columns count mismatch
	_, err = ToUpdatedRow([]string{"42", "bar"}, []any{int64(42)}, csvCols, "foo", "bar")
	assert.ErrorContains(t, err, "expected CSV, table definition and ClickHouse row to contain the same number of columns, but got 2, 2 and 1")
	// Table definition columns count mismatch
	_, err = ToUpdatedRow([]string{"42", "bar"}, []any{int64(42), "foo"}, &types.CSVColumns{
		All:         []*types.CSVColumn{colID},
		PrimaryKeys: []*types.CSVColumn{colID},
	}, "foo", "bar")
	assert.ErrorContains(t, err, "expected CSV, table definition and ClickHouse row to contain the same number of columns, but got 2, 1 and 2")
	_, err = ToUpdatedRow([]string{"qaz", "qux"}, []any{int64(42), "foo"}, csvCols, "foo", "bar")
	assert.ErrorContains(t, err, "can't parse value qaz as int64 for column id")
}

func TestToUpdatedRow(t *testing.T) {
	colID := &types.CSVColumn{Name: "id", Type: pb.DataType_LONG, Index: 0, TableIndex: 0}
	colName := &types.CSVColumn{Name: "name", Type: pb.DataType_STRING, Index: 1, TableIndex: 1}
	colFivetranSynced := &types.CSVColumn{Name: "_fivetran_synced", Type: pb.DataType_UTC_DATETIME, Index: 2, TableIndex: 2}
	colFivetranDeleted := &types.CSVColumn{Name: "_fivetran_deleted", Type: pb.DataType_BOOLEAN, Index: 3, TableIndex: 3}
	csvCols := &types.CSVColumns{
		All:         []*types.CSVColumn{colID, colName, colFivetranSynced, colFivetranDeleted},
		PrimaryKeys: []*types.CSVColumn{colID},
	}

	dbRow := []any{int64(42), "foo", time.Date(2023, 5, 6, 2, 12, 15, 234567890, time.UTC), false}

	// nanoseconds precision _fivetran_deleted
	row, err := ToUpdatedRow(
		[]string{"43", "bar", "2024-03-05T04:45:12.123456789Z", "true"},
		dbRow, csvCols, "my-null-str", "my-unmodified-str")
	assert.NoError(t, err)
	assert.Equal(t, []any{int64(43), "bar", time.Date(2024, 3, 5, 4, 45, 12, 123456789, time.UTC), true}, row)

	// microseconds
	row, err = ToUpdatedRow(
		[]string{"43", "bar", "2024-03-05T04:45:12.123456Z", "true"},
		dbRow, csvCols, "my-null-str", "my-unmodified-str")
	assert.NoError(t, err)
	assert.Equal(t, []any{int64(43), "bar", time.Date(2024, 3, 5, 4, 45, 12, 123456000, time.UTC), true}, row)

	// milliseconds
	row, err = ToUpdatedRow(
		[]string{"43", "bar", "2024-03-05T04:45:12.123Z", "true"},
		dbRow, csvCols, "my-null-str", "my-unmodified-str")
	assert.NoError(t, err)
	assert.Equal(t, []any{int64(43), "bar", time.Date(2024, 3, 5, 4, 45, 12, 123000000, time.UTC), true}, row)

	// no precision
	row, err = ToUpdatedRow(
		[]string{"43", "bar", "2024-03-05T04:45:12Z", "true"},
		dbRow, csvCols, "my-null-str", "my-unmodified-str")
	assert.NoError(t, err)
	assert.Equal(t, []any{int64(43), "bar", time.Date(2024, 3, 5, 4, 45, 12, 0, time.UTC), true}, row)

	row, err = ToUpdatedRow(
		[]string{"my-unmodified-str", "my-null-str", "my-unmodified-str", "my-unmodified-str"},
		dbRow, csvCols, "my-null-str", "my-unmodified-str")
	assert.NoError(t, err)
	assert.Equal(t, []any{int64(42), nil, time.Date(2023, 5, 6, 2, 12, 15, 234567890, time.UTC), false}, row)

	row, err = ToUpdatedRow(
		[]string{"my-null-str", "my-unmodified-str", "my-null-str", "my-null-str"},
		dbRow, csvCols, "my-null-str", "my-unmodified-str")
	assert.NoError(t, err)
	assert.Equal(t, []any{nil, "foo", nil, nil}, row)
}

func TestToSoftDeletedRowValidation(t *testing.T) {
	csvRow := []string{"null", "null", "2022-03-05T04:45:12.123456789Z", "false"}
	_, err := ToSoftDeletedRow(csvRow, nil, 1, 4)
	assert.ErrorContains(t, err, "can't find column _fivetran_deleted with index 4 in a CSV row")
	_, err = ToSoftDeletedRow(csvRow, nil, 5, 3)
	assert.ErrorContains(t, err, "can't find column _fivetran_synced with index 5 in a CSV row")
	_, err = ToSoftDeletedRow(csvRow, nil, 2, 3)
	assert.ErrorContains(t, err, "expected ClickHouse row to contain at least 2 columns, but got 0")
	_, err = ToSoftDeletedRow(csvRow, []any{int64(42)}, 2, 3)
	assert.ErrorContains(t, err, "expected ClickHouse row to contain at least 2 columns, but got 1")
}

func TestToSoftDeletedRow(t *testing.T) {
	dbRow := []any{int64(42), "foo", time.Now(), false}

	// nanoseconds precision _fivetran_deleted
	row, err := ToSoftDeletedRow(
		[]string{"null", "null", "2022-03-05T04:45:12.123456789Z", "false"},
		dbRow, 2, 3)
	assert.NoError(t, err)
	assert.Equal(t, []any{int64(42), "foo", time.Date(2022, 3, 5, 4, 45, 12, 123456789, time.UTC), true}, row)

	// microseconds
	row, err = ToSoftDeletedRow(
		[]string{"null", "null", "2022-03-05T04:45:12.123456Z", "false"},
		dbRow, 2, 3)
	assert.NoError(t, err)
	assert.Equal(t, []any{int64(42), "foo", time.Date(2022, 3, 5, 4, 45, 12, 123456000, time.UTC), true}, row)

	// milliseconds
	row, err = ToSoftDeletedRow(
		[]string{"null", "null", "2022-03-05T04:45:12.123Z", "false"},
		dbRow, 2, 3)
	assert.NoError(t, err)
	assert.Equal(t, []any{int64(42), "foo", time.Date(2022, 3, 5, 4, 45, 12, 123000000, time.UTC), true}, row)

	// no precision
	row, err = ToSoftDeletedRow(
		[]string{"null", "null", "2022-03-05T04:45:12Z", "false"},
		dbRow, 2, 3)
	assert.NoError(t, err)
	assert.Equal(t, []any{int64(42), "foo", time.Date(2022, 3, 5, 4, 45, 12, 0, time.UTC), true}, row)

	_, err = ToSoftDeletedRow(
		[]string{"null", "null", "2022-03-05T04:45:12.123456789Z", "false"},
		dbRow, 3, 3) // <- _fivetran_synced points to a Boolean column
	assert.ErrorContains(t, err, "can't parse value false as UTC datetime for column _fivetran_synced")
}
