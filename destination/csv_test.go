package main

import (
	"testing"
	"time"

	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func TestQuoteValue(t *testing.T) {
	assert.Equal(t, QuoteValue(pb.DataType_STRING, "foobar"), "'foobar'")
	assert.Equal(t, QuoteValue(pb.DataType_XML, "<foo>bar</foo>"), "'<foo>bar</foo>'")
	assert.Equal(t, QuoteValue(pb.DataType_BINARY, "0x42"), "'0x42'")
	assert.Equal(t, QuoteValue(pb.DataType_JSON, "{\"foo\": \"bar\"}"), "'{\"foo\": \"bar\"}'")
	assert.Equal(t, QuoteValue(pb.DataType_SHORT, "42"), "42")
	assert.Equal(t, QuoteValue(pb.DataType_INT, "42"), "42")
	assert.Equal(t, QuoteValue(pb.DataType_LONG, "42"), "42")
	assert.Equal(t, QuoteValue(pb.DataType_FLOAT, "42.42"), "42.42")
	assert.Equal(t, QuoteValue(pb.DataType_DOUBLE, "42.4242"), "42.4242")
	assert.Equal(t, QuoteValue(pb.DataType_DECIMAL, "42.424242"), "42.424242")
	assert.Equal(t, QuoteValue(pb.DataType_BOOLEAN, "true"), "true")
	assert.Equal(t, QuoteValue(pb.DataType_NAIVE_DATE, "2022-03-05"), "'2022-03-05'")
	assert.Equal(t, QuoteValue(pb.DataType_NAIVE_DATETIME, "2022-03-05T04:45:12"), "'2022-03-05T04:45:12'")
	assert.Equal(t, QuoteValue(pb.DataType_UTC_DATETIME, "2022-03-05T04:45:12.123456789Z"), "'2022-03-05T04:45:12.123456789Z'")
}

func TestParseValue(t *testing.T) {
	// Boolean
	val, err := ParseValue("test", pb.DataType_BOOLEAN, "true")
	assert.NoError(t, err)
	assert.Equal(t, true, val)
	val, err = ParseValue("test", pb.DataType_BOOLEAN, "false")
	assert.NoError(t, err)
	assert.Equal(t, false, val)

	_, err = ParseValue("test", pb.DataType_BOOLEAN, "x")
	assert.ErrorContains(t, err, "can't parse value x as boolean for column test")

	// Int16
	val, err = ParseValue("test", pb.DataType_SHORT, "42")
	assert.NoError(t, err)
	assert.Equal(t, int16(42), val)
	val, err = ParseValue("test", pb.DataType_SHORT, "-32768")
	assert.NoError(t, err)
	assert.Equal(t, int16(-32768), val)
	val, err = ParseValue("test", pb.DataType_SHORT, "32767")
	assert.NoError(t, err)
	assert.Equal(t, int16(32767), val)

	_, err = ParseValue("test", pb.DataType_SHORT, "x")
	assert.ErrorContains(t, err, "can't parse value x as int16 for column test")

	// Int32
	val, err = ParseValue("test", pb.DataType_INT, "42")
	assert.NoError(t, err)
	assert.Equal(t, int32(42), val)
	val, err = ParseValue("test", pb.DataType_INT, "-2147483648")
	assert.NoError(t, err)
	assert.Equal(t, int32(-2147483648), val)
	val, err = ParseValue("test", pb.DataType_INT, "2147483647")
	assert.NoError(t, err)
	assert.Equal(t, int32(2147483647), val)

	_, err = ParseValue("test", pb.DataType_INT, "x")
	assert.ErrorContains(t, err, "can't parse value x as int32 for column test")

	// Int64
	val, err = ParseValue("test", pb.DataType_LONG, "42")
	assert.NoError(t, err)
	assert.Equal(t, int64(42), val)
	val, err = ParseValue("test", pb.DataType_LONG, "-9223372036854775808")
	assert.NoError(t, err)
	assert.Equal(t, int64(-9223372036854775808), val)
	val, err = ParseValue("test", pb.DataType_LONG, "9223372036854775807")
	assert.NoError(t, err)
	assert.Equal(t, int64(9223372036854775807), val)

	_, err = ParseValue("test", pb.DataType_LONG, "x")
	assert.ErrorContains(t, err, "can't parse value x as int64 for column test")

	// Float32/64 (always parsed as float64)
	val, err = ParseValue("test", pb.DataType_FLOAT, "100.5")
	assert.NoError(t, err)
	assert.Equal(t, float64(100.5), val)

	_, err = ParseValue("test", pb.DataType_FLOAT, "x")
	assert.ErrorContains(t, err, "can't parse value x as float32 for column test")

	val, err = ParseValue("test", pb.DataType_DOUBLE, "200.55")
	assert.NoError(t, err)
	assert.Equal(t, float64(200.55), val)

	_, err = ParseValue("test", pb.DataType_DOUBLE, "x")
	assert.ErrorContains(t, err, "can't parse value x as float64 for column test")

	// Decimal
	val, err = ParseValue("test", pb.DataType_DECIMAL, "47.47")
	assert.NoError(t, err)
	assert.Equal(t, decimal.NewFromFloat32(47.47), val)

	_, err = ParseValue("test", pb.DataType_DECIMAL, "x")
	assert.ErrorContains(t, err, "can't parse value x as decimal for column test")

	// Date types
	val, err = ParseValue("test", pb.DataType_UTC_DATETIME, "2022-03-05T04:45:12.123456789Z")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2022, 3, 5, 4, 45, 12, 123456789, time.UTC), val)

	_, err = ParseValue("test", pb.DataType_UTC_DATETIME, "x")
	assert.ErrorContains(t, err, "can't parse value x as UTC datetime for column test")

	val, err = ParseValue("test", pb.DataType_NAIVE_DATETIME, "2022-03-05T04:45:12")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2022, 3, 5, 4, 45, 12, 0, time.UTC), val)

	_, err = ParseValue("test", pb.DataType_NAIVE_DATETIME, "x")
	assert.ErrorContains(t, err, "can't parse value x as naive datetime for column test")

	val, err = ParseValue("test", pb.DataType_NAIVE_DATE, "2022-03-05")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2022, 3, 5, 0, 0, 0, 0, time.UTC), val)

	_, err = ParseValue("test", pb.DataType_NAIVE_DATE, "x")
	assert.ErrorContains(t, err, "can't parse value x as naive date for column test")

	// String types
	val, err = ParseValue("test", pb.DataType_STRING, "foobar")
	assert.NoError(t, err)
	assert.Equal(t, "foobar", val)
	val, err = ParseValue("test", pb.DataType_JSON, "{\"foo\": \"bar\"}")
	assert.NoError(t, err)
	assert.Equal(t, "{\"foo\": \"bar\"}", val)

	// Unclear CH mapping: may be removed, currently just mapped as String
	val, err = ParseValue("test", pb.DataType_XML, "foobar")
	assert.NoError(t, err)
	assert.Equal(t, "foobar", val)
	val, err = ParseValue("test", pb.DataType_BINARY, "foobar")
	assert.NoError(t, err)
	assert.Equal(t, "foobar", val)

	// Unspecified
	_, err = ParseValue("test", pb.DataType_UNSPECIFIED, "foobar")
	assert.ErrorContains(t, err, "no target type for column test with type UNSPECIFIED")
}

func TestCSVRowToInsertValuesValidation(t *testing.T) {
	_, err := CSVRowToInsertValues(nil, nil, "")
	assert.ErrorContains(t, err, "nullStr can't be empty")
	_, err = CSVRowToInsertValues(nil, nil, "foobar")
	assert.ErrorContains(t, err, "table can't be nil")

	var table = &pb.Table{
		Name: "test_table",
		Columns: []*pb.Column{
			{Name: "id", Type: pb.DataType_LONG},
			{Name: "name", Type: pb.DataType_STRING},
		},
	}

	_, err = CSVRowToInsertValues(nil, table, "foobar")
	assert.ErrorContains(t, err, "expected 2 columns, but row contains 0")
	_, err = CSVRowToInsertValues([]string{"1", "2", "3"}, table, "foobar")
	assert.ErrorContains(t, err, "expected 2 columns, but row contains 3")
	_, err = CSVRowToInsertValues([]string{"foo", "2"}, table, "foobar")
	assert.ErrorContains(t, err, "can't parse value foo as int64 for column id")
}

func TestCSVRowToInsertValues(t *testing.T) {
	table := &pb.Table{
		Name: "test_table",
		Columns: []*pb.Column{
			{Name: "boolean", Type: pb.DataType_BOOLEAN},
			{Name: "short", Type: pb.DataType_SHORT},
			{Name: "int", Type: pb.DataType_INT},
			{Name: "long", Type: pb.DataType_LONG},
			{Name: "float", Type: pb.DataType_FLOAT},
			{Name: "double", Type: pb.DataType_DOUBLE},
			{Name: "decimal", Type: pb.DataType_DECIMAL},
			{Name: "utc_datetime", Type: pb.DataType_UTC_DATETIME},
			{Name: "naive_datetime", Type: pb.DataType_NAIVE_DATETIME},
			{Name: "naive_date", Type: pb.DataType_NAIVE_DATE},
			{Name: "str", Type: pb.DataType_STRING},
			{Name: "json", Type: pb.DataType_JSON},
			// Unclear CH mapping: may be removed, currently just mapped as String
			{Name: "binary", Type: pb.DataType_BINARY},
			{Name: "xml", Type: pb.DataType_XML},
		},
	}
	row, err := CSVRowToInsertValues([]string{
		"false", "42", "43", "44", "100.5", "200.55", "47.47",
		"2022-03-05T04:45:12.123456789Z",
		"2022-03-05T04:45:12",
		"2022-03-05",
		"foo", "{\"foo\": \"bar\"}", "0x42", "<foo>bar</foo>",
	}, table, "my-null-str")
	assert.NoError(t, err)
	assert.Equal(t, []any{
		false, int16(42), int32(43), int64(44), float64(100.5), float64(200.55), decimal.NewFromFloat32(47.47),
		time.Date(2022, 3, 5, 4, 45, 12, 123456789, time.UTC),
		time.Date(2022, 3, 5, 4, 45, 12, 0, time.UTC),
		time.Date(2022, 3, 5, 0, 0, 0, 0, time.UTC),
		"foo", "{\"foo\": \"bar\"}", "0x42", "<foo>bar</foo>",
	}, row)

	row, err = CSVRowToInsertValues([]string{
		"true", "32767", "2147483647", "9223372036854775807", "-100.5", "-200.55", "-100.55",
		"2023-05-06T02:12:15.234567890Z",
		"2023-05-06T02:12:15",
		"2023-05-06",
		"qaz", "{\"qaz\": \"qux\"}", "0xFF", "<x/>",
	}, table, "my-null-str")
	assert.NoError(t, err)
	assert.Equal(t, []any{
		true, int16(32767), int32(2147483647), int64(9223372036854775807), float64(-100.5), float64(-200.55), decimal.NewFromFloat32(-100.55),
		time.Date(2023, 5, 6, 2, 12, 15, 234567890, time.UTC),
		time.Date(2023, 5, 6, 2, 12, 15, 0, time.UTC),
		time.Date(2023, 5, 6, 0, 0, 0, 0, time.UTC),
		"qaz", "{\"qaz\": \"qux\"}", "0xFF", "<x/>",
	}, row)
}

func TestCSVRowToInsertValuesNullStr(t *testing.T) {
	table := &pb.Table{
		Name: "test_table",
		Columns: []*pb.Column{
			{Name: "id", Type: pb.DataType_LONG},
			{Name: "name", Type: pb.DataType_STRING},
			{Name: "is_deleted", Type: pb.DataType_BOOLEAN},
			{Name: "some_json_field", Type: pb.DataType_JSON},
		},
	}

	row, err := CSVRowToInsertValues([]string{
		"my-null-str", "foo", "true", `{"foo": "bar"}`,
	}, table, "my-null-str")
	assert.Equal(t, []any{
		nil, "foo", true, `{"foo": "bar"}`,
	}, row)
	assert.NoError(t, err)

	row, err = CSVRowToInsertValues([]string{
		"42", "my-null-str", "false", "my-null-str",
	}, table, "my-null-str")
	assert.NoError(t, err)
	assert.Equal(t, []any{
		int64(42), nil, false, "{}", // <- JSON can't be nullable, so we use an empty object instead
	}, row)

	row, err = CSVRowToInsertValues([]string{
		"43", "bar", "my-null-str", `{"foo": "bar"}`,
	}, table, "my-null-str")
	assert.NoError(t, err)
	assert.Equal(t, []any{
		int64(43), "bar", nil, `{"foo": "bar"}`,
	}, row)
}

func TestCSVRowsToSelectQueryValidation(t *testing.T) {
	pkCols := []*PrimaryKeyColumn{{Index: 0, Name: "id", Type: pb.DataType_LONG}}
	_, err := CSVRowsToSelectQuery(nil, "", nil)
	assert.ErrorContains(t, err, "expected non-empty list of primary keys columns")
	_, err = CSVRowsToSelectQuery(nil, "", pkCols)
	assert.ErrorContains(t, err, "table name is empty")
	_, err = CSVRowsToSelectQuery(CSV{}, "test_table", pkCols)
	assert.ErrorContains(t, err, "expected non-empty CSV slice")
	_, err = CSVRowsToSelectQuery(CSV{{"foo"}}, "test_table",
		[]*PrimaryKeyColumn{{Index: 5, Name: "id", Type: pb.DataType_LONG}})
	assert.ErrorContains(t, err, "can't find matching value for primary key with index 5")
}

func TestCSVRowsToSelectQuery(t *testing.T) {
	fullTableName := "`foo`.`bar`"
	batch := CSV{
		{"42", "foo", "2022-03-05T04:45:12.123456789Z", "false"},
		{"43", "bar", "2022-03-05T04:45:12.123456789Z", "false"},
	}
	pkCols := []*PrimaryKeyColumn{
		{Index: 0, Name: "id", Type: pb.DataType_LONG},
	}
	query, err := CSVRowsToSelectQuery(batch, fullTableName, pkCols)
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM `foo`.`bar` FINAL WHERE (id) IN ((42), (43)) ORDER BY (id) LIMIT 2", query)

	batch = CSV{
		{"42", "foo", "2022-03-05T04:45:12.123456789Z", "false"},
		{"43", "bar", "2022-03-05T04:45:12.123456789Z", "false"},
		{"44", "qaz", "2022-03-05T04:45:12.123456789Z", "false"},
		{"45", "qux", "2022-03-05T04:45:12.123456789Z", "false"},
	}
	pkCols = []*PrimaryKeyColumn{
		{Index: 0, Name: "id", Type: pb.DataType_LONG},
		{Index: 1, Name: "name", Type: pb.DataType_STRING},
	}
	query, err = CSVRowsToSelectQuery(batch, fullTableName, pkCols)
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM `foo`.`bar` FINAL WHERE (id, name) IN ((42, 'foo'), (43, 'bar'), (44, 'qaz'), (45, 'qux')) ORDER BY (id, name) LIMIT 4", query)
}

func TestCSVRowToUpdatedDBRowValidation(t *testing.T) {
	_, err := CSVRowToUpdatedDBRow(nil, nil, nil, "", "")
	assert.ErrorContains(t, err, "unmodifiedStr can't be empty")
	_, err = CSVRowToUpdatedDBRow(nil, nil, nil, "", "bar")
	assert.ErrorContains(t, err, "nullStr can't be empty")
	_, err = CSVRowToUpdatedDBRow(nil, nil, nil, "foo", "bar")
	assert.ErrorContains(t, err, "table can't be nil")

	var table = &pb.Table{
		Name: "test_table",
		Columns: []*pb.Column{
			{Name: "id", Type: pb.DataType_LONG},
			{Name: "name", Type: pb.DataType_STRING},
		},
	}
	_, err = CSVRowToUpdatedDBRow(nil, nil, table, "foo", "bar")
	assert.ErrorContains(t, err, "expected CSV, table definition and ClickHouse row to contain the same number of columns, but got 0, 2 and 0")
	_, err = CSVRowToUpdatedDBRow(nil, []any{int64(42), "foo"}, table, "foo", "bar")
	assert.ErrorContains(t, err, "expected CSV, table definition and ClickHouse row to contain the same number of columns, but got 0, 2 and 2")
	// CSV columns count mismatch
	_, err = CSVRowToUpdatedDBRow([]string{"42"}, []any{int64(42), "foo"}, table, "foo", "bar")
	assert.ErrorContains(t, err, "expected CSV, table definition and ClickHouse row to contain the same number of columns, but got 1, 2 and 2")
	// DB columns count mismatch
	_, err = CSVRowToUpdatedDBRow([]string{"42", "bar"}, []any{int64(42)}, table, "foo", "bar")
	assert.ErrorContains(t, err, "expected CSV, table definition and ClickHouse row to contain the same number of columns, but got 2, 2 and 1")
	// Table definition columns count mismatch
	_, err = CSVRowToUpdatedDBRow([]string{"42", "bar"}, []any{int64(42), "foo"}, &pb.Table{
		Name:    "test_table",
		Columns: []*pb.Column{{Name: "id", Type: pb.DataType_LONG}},
	}, "foo", "bar")
	assert.ErrorContains(t, err, "expected CSV, table definition and ClickHouse row to contain the same number of columns, but got 2, 1 and 2")
	_, err = CSVRowToUpdatedDBRow([]string{"qaz", "qux"}, []any{int64(42), "foo"}, table, "foo", "bar")
	assert.ErrorContains(t, err, "can't parse value qaz as int64 for column id")
}

func TestCSVRowToUpdatedDBRow(t *testing.T) {
	table := &pb.Table{
		Name: "test_table",
		Columns: []*pb.Column{
			{Name: "id", Type: pb.DataType_LONG},
			{Name: "name", Type: pb.DataType_STRING},
			{Name: "_fivetran_synced", Type: pb.DataType_UTC_DATETIME},
			{Name: "_fivetran_deleted", Type: pb.DataType_BOOLEAN},
		},
	}
	dbRow := []any{int64(42), "foo", time.Date(2023, 5, 6, 2, 12, 15, 234567890, time.UTC), false}

	row, err := CSVRowToUpdatedDBRow(
		[]string{"43", "bar", "2024-03-05T04:45:12.123456789Z", "true"},
		dbRow, table, "my-null-str", "my-unmodified-str")
	assert.NoError(t, err)
	assert.Equal(t, []any{int64(43), "bar", time.Date(2024, 3, 5, 4, 45, 12, 123456789, time.UTC), true}, row)

	row, err = CSVRowToUpdatedDBRow(
		[]string{"my-unmodified-str", "my-null-str", "my-unmodified-str", "my-unmodified-str"},
		dbRow, table, "my-null-str", "my-unmodified-str")
	assert.NoError(t, err)
	assert.Equal(t, []any{int64(42), nil, time.Date(2023, 5, 6, 2, 12, 15, 234567890, time.UTC), false}, row)

	row, err = CSVRowToUpdatedDBRow(
		[]string{"my-null-str", "my-unmodified-str", "my-null-str", "my-null-str"},
		dbRow, table, "my-null-str", "my-unmodified-str")
	assert.NoError(t, err)
	assert.Equal(t, []any{nil, "foo", nil, nil}, row)
}

func TestCSVRowToSoftDeletedRowValidation(t *testing.T) {
	csvRow := []string{"null", "null", "2022-03-05T04:45:12.123456789Z", "false"}
	_, err := CSVRowToSoftDeletedRow(csvRow, nil, 1, 4)
	assert.ErrorContains(t, err, "can't find column _fivetran_deleted with index 4 in a CSV row")
	_, err = CSVRowToSoftDeletedRow(csvRow, nil, 5, 3)
	assert.ErrorContains(t, err, "can't find column _fivetran_synced with index 5 in a CSV row")
	_, err = CSVRowToSoftDeletedRow(csvRow, nil, 2, 3)
	assert.ErrorContains(t, err, "expected ClickHouse row to contain at least 2 columns, but got 0")
	_, err = CSVRowToSoftDeletedRow(csvRow, []any{int64(42)}, 2, 3)
	assert.ErrorContains(t, err, "expected ClickHouse row to contain at least 2 columns, but got 1")
}

func TestCSVRowToSoftDeletedRow(t *testing.T) {
	dbRow := []any{int64(42), "foo", time.Now(), false}
	row, err := CSVRowToSoftDeletedRow(
		[]string{"null", "null", "2022-03-05T04:45:12.123456789Z", "false"},
		dbRow, 2, 3)
	assert.NoError(t, err)
	assert.Equal(t, []any{int64(42), "foo", time.Date(2022, 3, 5, 4, 45, 12, 123456789, time.UTC), true}, row)

	_, err = CSVRowToSoftDeletedRow(
		[]string{"null", "null", "2022-03-05T04:45:12.123456789Z", "false"},
		dbRow, 3, 3)
	assert.ErrorContains(t, err, "can't parse value false as UTC datetime for column _fivetran_synced")
}

func TestCalcCSVIndicesForParallel(t *testing.T) {
	res, err := CalcCSVSlicesGroupsForParallel(0, 1, 1)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(res))

	res, err = CalcCSVSlicesGroupsForParallel(2, 1, 1)
	assert.NoError(t, err)
	assert.Equal(t, [][]CSVSliceIndices{
		{ // parallel group 1
			{0, 1}, // slice 1 in group 1
		},
		{ // parallel group 2
			{1, 2}, // slice 1 in group 2
		},
	}, res)

	res, err = CalcCSVSlicesGroupsForParallel(1, 2, 2)
	assert.NoError(t, err)
	assert.Equal(t, [][]CSVSliceIndices{
		{ // parallel group 1
			{0, 1}, // slice 1 in group 1
		},
	}, res)

	res, err = CalcCSVSlicesGroupsForParallel(2, 2, 2)
	assert.NoError(t, err)
	assert.Equal(t, [][]CSVSliceIndices{
		{ // parallel group 1
			{0, 2}, // slice 1 in group 1
		},
	}, res)

	res, err = CalcCSVSlicesGroupsForParallel(3, 2, 2)
	assert.NoError(t, err)
	assert.Equal(t, [][]CSVSliceIndices{
		{ // parallel group 1
			{0, 2}, // slice 1 in group 1
			{2, 3}, // slice 2 in group 1
		},
	}, res)

	res, err = CalcCSVSlicesGroupsForParallel(4, 2, 2)
	assert.NoError(t, err)
	assert.Equal(t, [][]CSVSliceIndices{
		{ // parallel group 1
			{0, 2}, // slice 1 in group 1
			{2, 4}, // slice 2 in group 1
		},
	}, res)

	res, err = CalcCSVSlicesGroupsForParallel(5, 2, 2)
	assert.NoError(t, err)
	assert.Equal(t, [][]CSVSliceIndices{
		{ // parallel group 1
			{0, 2}, // slice 1 in group 1
			{2, 4}, // slice 2 in group 1
		},
		{ // parallel group 2
			{4, 5}, // slice 1 in group 2
		},
	}, res)

	res, err = CalcCSVSlicesGroupsForParallel(500, 100, 2)
	assert.NoError(t, err)
	assert.Equal(t, [][]CSVSliceIndices{
		{ // parallel group 1
			{0, 100},   // slice 1 in group 1
			{100, 200}, // slice 2 in group 1
		},
		{ // parallel group 2
			{200, 300}, // slice 1 in group 2
			{300, 400}, // slice 2 in group 2
		},
		{ // parallel group 3
			{400, 500}, // slice 1 in group 3
		},
	}, res)

	res, err = CalcCSVSlicesGroupsForParallel(10000, 1000, 5)
	assert.NoError(t, err)
	assert.Equal(t, [][]CSVSliceIndices{
		{ // parallel group 1
			{0, 1000},    // slice 1 in group 1
			{1000, 2000}, // slice 2 in group 1
			{2000, 3000}, // slice 3 in group 1
			{3000, 4000}, // slice 4 in group 1
			{4000, 5000}, // slice 5 in group 1
		},
		{ // parallel group 2
			{5000, 6000},  // slice 1 in group 2
			{6000, 7000},  // slice 2 in group 2
			{7000, 8000},  // slice 3 in group 2
			{8000, 9000},  // slice 4 in group 2
			{9000, 10000}, // slice 5 in group 2
		},
	}, res)

	res, err = CalcCSVSlicesGroupsForParallel(999, 100, 5)
	assert.NoError(t, err)
	assert.Equal(t, [][]CSVSliceIndices{
		{ // parallel group 1
			{0, 100},   // slice 1 in group 1
			{100, 200}, // slice 2 in group 1
			{200, 300}, // slice 3 in group 1
			{300, 400}, // slice 4 in group 1
			{400, 500}, // slice 5 in group 1

		},
		{ // parallel group 2
			{500, 600}, // slice 1 in group 2
			{600, 700}, // slice 2 in group 2
			{700, 800}, // slice 3 in group 2
			{800, 900}, // slice 4 in group 2
			{900, 999}, // slice 5 in group 2
		},
	}, res)

	res, err = CalcCSVSlicesGroupsForParallel(1001, 100, 5)
	assert.NoError(t, err)
	assert.Equal(t, [][]CSVSliceIndices{
		{ // parallel group 1
			{0, 100},   // slice 1 in group 1
			{100, 200}, // slice 2 in group 1
			{200, 300}, // slice 3 in group 1
			{300, 400}, // slice 4 in group 1
			{400, 500}, // slice 5 in group 1

		},
		{ // parallel group 2
			{500, 600},  // slice 1 in group 2
			{600, 700},  // slice 2 in group 2
			{700, 800},  // slice 3 in group 2
			{800, 900},  // slice 4 in group 2
			{900, 1000}, // slice 5 in group 2
		},
		{ // parallel group 3
			{1000, 1001}, // slice 1 in group 3
		},
	}, res)

	res, err = CalcCSVSlicesGroupsForParallel(300_000, 100_000, 1)
	assert.NoError(t, err)
	assert.Equal(t, [][]CSVSliceIndices{
		{ // only one slice in each group
			{0, 100_000},
		},
		{
			{100_000, 200_000},
		},
		{
			{200_000, 300_000},
		},
	}, res)

	res, err = CalcCSVSlicesGroupsForParallel(299_999, 100_000, 1)
	assert.NoError(t, err)
	assert.Equal(t, [][]CSVSliceIndices{
		{ // only one slice in each group
			{0, 100_000},
		},
		{
			{100_000, 200_000},
		},
		{
			{200_000, 299_999},
		},
	}, res)

	res, err = CalcCSVSlicesGroupsForParallel(300_001, 100_000, 1)
	assert.NoError(t, err)
	assert.Equal(t, [][]CSVSliceIndices{
		{ // only one slice in each group
			{0, 100_000},
		},
		{
			{100_000, 200_000},
		},
		{
			{200_000, 300_000},
		},
		{
			{300_000, 300_001},
		},
	}, res)

	_, err = CalcCSVSlicesGroupsForParallel(0, 0, 1)
	assert.ErrorContains(t, err, "batchSize can't be zero")

	_, err = CalcCSVSlicesGroupsForParallel(0, 1, 0)
	assert.ErrorContains(t, err, "maxParallelOperations can't be zero")
}
