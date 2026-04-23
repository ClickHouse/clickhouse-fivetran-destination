package values

import (
	"testing"
	"time"

	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuoteValue(t *testing.T) {
	args := []struct {
		colType pb.DataType
		value   string
		result  string
	}{
		{pb.DataType_BOOLEAN, "true", "true"},
		{pb.DataType_SHORT, "42", "42"},
		{pb.DataType_INT, "42", "42"},
		{pb.DataType_LONG, "42", "42"},
		{pb.DataType_DECIMAL, "42.424242", "'42.424242'"},
		{pb.DataType_FLOAT, "42.42", "'42.42'"},
		{pb.DataType_DOUBLE, "42.4242", "'42.4242'"},
		{pb.DataType_STRING, "foobar", "'foobar'"},
		{pb.DataType_BINARY, "0x42", "'0x42'"},
		{pb.DataType_XML, "<foo>bar</foo>", "'<foo>bar</foo>'"},
		{pb.DataType_JSON, "{\"foo\": \"bar\"}", "'{\"foo\": \"bar\"}'"},
		{pb.DataType_NAIVE_DATE, "2022-03-05", "'2022-03-05'"},
		{pb.DataType_NAIVE_DATETIME, "2022-03-05T04:45:12", "'2022-03-05T04:45:12'"},
	}
	for _, arg := range args {
		result, err := Value(arg.colType, arg.value)
		assert.NoError(t, err, "expected no error for value %s with type %s", arg.value, arg.colType.String())
		assert.Equal(t, arg.result, result, "values mismatch for type %s", arg.colType.String())
	}
}

func TestQuoteUTCDateTime(t *testing.T) {
	result, err := Value(pb.DataType_UTC_DATETIME, "2022-03-05T04:45:12.123456789Z")
	assert.NoError(t, err)
	assert.Equal(t, "'1646455512123456789'", result)

	result, err = Value(pb.DataType_UTC_DATETIME, "2022-03-05T04:45:12.123456Z")
	assert.NoError(t, err)
	assert.Equal(t, "'1646455512123456000'", result)

	result, err = Value(pb.DataType_UTC_DATETIME, "2022-03-05T04:45:12.123Z")
	assert.NoError(t, err)
	assert.Equal(t, "'1646455512123000000'", result)

	result, err = Value(pb.DataType_UTC_DATETIME, "2022-03-05T04:45:12Z")
	assert.NoError(t, err)
	assert.Equal(t, "'1646455512000000000'", result)

	result, err = Value(pb.DataType_UTC_DATETIME, "foobar")
	assert.ErrorContains(t, err, "can't parse value foobar as UTC datetime")
	assert.Equal(t, "", result)
}

func TestParseValue(t *testing.T) {
	// Boolean
	val, err := Parse("test", pb.DataType_BOOLEAN, "true")
	assert.NoError(t, err)
	assert.Equal(t, true, val)
	val, err = Parse("test", pb.DataType_BOOLEAN, "false")
	assert.NoError(t, err)
	assert.Equal(t, false, val)

	_, err = Parse("test", pb.DataType_BOOLEAN, "x")
	assert.ErrorContains(t, err, "can't parse value x as boolean for column test")

	// Int16
	val, err = Parse("test", pb.DataType_SHORT, "42")
	assert.NoError(t, err)
	assert.Equal(t, int16(42), val)
	val, err = Parse("test", pb.DataType_SHORT, "-32768")
	assert.NoError(t, err)
	assert.Equal(t, int16(-32768), val)
	val, err = Parse("test", pb.DataType_SHORT, "32767")
	assert.NoError(t, err)
	assert.Equal(t, int16(32767), val)

	_, err = Parse("test", pb.DataType_SHORT, "x")
	assert.ErrorContains(t, err, "can't parse value x as int16 for column test")

	// Int32
	val, err = Parse("test", pb.DataType_INT, "42")
	assert.NoError(t, err)
	assert.Equal(t, int32(42), val)
	val, err = Parse("test", pb.DataType_INT, "-2147483648")
	assert.NoError(t, err)
	assert.Equal(t, int32(-2147483648), val)
	val, err = Parse("test", pb.DataType_INT, "2147483647")
	assert.NoError(t, err)
	assert.Equal(t, int32(2147483647), val)

	_, err = Parse("test", pb.DataType_INT, "x")
	assert.ErrorContains(t, err, "can't parse value x as int32 for column test")

	// Int64
	val, err = Parse("test", pb.DataType_LONG, "42")
	assert.NoError(t, err)
	assert.Equal(t, int64(42), val)
	val, err = Parse("test", pb.DataType_LONG, "-9223372036854775808")
	assert.NoError(t, err)
	assert.Equal(t, int64(-9223372036854775808), val)
	val, err = Parse("test", pb.DataType_LONG, "9223372036854775807")
	assert.NoError(t, err)
	assert.Equal(t, int64(9223372036854775807), val)

	_, err = Parse("test", pb.DataType_LONG, "x")
	assert.ErrorContains(t, err, "can't parse value x as int64 for column test")

	// Float32/64 (always parsed as float64)
	val, err = Parse("test", pb.DataType_FLOAT, "100.5")
	assert.NoError(t, err)
	assert.Equal(t, float64(100.5), val)

	_, err = Parse("test", pb.DataType_FLOAT, "x")
	assert.ErrorContains(t, err, "can't parse value x as float32 for column test")

	val, err = Parse("test", pb.DataType_DOUBLE, "200.55")
	assert.NoError(t, err)
	assert.Equal(t, float64(200.55), val)

	_, err = Parse("test", pb.DataType_DOUBLE, "x")
	assert.ErrorContains(t, err, "can't parse value x as float64 for column test")

	// Decimal
	val, err = Parse("test", pb.DataType_DECIMAL, "47.47")
	assert.NoError(t, err)
	assert.Equal(t, decimal.NewFromFloat32(47.47), val)

	_, err = Parse("test", pb.DataType_DECIMAL, "x")
	assert.ErrorContains(t, err, "can't parse value x as decimal for column test")

	// UTC DateTime - variable precision
	val, err = Parse("test", pb.DataType_UTC_DATETIME, "2022-03-05T04:45:12.123456789Z")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2022, 3, 5, 4, 45, 12, 123456789, time.UTC), val)

	val, err = Parse("test", pb.DataType_UTC_DATETIME, "2022-03-05T04:45:12.123456Z")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2022, 3, 5, 4, 45, 12, 123456000, time.UTC), val)

	val, err = Parse("test", pb.DataType_UTC_DATETIME, "2022-03-05T04:45:12.123Z")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2022, 3, 5, 4, 45, 12, 123000000, time.UTC), val)

	val, err = Parse("test", pb.DataType_UTC_DATETIME, "2022-03-05T04:45:12Z")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2022, 3, 5, 4, 45, 12, 0, time.UTC), val)

	_, err = Parse("test", pb.DataType_UTC_DATETIME, "x")
	assert.ErrorContains(t, err, "can't parse value x as UTC datetime for column test")

	// Naive DateTime
	val, err = Parse("test", pb.DataType_NAIVE_DATETIME, "2022-03-05T04:45:12")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2022, 3, 5, 4, 45, 12, 0, time.UTC), val)

	_, err = Parse("test", pb.DataType_NAIVE_DATETIME, "x")
	assert.ErrorContains(t, err, "can't parse value x as naive datetime for column test")

	// Naive Date
	val, err = Parse("test", pb.DataType_NAIVE_DATE, "2022-03-05")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2022, 3, 5, 0, 0, 0, 0, time.UTC), val)

	_, err = Parse("test", pb.DataType_NAIVE_DATE, "x")
	assert.ErrorContains(t, err, "can't parse value x as naive date for column test")

	// String types
	val, err = Parse("test", pb.DataType_STRING, "foobar")
	assert.NoError(t, err)
	assert.Equal(t, "foobar", val)
	val, err = Parse("test", pb.DataType_JSON, "{\"foo\": \"bar\"}")
	assert.NoError(t, err)
	assert.Equal(t, "{\"foo\": \"bar\"}", val)

	// Unclear CH mapping: may be removed, currently just mapped as String
	val, err = Parse("test", pb.DataType_XML, "foobar")
	assert.NoError(t, err)
	assert.Equal(t, "foobar", val)
	val, err = Parse("test", pb.DataType_BINARY, "foobar")
	assert.NoError(t, err)
	assert.Equal(t, "foobar", val)

	// Unspecified
	_, err = Parse("test", pb.DataType_UNSPECIFIED, "foobar")
	assert.ErrorContains(t, err, "no target type for column test with type UNSPECIFIED")
}

func TestParseTruncatedDate(t *testing.T) {
	val, err := Parse("test", pb.DataType_NAIVE_DATE, "1899-12-31")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC), val)

	val, err = Parse("test", pb.DataType_NAIVE_DATE, "1900-01-01")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC), val)

	val, err = Parse("test", pb.DataType_NAIVE_DATE, "1900-01-02")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(1900, 1, 2, 0, 0, 0, 0, time.UTC), val)

	val, err = Parse("test", pb.DataType_NAIVE_DATE, "2300-01-01")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2299, 12, 31, 0, 0, 0, 0, time.UTC), val)

	val, err = Parse("test", pb.DataType_NAIVE_DATE, "2299-12-31")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2299, 12, 31, 0, 0, 0, 0, time.UTC), val)

	val, err = Parse("test", pb.DataType_NAIVE_DATE, "2299-12-30")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2299, 12, 30, 0, 0, 0, 0, time.UTC), val)

	// MySQL-like edge cases
	val, err = Parse("test", pb.DataType_NAIVE_DATE, "0000-01-01")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC), val)

	val, err = Parse("test", pb.DataType_NAIVE_DATE, "9999-12-31")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2299, 12, 31, 0, 0, 0, 0, time.UTC), val)
}

func TestParseTruncatedDateTime(t *testing.T) {
	val, err := Parse("test", pb.DataType_NAIVE_DATETIME, "1899-12-31T23:59:59")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC), val)

	val, err = Parse("test", pb.DataType_NAIVE_DATETIME, "1900-01-01T00:00:00")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC), val)

	val, err = Parse("test", pb.DataType_NAIVE_DATETIME, "1900-01-01T00:00:01")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(1900, 1, 1, 0, 0, 1, 0, time.UTC), val)

	// year > 2262
	val, err = Parse("test", pb.DataType_NAIVE_DATETIME, "2263-04-11T00:00:00")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2262, 4, 11, 23, 47, 16, 0, time.UTC), val)

	// year == 2262, month > 04
	val, err = Parse("test", pb.DataType_NAIVE_DATETIME, "2262-05-11T00:00:00")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2262, 4, 11, 23, 47, 16, 0, time.UTC), val)

	// year == 2262, month == 04, day > 11
	val, err = Parse("test", pb.DataType_NAIVE_DATETIME, "2262-04-12T00:00:00")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2262, 4, 11, 23, 47, 16, 0, time.UTC), val)

	// minute > 47
	val, err = Parse("test", pb.DataType_NAIVE_DATETIME, "2262-04-11T23:48:00")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2262, 4, 11, 23, 47, 16, 0, time.UTC), val)

	// seconds > 16
	val, err = Parse("test", pb.DataType_NAIVE_DATETIME, "2262-04-11T23:47:17")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2262, 4, 11, 23, 47, 16, 0, time.UTC), val)

	// an exact fit
	val, err = Parse("test", pb.DataType_NAIVE_DATETIME, "2262-04-11T23:47:16")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2262, 4, 11, 23, 47, 16, 0, time.UTC), val)

	// MySQL-like edge cases
	val, err = Parse("test", pb.DataType_NAIVE_DATETIME, "0000-01-01T00:00:00")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC), val)

	val, err = Parse("test", pb.DataType_NAIVE_DATETIME, "9999-12-31T23:59:59")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2262, 4, 11, 23, 47, 16, 0, time.UTC), val)
}

func TestParseTruncatedUTCDateTime(t *testing.T) {
	val, err := Parse("test", pb.DataType_UTC_DATETIME, "1899-12-31T23:59:59.999999999Z")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC), val)

	val, err = Parse("test", pb.DataType_UTC_DATETIME, "1900-01-01T00:00:00.000000000Z")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC), val)

	val, err = Parse("test", pb.DataType_UTC_DATETIME, "1900-01-01T00:00:00.000000001Z")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(1900, 1, 1, 0, 0, 0, 1, time.UTC), val)

	// year > 2262
	val, err = Parse("test", pb.DataType_UTC_DATETIME, "2263-04-11T00:00:00.000Z")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2262, 4, 11, 23, 47, 16, 0, time.UTC), val)

	// year == 2262, month > 04
	val, err = Parse("test", pb.DataType_UTC_DATETIME, "2262-05-11T00:00:00.000Z")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2262, 4, 11, 23, 47, 16, 0, time.UTC), val)

	// year == 2262, month == 04, day > 11
	val, err = Parse("test", pb.DataType_UTC_DATETIME, "2262-04-12T00:00:00.000Z")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2262, 4, 11, 23, 47, 16, 0, time.UTC), val)

	// minute > 47
	val, err = Parse("test", pb.DataType_UTC_DATETIME, "2262-04-11T23:48:00.000Z")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2262, 4, 11, 23, 47, 16, 0, time.UTC), val)

	// seconds > 16
	val, err = Parse("test", pb.DataType_UTC_DATETIME, "2262-04-11T23:47:17.000Z")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2262, 4, 11, 23, 47, 16, 0, time.UTC), val)

	// non-zero nanoseconds
	val, err = Parse("test", pb.DataType_UTC_DATETIME, "2262-04-11T23:47:16.000000001Z")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2262, 4, 11, 23, 47, 16, 0, time.UTC), val)

	// an exact fit
	val, err = Parse("test", pb.DataType_UTC_DATETIME, "2262-04-11T23:47:16.0Z")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2262, 4, 11, 23, 47, 16, 0, time.UTC), val)

	// barely fits
	val, err = Parse("test", pb.DataType_UTC_DATETIME, "2262-04-11T23:47:15.999999999Z")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2262, 4, 11, 23, 47, 15, 999_999_999, time.UTC), val)

	// MySQL-like edge cases
	val, err = Parse("test", pb.DataType_UTC_DATETIME, "0000-01-01T00:00:00.000000000Z")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC), val)

	val, err = Parse("test", pb.DataType_UTC_DATETIME, "9999-12-31T23:59:59.999999999Z")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2262, 4, 11, 23, 47, 16, 0, time.UTC), val)
}

func TestMigrateValue(t *testing.T) {
	// NULL
	mv := NewMigrateValueNull()
	assert.True(t, mv.IsNull())
	assert.Equal(t, "NULL", mv.Literal())

	// Plain value — quoted
	mv = NewMigrateValueQuoted("hello")
	assert.False(t, mv.IsNull())
	assert.Equal(t, "'hello'", mv.Literal())

	// Embedded single quote — escaped (SQL-standard doubling)
	mv = NewMigrateValueQuoted("O'Brien")
	assert.False(t, mv.IsNull())
	assert.Equal(t, "'O''Brien'", mv.Literal())

	// Empty string — valid quoted literal, NOT NULL
	mv = NewMigrateValueQuoted("")
	assert.False(t, mv.IsNull())
	assert.Equal(t, "''", mv.Literal())

	// Zero-value MigrateValue: safe defaults (not NULL, empty literal)
	// This guards against accidental construction without going through the
	// constructors — builders must still behave predictably.
	var zero MigrateValue
	assert.False(t, zero.IsNull())
	assert.Equal(t, "", zero.Literal())
}

// TestNewMigrateValue exercises the migration-path formatter. The invariants
// covered here include the ones the old service.formatMigrateValue covered plus
// a few regressions (UTC parse errors now surface; single quotes are escaped).
//
// Keep this table in sync with TestQuoteValue / TestQuoteUTCDateTime above —
// Value (write path) and NewMigrateValue (migration path) must agree on the
// effective SQL literal they produce so migrations stay consistent with writes
// for the same DataType + value.
func TestNewMigrateValue(t *testing.T) {
	cases := []struct {
		name    string
		colType pb.DataType
		value   string
		want    string
	}{
		// UTC_DATETIME: ISO 8601 -> nanos-since-epoch, quoted.
		{"UTC_DATETIME, seconds", pb.DataType_UTC_DATETIME, "2005-05-28T20:57:00Z", "'1117313820000000000'"},
		{"UTC_DATETIME, milliseconds", pb.DataType_UTC_DATETIME, "2024-01-15T10:30:00.123Z", "'1705314600123000000'"},
		{"UTC_DATETIME, nanoseconds", pb.DataType_UTC_DATETIME, "2022-03-05T04:45:12.123456789Z", "'1646455512123456789'"},

		// NAIVE_DATETIME / NAIVE_DATE: pass-through, quoted. ClickHouse accepts
		// both T and space separators so there's no reformatting needed.
		{"NAIVE_DATETIME passthrough", pb.DataType_NAIVE_DATETIME, "2005-05-28T20:57:00", "'2005-05-28T20:57:00'"},
		{"NAIVE_DATE passthrough", pb.DataType_NAIVE_DATE, "2005-05-28", "'2005-05-28'"},

		// Strings: quoted + escape embedded single quotes (defense in depth —
		// SDK defaults are user-configurable).
		{"STRING plain", pb.DataType_STRING, "hello world", "'hello world'"},
		{"STRING with single quote", pb.DataType_STRING, "O'Brien", "'O''Brien'"},
		{"STRING empty", pb.DataType_STRING, "", "''"},

		// Numerics: the migration UPDATE path quotes everything and ClickHouse
		// coerces. Matches the pre-refactor behavior.
		{"INT", pb.DataType_INT, "42", "'42'"},
		{"BOOLEAN", pb.DataType_BOOLEAN, "true", "'true'"},
		{"DOUBLE", pb.DataType_DOUBLE, "3.14", "'3.14'"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewMigrateValue(tc.colType, tc.value)
			require.NoError(t, err)
			assert.False(t, got.IsNull())
			assert.Equal(t, tc.want, got.Literal())
		})
	}
}

func TestNewMigrateValue_InvalidUTCDateTime(t *testing.T) {
	// Unlike the old formatMigrateValue (which quietly returned the raw string),
	// NewMigrateValue errors out so the handler can fail fast instead of sending
	// a malformed literal to ClickHouse.
	_, err := NewMigrateValue(pb.DataType_UTC_DATETIME, "not-a-date")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "UTC datetime")
}

func TestNewMigrateValue_InvalidNaiveInputsArePassedThrough(t *testing.T) {
	// NAIVE_DATE and NAIVE_DATETIME are not parsed here — ClickHouse does the
	// parsing at execution time. Malformed strings pass through quoted; if they
	// are actually invalid, ClickHouse surfaces the error at UPDATE time.
	got, err := NewMigrateValue(pb.DataType_NAIVE_DATE, "not-a-date")
	require.NoError(t, err)
	assert.Equal(t, "'not-a-date'", got.Literal())

	got, err = NewMigrateValue(pb.DataType_NAIVE_DATETIME, "not-a-date")
	require.NoError(t, err)
	assert.Equal(t, "'not-a-date'", got.Literal())
}
