package values

import (
	"testing"
	"time"

	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
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
