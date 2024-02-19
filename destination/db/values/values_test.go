package values

import (
	"testing"
	"time"

	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func TestQuoteValue(t *testing.T) {
	assert.Equal(t, Quote(pb.DataType_STRING, "foobar"), "'foobar'")
	assert.Equal(t, Quote(pb.DataType_XML, "<foo>bar</foo>"), "'<foo>bar</foo>'")
	assert.Equal(t, Quote(pb.DataType_BINARY, "0x42"), "'0x42'")
	assert.Equal(t, Quote(pb.DataType_JSON, "{\"foo\": \"bar\"}"), "'{\"foo\": \"bar\"}'")
	assert.Equal(t, Quote(pb.DataType_SHORT, "42"), "42")
	assert.Equal(t, Quote(pb.DataType_INT, "42"), "42")
	assert.Equal(t, Quote(pb.DataType_LONG, "42"), "42")
	assert.Equal(t, Quote(pb.DataType_FLOAT, "42.42"), "42.42")
	assert.Equal(t, Quote(pb.DataType_DOUBLE, "42.4242"), "42.4242")
	assert.Equal(t, Quote(pb.DataType_DECIMAL, "42.424242"), "42.424242")
	assert.Equal(t, Quote(pb.DataType_BOOLEAN, "true"), "true")
	assert.Equal(t, Quote(pb.DataType_NAIVE_DATE, "2022-03-05"), "'2022-03-05'")
	assert.Equal(t, Quote(pb.DataType_NAIVE_DATETIME, "2022-03-05T04:45:12"), "'2022-03-05T04:45:12'")
	assert.Equal(t, Quote(pb.DataType_UTC_DATETIME, "2022-03-05T04:45:12.123456789Z"), "'2022-03-05T04:45:12.123456789Z'")
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

	// Date types
	val, err = Parse("test", pb.DataType_UTC_DATETIME, "2022-03-05T04:45:12.123456789Z")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2022, 3, 5, 4, 45, 12, 123456789, time.UTC), val)

	_, err = Parse("test", pb.DataType_UTC_DATETIME, "x")
	assert.ErrorContains(t, err, "can't parse value x as UTC datetime for column test")

	val, err = Parse("test", pb.DataType_NAIVE_DATETIME, "2022-03-05T04:45:12")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2022, 3, 5, 4, 45, 12, 0, time.UTC), val)

	_, err = Parse("test", pb.DataType_NAIVE_DATETIME, "x")
	assert.ErrorContains(t, err, "can't parse value x as naive datetime for column test")

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
