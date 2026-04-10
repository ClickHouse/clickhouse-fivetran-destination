package service

import (
	"testing"

	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMigrateColumnType(t *testing.T) {
	// Standard types become Nullable
	chType, err := migrateColumnType(pb.DataType_INT)
	require.NoError(t, err)
	assert.Equal(t, "Nullable(Int32)", chType.Type)
	assert.Equal(t, "", chType.Comment)

	chType, err = migrateColumnType(pb.DataType_STRING)
	require.NoError(t, err)
	assert.Equal(t, "Nullable(String)", chType.Type)

	chType, err = migrateColumnType(pb.DataType_UTC_DATETIME)
	require.NoError(t, err)
	assert.Equal(t, "Nullable(DateTime64(9, 'UTC'))", chType.Type)

	chType, err = migrateColumnType(pb.DataType_BOOLEAN)
	require.NoError(t, err)
	assert.Equal(t, "Nullable(Bool)", chType.Type)

	// Types with comments
	chType, err = migrateColumnType(pb.DataType_JSON)
	require.NoError(t, err)
	assert.Equal(t, "Nullable(String)", chType.Type)
	assert.Equal(t, "JSON", chType.Comment)

	chType, err = migrateColumnType(pb.DataType_XML)
	require.NoError(t, err)
	assert.Equal(t, "Nullable(String)", chType.Type)
	assert.Equal(t, "XML", chType.Comment)

	// Unknown type
	_, err = migrateColumnType(pb.DataType_UNSPECIFIED)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown datatype")
}

func TestFormatMigrateValue(t *testing.T) {
	// UTC_DATETIME — converts to nanoseconds
	result := formatMigrateValue("2005-05-28T20:57:00Z", pb.DataType_UTC_DATETIME)
	assert.Equal(t, "1117313820000000000", result)

	// UTC_DATETIME with subseconds
	result = formatMigrateValue("2024-01-15T10:30:00.123Z", pb.DataType_UTC_DATETIME)
	assert.Equal(t, "1705314600123000000", result)

	// NAIVE_DATETIME — reformats
	result = formatMigrateValue("2005-05-28T20:57:00", pb.DataType_NAIVE_DATETIME)
	assert.Equal(t, "2005-05-28 20:57:00", result)

	// NAIVE_DATE — passes through
	result = formatMigrateValue("2005-05-28", pb.DataType_NAIVE_DATE)
	assert.Equal(t, "2005-05-28", result)

	// STRING — passes through unchanged
	result = formatMigrateValue("hello world", pb.DataType_STRING)
	assert.Equal(t, "hello world", result)

	// Invalid datetime — returns original value
	result = formatMigrateValue("not-a-date", pb.DataType_UTC_DATETIME)
	assert.Equal(t, "not-a-date", result)

	// Empty value
	result = formatMigrateValue("", pb.DataType_STRING)
	assert.Equal(t, "", result)

	// INT type — passes through
	result = formatMigrateValue("42", pb.DataType_INT)
	assert.Equal(t, "42", result)
}

func TestParseTimestampToNanos(t *testing.T) {
	// RFC3339 format
	nanos, err := parseTimestampToNanos("2005-05-28T20:57:00Z")
	require.NoError(t, err)
	assert.Equal(t, "1117313820000000000", nanos)

	// With timezone offset
	nanos, err = parseTimestampToNanos("2024-01-15T10:30:00+00:00")
	require.NoError(t, err)
	assert.Equal(t, "1705314600000000000", nanos)

	// Invalid format
	_, err = parseTimestampToNanos("not-a-timestamp")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse operation timestamp")

	// Empty string
	_, err = parseTimestampToNanos("")
	assert.Error(t, err)
}
