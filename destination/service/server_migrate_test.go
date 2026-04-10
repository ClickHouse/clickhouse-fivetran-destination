package service

import (
	"context"
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

func TestHandleDropOperation_DefaultEntity(t *testing.T) {
	resp, err := handleDropOperation(context.Background(), nil, "schema", "table", &pb.DropOperation{})
	require.NoError(t, err)
	assert.NotNil(t, resp.GetTask())
	assert.Contains(t, resp.GetTask().GetMessage(), "unsupported drop operation entity")
}

func TestHandleDropOperation_InvalidTimestamp(t *testing.T) {
	resp, err := handleDropOperation(context.Background(), nil, "schema", "table", &pb.DropOperation{
		Entity: &pb.DropOperation_DropColumnInHistoryMode{
			DropColumnInHistoryMode: &pb.DropColumnInHistoryMode{
				Column:             "col",
				OperationTimestamp: "not-a-timestamp",
			},
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, resp.GetTask())
	assert.Contains(t, resp.GetTask().GetMessage(), "failed to parse operation timestamp")
}

func TestHandleCopyOperation_DefaultEntity(t *testing.T) {
	resp, err := handleCopyOperation(context.Background(), nil, "schema", "table", &pb.CopyOperation{})
	require.NoError(t, err)
	assert.NotNil(t, resp.GetTask())
	assert.Contains(t, resp.GetTask().GetMessage(), "unsupported copy operation entity")
}

func TestHandleRenameOperation_DefaultEntity(t *testing.T) {
	resp, err := handleRenameOperation(context.Background(), nil, "schema", "table", &pb.RenameOperation{})
	require.NoError(t, err)
	assert.NotNil(t, resp.GetTask())
	assert.Contains(t, resp.GetTask().GetMessage(), "unsupported rename operation entity")
}

func TestHandleAddOperation_DefaultEntity(t *testing.T) {
	resp, err := handleAddOperation(context.Background(), nil, "schema", "table", &pb.AddOperation{})
	require.NoError(t, err)
	assert.NotNil(t, resp.GetTask())
	assert.Contains(t, resp.GetTask().GetMessage(), "unsupported add operation entity")
}

func TestHandleAddOperation_UnknownColumnType(t *testing.T) {
	resp, err := handleAddOperation(context.Background(), nil, "schema", "table", &pb.AddOperation{
		Entity: &pb.AddOperation_AddColumnWithDefaultValue{
			AddColumnWithDefaultValue: &pb.AddColumnWithDefaultValue{
				Column:       "col",
				ColumnType:   pb.DataType_UNSPECIFIED,
				DefaultValue: "val",
			},
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, resp.GetTask())
	assert.Contains(t, resp.GetTask().GetMessage(), "unknown datatype")
}

func TestHandleAddOperation_HistoryMode_UnknownColumnType(t *testing.T) {
	resp, err := handleAddOperation(context.Background(), nil, "schema", "table", &pb.AddOperation{
		Entity: &pb.AddOperation_AddColumnInHistoryMode{
			AddColumnInHistoryMode: &pb.AddColumnInHistoryMode{
				Column:             "col",
				ColumnType:         pb.DataType_UNSPECIFIED,
				DefaultValue:       "val",
				OperationTimestamp: "2024-01-15T10:30:00Z",
			},
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, resp.GetTask())
	assert.Contains(t, resp.GetTask().GetMessage(), "unknown datatype")
}

func TestHandleAddOperation_HistoryMode_InvalidTimestamp(t *testing.T) {
	resp, err := handleAddOperation(context.Background(), nil, "schema", "table", &pb.AddOperation{
		Entity: &pb.AddOperation_AddColumnInHistoryMode{
			AddColumnInHistoryMode: &pb.AddColumnInHistoryMode{
				Column:             "col",
				ColumnType:         pb.DataType_STRING,
				DefaultValue:       "val",
				OperationTimestamp: "not-a-timestamp",
			},
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, resp.GetTask())
	assert.Contains(t, resp.GetTask().GetMessage(), "failed to parse operation timestamp")
}

func TestHandleTableSyncModeMigration_UnsupportedLiveTransitions(t *testing.T) {
	unsupportedTypes := []pb.TableSyncModeMigrationType{
		pb.TableSyncModeMigrationType_SOFT_DELETE_TO_LIVE,
		pb.TableSyncModeMigrationType_HISTORY_TO_LIVE,
		pb.TableSyncModeMigrationType_LIVE_TO_SOFT_DELETE,
		pb.TableSyncModeMigrationType_LIVE_TO_HISTORY,
	}
	for _, mt := range unsupportedTypes {
		t.Run(mt.String(), func(t *testing.T) {
			resp, err := handleTableSyncModeMigration(context.Background(), nil, "schema", "table",
				&pb.TableSyncModeMigrationOperation{Type: mt})
			require.NoError(t, err)
			assert.True(t, resp.GetUnsupported())
		})
	}
}

func TestHandleTableSyncModeMigration_DefaultUnknownType(t *testing.T) {
	resp, err := handleTableSyncModeMigration(context.Background(), nil, "schema", "table",
		&pb.TableSyncModeMigrationOperation{Type: pb.TableSyncModeMigrationType(999)})
	require.NoError(t, err)
	assert.NotNil(t, resp.GetTask())
	assert.Contains(t, resp.GetTask().GetMessage(), "unknown sync mode migration type")
}

func TestFormatMigrateValue_InvalidNaiveDatetime(t *testing.T) {
	result := formatMigrateValue("not-a-date", pb.DataType_NAIVE_DATETIME)
	assert.Equal(t, "not-a-date", result)
}

func TestFormatMigrateValue_InvalidNaiveDate(t *testing.T) {
	result := formatMigrateValue("not-a-date", pb.DataType_NAIVE_DATE)
	assert.Equal(t, "not-a-date", result)
}
