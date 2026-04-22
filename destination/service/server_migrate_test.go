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

// Each Migrate handler validates non-optional proto fields inline, right where the
// field is read. Downstream code trusts these invariants, so the handler must
// respond with FailedMigrateResponse when any required scalar is empty.

func TestHandleDropOperation_MissingColumn(t *testing.T) {
	resp, err := handleDropOperation(context.Background(), nil, "schema", "table", &pb.DropOperation{
		Entity: &pb.DropOperation_DropColumnInHistoryMode{
			DropColumnInHistoryMode: &pb.DropColumnInHistoryMode{
				OperationTimestamp: "2024-01-15T10:30:00Z",
			},
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, resp.GetTask())
	assert.Contains(t, resp.GetTask().GetMessage(), "drop_column_in_history_mode.column is required")
}

func TestHandleCopyOperation_CopyTable_MissingFields(t *testing.T) {
	cases := []struct {
		name    string
		op      *pb.CopyTable
		wantMsg string
	}{
		{"missing from_table", &pb.CopyTable{ToTable: "t2"}, "copy_table.from_table is required"},
		{"missing to_table", &pb.CopyTable{FromTable: "t1"}, "copy_table.to_table is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := handleCopyOperation(context.Background(), nil, "schema", "table", &pb.CopyOperation{
				Entity: &pb.CopyOperation_CopyTable{CopyTable: tc.op},
			})
			require.NoError(t, err)
			assert.Contains(t, resp.GetTask().GetMessage(), tc.wantMsg)
		})
	}
}

func TestHandleCopyOperation_CopyColumn_MissingFields(t *testing.T) {
	cases := []struct {
		name    string
		op      *pb.CopyColumn
		wantMsg string
	}{
		{"missing from_column", &pb.CopyColumn{ToColumn: "b"}, "copy_column.from_column is required"},
		{"missing to_column", &pb.CopyColumn{FromColumn: "a"}, "copy_column.to_column is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := handleCopyOperation(context.Background(), nil, "schema", "table", &pb.CopyOperation{
				Entity: &pb.CopyOperation_CopyColumn{CopyColumn: tc.op},
			})
			require.NoError(t, err)
			assert.Contains(t, resp.GetTask().GetMessage(), tc.wantMsg)
		})
	}
}

func TestHandleCopyOperation_CopyTableToHistoryMode_MissingFields(t *testing.T) {
	cases := []struct {
		name    string
		op      *pb.CopyTableToHistoryMode
		wantMsg string
	}{
		{"missing from_table", &pb.CopyTableToHistoryMode{ToTable: "t2"}, "copy_table_to_history_mode.from_table is required"},
		{"missing to_table", &pb.CopyTableToHistoryMode{FromTable: "t1"}, "copy_table_to_history_mode.to_table is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := handleCopyOperation(context.Background(), nil, "schema", "table", &pb.CopyOperation{
				Entity: &pb.CopyOperation_CopyTableToHistoryMode{CopyTableToHistoryMode: tc.op},
			})
			require.NoError(t, err)
			assert.Contains(t, resp.GetTask().GetMessage(), tc.wantMsg)
		})
	}
}

func TestHandleRenameOperation_RenameTable_MissingFields(t *testing.T) {
	cases := []struct {
		name    string
		op      *pb.RenameTable
		wantMsg string
	}{
		{"missing from_table", &pb.RenameTable{ToTable: "t2"}, "rename_table.from_table is required"},
		{"missing to_table", &pb.RenameTable{FromTable: "t1"}, "rename_table.to_table is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := handleRenameOperation(context.Background(), nil, "schema", "table", &pb.RenameOperation{
				Entity: &pb.RenameOperation_RenameTable{RenameTable: tc.op},
			})
			require.NoError(t, err)
			assert.Contains(t, resp.GetTask().GetMessage(), tc.wantMsg)
		})
	}
}

func TestHandleRenameOperation_RenameColumn_MissingFields(t *testing.T) {
	cases := []struct {
		name    string
		op      *pb.RenameColumn
		wantMsg string
	}{
		{"missing from_column", &pb.RenameColumn{ToColumn: "b"}, "rename_column.from_column is required"},
		{"missing to_column", &pb.RenameColumn{FromColumn: "a"}, "rename_column.to_column is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := handleRenameOperation(context.Background(), nil, "schema", "table", &pb.RenameOperation{
				Entity: &pb.RenameOperation_RenameColumn{RenameColumn: tc.op},
			})
			require.NoError(t, err)
			assert.Contains(t, resp.GetTask().GetMessage(), tc.wantMsg)
		})
	}
}

func TestHandleAddOperation_AddColumnWithDefault_MissingColumn(t *testing.T) {
	resp, err := handleAddOperation(context.Background(), nil, "schema", "table", &pb.AddOperation{
		Entity: &pb.AddOperation_AddColumnWithDefaultValue{
			AddColumnWithDefaultValue: &pb.AddColumnWithDefaultValue{
				ColumnType:   pb.DataType_STRING,
				DefaultValue: "val",
			},
		},
	})
	require.NoError(t, err)
	assert.Contains(t, resp.GetTask().GetMessage(), "add_column_with_default_value.column is required")
}

func TestHandleAddOperation_HistoryMode_MissingColumn(t *testing.T) {
	resp, err := handleAddOperation(context.Background(), nil, "schema", "table", &pb.AddOperation{
		Entity: &pb.AddOperation_AddColumnInHistoryMode{
			AddColumnInHistoryMode: &pb.AddColumnInHistoryMode{
				ColumnType:         pb.DataType_STRING,
				OperationTimestamp: "2024-01-15T10:30:00Z",
			},
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, resp.GetTask())
	assert.Contains(t, resp.GetTask().GetMessage(), "add_column_in_history_mode.column is required")
}

func TestHandleUpdateColumnValue_MissingColumn(t *testing.T) {
	resp, err := handleUpdateColumnValue(context.Background(), nil, "schema", "table",
		&pb.UpdateColumnValueOperation{Value: "v"})
	require.NoError(t, err)
	assert.Contains(t, resp.GetTask().GetMessage(), "update_column_value.column is required")
}

func TestMigrate_MissingSchema(t *testing.T) {
	// schema/table are checked before any connection is attempted, so a zero-value Server is safe here.
	resp, err := (&Server{}).Migrate(context.Background(), &pb.MigrateRequest{
		Details: &pb.MigrationDetails{Table: "t"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp.GetTask().GetMessage(), "migration_details.schema is required")
}

func TestMigrate_MissingTable(t *testing.T) {
	resp, err := (&Server{}).Migrate(context.Background(), &pb.MigrateRequest{
		Details: &pb.MigrationDetails{Schema: "s"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp.GetTask().GetMessage(), "migration_details.table is required")
}
