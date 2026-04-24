package service

import (
	"context"
	"testing"

	"fivetran.com/fivetran_sdk/destination/common/constants"
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

// values.NewMigrateValue surfaces UTC_DATETIME parse failures to the caller; make sure
// the handler propagates that error rather than silently sending a malformed literal
// to ClickHouse (regression guard — the old formatMigrateValue quietly passed the
// raw string through on parse failure).
func TestHandleAddOperation_AddColumnWithDefault_InvalidUTCDateTime(t *testing.T) {
	resp, err := handleAddOperation(context.Background(), nil, "schema", "table", &pb.AddOperation{
		Entity: &pb.AddOperation_AddColumnWithDefaultValue{
			AddColumnWithDefaultValue: &pb.AddColumnWithDefaultValue{
				Column:       "col",
				ColumnType:   pb.DataType_UTC_DATETIME,
				DefaultValue: "not-a-date",
			},
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, resp.GetTask())
	assert.Contains(t, resp.GetTask().GetMessage(), "UTC datetime")
}

func TestHandleAddOperation_HistoryMode_InvalidUTCDateTime(t *testing.T) {
	resp, err := handleAddOperation(context.Background(), nil, "schema", "table", &pb.AddOperation{
		Entity: &pb.AddOperation_AddColumnInHistoryMode{
			AddColumnInHistoryMode: &pb.AddColumnInHistoryMode{
				Column:             "col",
				ColumnType:         pb.DataType_UTC_DATETIME,
				DefaultValue:       "not-a-date",
				OperationTimestamp: "2024-01-15T10:30:00Z",
			},
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, resp.GetTask())
	assert.Contains(t, resp.GetTask().GetMessage(), "UTC datetime")
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

// HISTORY_TO_SOFT_DELETE is the only sync-mode operation that *creates* a
// soft-delete column, so the spec's literal reference to `_fivetran_deleted`
// in step 2 becomes the default target when the caller omits the optional
// field. Any non-empty value is a caller override and passed through verbatim,
// including columns that happen to share the canonical name.
func TestResolveSoftDeletedColumnForHistoryToSoftDelete(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{"empty defaults to _fivetran_deleted", "", constants.FivetranDeleted},
		{"explicit _fivetran_deleted is preserved", constants.FivetranDeleted, constants.FivetranDeleted},
		{"custom column name is preserved", "is_deleted", "is_deleted"},
		{"whitespace is not empty and is preserved", " ", " "},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, resolveSoftDeletedColumnForHistoryToSoftDelete(tc.input))
		})
	}
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

// buildUpdateColumnMigrateValue short-circuits to SQL NULL when isNull is true,
// without round-tripping to DescribeTable. The nil conn proves that contract:
// any accidental DescribeTable call would panic.
func TestBuildUpdateColumnMigrateValue_NullShortCircuit(t *testing.T) {
	mv, err := buildUpdateColumnMigrateValue(context.Background(), nil, "s", "t", "col", "", true)
	require.NoError(t, err)
	assert.True(t, mv.IsNull())
	assert.Equal(t, "NULL", mv.Literal())
}

// The SDK tester's set_column_to_null scenario arrives as an
// UpdateColumnValueOperation with value either "" or the literal string "NULL"
// (see AGENTS.md). Both must be treated as SQL NULL; any other value — even
// one that looks similar, like "null" or "0" — is a real value that should be
// written as a quoted literal.
func TestIsSetColumnToNullValue(t *testing.T) {
	cases := []struct {
		value string
		null  bool
	}{
		{"", true},
		{"NULL", true},
		{"null", false}, // case-sensitive per spec
		{"Null", false},
		{"0", false},
		{"false", false},
		{"NULL ", false}, // trailing whitespace is a real value
		{" NULL", false},
		{"anything else", false},
	}
	for _, tc := range cases {
		t.Run(tc.value, func(t *testing.T) {
			assert.Equal(t, tc.null, isSetColumnToNullValue(tc.value))
		})
	}
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
