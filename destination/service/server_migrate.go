package service

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"fivetran.com/fivetran_sdk/destination/common/constants"
	dt "fivetran.com/fivetran_sdk/destination/common/data_types"
	"fivetran.com/fivetran_sdk/destination/common/log"
	"fivetran.com/fivetran_sdk/destination/db"
	"fivetran.com/fivetran_sdk/destination/db/config"
	"fivetran.com/fivetran_sdk/destination/db/sql"
	"fivetran.com/fivetran_sdk/destination/db/values"
	pb "fivetran.com/fivetran_sdk/proto"
)

func (s *Server) Migrate(ctx context.Context, in *pb.MigrateRequest) (*pb.MigrateResponse, error) {
	details := in.GetDetails()
	schema := details.GetSchema()
	table := details.GetTable()

	log.Info(fmt.Sprintf("[Migrate] Starting for %s.%s", schema, table))

	if schema == "" {
		return FailedMigrateResponse(schema, table, fmt.Errorf("migration_details.schema is required")), nil
	}
	if table == "" {
		return FailedMigrateResponse(schema, table, fmt.Errorf("migration_details.table is required")), nil
	}

	connConfig, err := config.ParseAll(in.GetConfiguration())
	if err != nil {
		log.Error(fmt.Errorf("[Migrate] %w", err))
		return FailedMigrateResponse(schema, table, err), nil
	}
	conn, err := db.GetClickHouseConnection(ctx, connConfig)
	if err != nil {
		log.Error(fmt.Errorf("[Migrate] Failed to connect for %s.%s: %w", schema, table, err))
		return FailedMigrateResponse(schema, table, err), nil
	}
	defer conn.Close() //nolint:errcheck

	switch op := details.GetOperation().(type) {
	case *pb.MigrationDetails_Drop:
		return handleDropOperation(ctx, conn, schema, table, op.Drop)
	case *pb.MigrationDetails_Copy:
		return handleCopyOperation(ctx, conn, schema, table, op.Copy)
	case *pb.MigrationDetails_Rename:
		return handleRenameOperation(ctx, conn, schema, table, op.Rename)
	case *pb.MigrationDetails_Add:
		return handleAddOperation(ctx, conn, schema, table, op.Add)
	case *pb.MigrationDetails_UpdateColumnValue:
		return handleUpdateColumnValue(ctx, conn, schema, table, op.UpdateColumnValue)
	case *pb.MigrationDetails_TableSyncModeMigration:
		return handleTableSyncModeMigration(ctx, conn, schema, table, op.TableSyncModeMigration)
	default:
		err := fmt.Errorf("unsupported migration operation type: %T", details.GetOperation())
		log.Error(fmt.Errorf("[Migrate] %w", err))
		return FailedMigrateResponse(schema, table, err), nil
	}
}

func handleDropOperation(
	ctx context.Context,
	conn *db.ClickHouseConnection,
	schema string,
	table string,
	drop *pb.DropOperation,
) (*pb.MigrateResponse, error) {
	switch entity := drop.GetEntity().(type) {
	case *pb.DropOperation_DropTable:
		log.Info(fmt.Sprintf("[Migrate] Dropping table %s.%s", schema, table))
		qualifiedName, err := sql.GetQualifiedTableName(schema, table)
		if err != nil {
			return FailedMigrateResponse(schema, table, err), nil
		}
		err = conn.DropTable(ctx, qualifiedName)
		if err != nil {
			return FailedMigrateResponse(schema, table, err), nil
		}
		log.Info(fmt.Sprintf("[Migrate] Dropped table %s.%s", schema, table))
		return SuccessfulMigrateResponse(), nil

	case *pb.DropOperation_DropColumnInHistoryMode:
		col := entity.DropColumnInHistoryMode.GetColumn()
		opTS := entity.DropColumnInHistoryMode.GetOperationTimestamp()
		log.Info(fmt.Sprintf("[Migrate] Dropping column %s in history mode for %s.%s at %s", col, schema, table, opTS))

		if col == "" {
			return FailedMigrateResponse(schema, table, fmt.Errorf("drop_column_in_history_mode.column is required")), nil
		}
		tsNanos, err := parseTimestampToNanos(opTS)
		if err != nil {
			return FailedMigrateResponse(schema, table, err), nil
		}
		err = conn.MigrateDropColumnInHistoryMode(ctx, schema, table, col, tsNanos)
		if err != nil {
			return FailedMigrateResponse(schema, table, err), nil
		}
		log.Info(fmt.Sprintf("[Migrate] Dropped column %s in history mode for %s.%s", col, schema, table))
		return SuccessfulMigrateResponse(), nil

	default:
		err := fmt.Errorf("unsupported drop operation entity: %T", drop.GetEntity())
		return FailedMigrateResponse(schema, table, err), nil
	}
}

func handleCopyOperation(
	ctx context.Context,
	conn *db.ClickHouseConnection,
	schema string,
	table string,
	copy *pb.CopyOperation,
) (*pb.MigrateResponse, error) {
	switch entity := copy.GetEntity().(type) {
	case *pb.CopyOperation_CopyTable:
		fromTable := entity.CopyTable.GetFromTable()
		toTable := entity.CopyTable.GetToTable()
		log.Info(fmt.Sprintf("[Migrate] Copying table %s.%s to %s.%s", schema, fromTable, schema, toTable))

		if fromTable == "" {
			return FailedMigrateResponse(schema, table, fmt.Errorf("copy_table.from_table is required")), nil
		}
		if toTable == "" {
			return FailedMigrateResponse(schema, table, fmt.Errorf("copy_table.to_table is required")), nil
		}
		err := conn.MigrateCopyTable(ctx, schema, fromTable, toTable)
		if err != nil {
			return FailedMigrateResponse(schema, table, err), nil
		}
		log.Info(fmt.Sprintf("[Migrate] Copied table %s.%s to %s.%s", schema, fromTable, schema, toTable))
		return SuccessfulMigrateResponse(), nil

	case *pb.CopyOperation_CopyColumn:
		fromCol := entity.CopyColumn.GetFromColumn()
		toCol := entity.CopyColumn.GetToColumn()
		log.Info(fmt.Sprintf("[Migrate] Copying column %s to %s in %s.%s", fromCol, toCol, schema, table))

		if fromCol == "" {
			return FailedMigrateResponse(schema, table, fmt.Errorf("copy_column.from_column is required")), nil
		}
		if toCol == "" {
			return FailedMigrateResponse(schema, table, fmt.Errorf("copy_column.to_column is required")), nil
		}
		err := conn.MigrateCopyColumn(ctx, schema, table, fromCol, toCol)
		if err != nil {
			return FailedMigrateResponse(schema, table, err), nil
		}
		log.Info(fmt.Sprintf("[Migrate] Copied column %s to %s in %s.%s", fromCol, toCol, schema, table))
		return SuccessfulMigrateResponse(), nil

	case *pb.CopyOperation_CopyTableToHistoryMode:
		fromTable := entity.CopyTableToHistoryMode.GetFromTable()
		toTable := entity.CopyTableToHistoryMode.GetToTable()
		softDeletedCol := entity.CopyTableToHistoryMode.GetSoftDeletedColumn() // optional per proto
		log.Info(fmt.Sprintf("[Migrate] Copying table %s.%s to history mode %s.%s", schema, fromTable, schema, toTable))

		if fromTable == "" {
			return FailedMigrateResponse(schema, table, fmt.Errorf("copy_table_to_history_mode.from_table is required")), nil
		}
		if toTable == "" {
			return FailedMigrateResponse(schema, table, fmt.Errorf("copy_table_to_history_mode.to_table is required")), nil
		}
		err := conn.MigrateCopyTableToHistoryMode(ctx, schema, fromTable, toTable, softDeletedCol)
		if err != nil {
			return FailedMigrateResponse(schema, table, err), nil
		}
		log.Info(fmt.Sprintf("[Migrate] Copied table %s.%s to history mode %s.%s", schema, fromTable, schema, toTable))
		return SuccessfulMigrateResponse(), nil

	default:
		err := fmt.Errorf("unsupported copy operation entity: %T", copy.GetEntity())
		return FailedMigrateResponse(schema, table, err), nil
	}
}

func handleRenameOperation(
	ctx context.Context,
	conn *db.ClickHouseConnection,
	schema string,
	table string,
	rename *pb.RenameOperation,
) (*pb.MigrateResponse, error) {
	switch entity := rename.GetEntity().(type) {
	case *pb.RenameOperation_RenameTable:
		fromTable := entity.RenameTable.GetFromTable()
		toTable := entity.RenameTable.GetToTable()
		log.Info(fmt.Sprintf("[Migrate] Renaming table %s.%s to %s.%s", schema, fromTable, schema, toTable))

		if fromTable == "" {
			return FailedMigrateResponse(schema, table, fmt.Errorf("rename_table.from_table is required")), nil
		}
		if toTable == "" {
			return FailedMigrateResponse(schema, table, fmt.Errorf("rename_table.to_table is required")), nil
		}
		err := conn.RenameTable(ctx, schema, fromTable, toTable)
		if err != nil {
			return FailedMigrateResponse(schema, table, err), nil
		}
		log.Info(fmt.Sprintf("[Migrate] Renamed table %s.%s to %s.%s", schema, fromTable, schema, toTable))
		return SuccessfulMigrateResponse(), nil

	case *pb.RenameOperation_RenameColumn:
		fromCol := entity.RenameColumn.GetFromColumn()
		toCol := entity.RenameColumn.GetToColumn()
		log.Info(fmt.Sprintf("[Migrate] Renaming column %s to %s in %s.%s", fromCol, toCol, schema, table))

		if fromCol == "" {
			return FailedMigrateResponse(schema, table, fmt.Errorf("rename_column.from_column is required")), nil
		}
		if toCol == "" {
			return FailedMigrateResponse(schema, table, fmt.Errorf("rename_column.to_column is required")), nil
		}
		err := conn.RenameColumn(ctx, schema, table, fromCol, toCol)
		if err != nil {
			return FailedMigrateResponse(schema, table, err), nil
		}
		log.Info(fmt.Sprintf("[Migrate] Renamed column %s to %s in %s.%s", fromCol, toCol, schema, table))
		return SuccessfulMigrateResponse(), nil

	default:
		err := fmt.Errorf("unsupported rename operation entity: %T", rename.GetEntity())
		return FailedMigrateResponse(schema, table, err), nil
	}
}

func handleAddOperation(
	ctx context.Context,
	conn *db.ClickHouseConnection,
	schema string,
	table string,
	add *pb.AddOperation,
) (*pb.MigrateResponse, error) {
	switch entity := add.GetEntity().(type) {
	case *pb.AddOperation_AddColumnWithDefaultValue:
		col := entity.AddColumnWithDefaultValue.GetColumn()
		colType := entity.AddColumnWithDefaultValue.GetColumnType()
		defaultValue := entity.AddColumnWithDefaultValue.GetDefaultValue() // pass-through; "" is a valid default
		log.Info(fmt.Sprintf("[Migrate] Adding column %s (%s) with default '%s' to %s.%s",
			col, colType.String(), defaultValue, schema, table))

		if col == "" {
			return FailedMigrateResponse(schema, table, fmt.Errorf("add_column_with_default_value.column is required")), nil
		}
		chType, err := migrateColumnType(colType)
		if err != nil {
			return FailedMigrateResponse(schema, table, err), nil
		}
		defaultMV, err := values.NewMigrateValue(colType, defaultValue)
		if err != nil {
			return FailedMigrateResponse(schema, table, err), nil
		}
		err = conn.MigrateAddColumnWithDefault(ctx, schema, table, col, chType.Type, chType.Comment, defaultMV)
		if err != nil {
			return FailedMigrateResponse(schema, table, err), nil
		}
		log.Info(fmt.Sprintf("[Migrate] Added column %s to %s.%s", col, schema, table))
		return SuccessfulMigrateResponse(), nil

	case *pb.AddOperation_AddColumnInHistoryMode:
		col := entity.AddColumnInHistoryMode.GetColumn()
		colType := entity.AddColumnInHistoryMode.GetColumnType()
		defaultValue := entity.AddColumnInHistoryMode.GetDefaultValue() // pass-through; "" is a valid default
		opTS := entity.AddColumnInHistoryMode.GetOperationTimestamp()
		log.Info(fmt.Sprintf("[Migrate] Adding column %s in history mode to %s.%s at %s", col, schema, table, opTS))

		if col == "" {
			return FailedMigrateResponse(schema, table, fmt.Errorf("add_column_in_history_mode.column is required")), nil
		}
		chType, err := migrateColumnType(colType)
		if err != nil {
			return FailedMigrateResponse(schema, table, err), nil
		}
		tsNanos, err := parseTimestampToNanos(opTS)
		if err != nil {
			return FailedMigrateResponse(schema, table, err), nil
		}
		defaultMV, err := values.NewMigrateValue(colType, defaultValue)
		if err != nil {
			return FailedMigrateResponse(schema, table, err), nil
		}
		err = conn.MigrateAddColumnInHistoryMode(ctx, schema, table, col, chType.Type, chType.Comment, defaultMV, tsNanos)
		if err != nil {
			return FailedMigrateResponse(schema, table, err), nil
		}
		log.Info(fmt.Sprintf("[Migrate] Added column %s in history mode to %s.%s", col, schema, table))
		return SuccessfulMigrateResponse(), nil

	default:
		err := fmt.Errorf("unsupported add operation entity: %T", add.GetEntity())
		return FailedMigrateResponse(schema, table, err), nil
	}
}

func handleUpdateColumnValue(
	ctx context.Context,
	conn *db.ClickHouseConnection,
	schema string,
	table string,
	op *pb.UpdateColumnValueOperation,
) (*pb.MigrateResponse, error) {
	col := op.GetColumn()
	value := op.GetValue() // pass-through; "" means NULL
	isNull := isSetColumnToNullValue(value)
	if isNull {
		log.Info(fmt.Sprintf("[Migrate] Setting column %s to NULL in %s.%s", col, schema, table))
	} else {
		log.Info(fmt.Sprintf("[Migrate] Updating column %s to '%s' in %s.%s", col, value, schema, table))
	}

	if col == "" {
		return FailedMigrateResponse(schema, table, fmt.Errorf("update_column_value.column is required")), nil
	}

	// UpdateColumnValueOperation carries no DataType in the proto (unlike
	// AddColumnWithDefaultValue, which does). To apply the same type-aware
	// formatting as the add-column path — critical for UTC_DATETIME, where
	// DateTime64(9,'UTC') needs an unambiguous nanosecond literal — we
	// recover the Fivetran DataType from the existing column metadata.
	//
	// This round-trip goes away once the upstream proto adds a DataType field
	// to UpdateColumnValueOperation (tracked in fivetran_partner_sdk).
	defaultVal, err := buildUpdateColumnMigrateValue(ctx, conn, schema, table, col, value, isNull)
	if err != nil {
		return FailedMigrateResponse(schema, table, err), nil
	}
	if err := conn.UpdateColumnValue(ctx, schema, table, col, defaultVal); err != nil {
		return FailedMigrateResponse(schema, table, err), nil
	}
	log.Info(fmt.Sprintf("[Migrate] Updated column %s in %s.%s", col, schema, table))
	return SuccessfulMigrateResponse(), nil
}

// isSetColumnToNullValue reports whether an UpdateColumnValueOperation.Value
// should be treated as SQL NULL.
//
// The SDK tester's set_column_to_null scenario arrives as an
// UpdateColumnValueOperation with value == "" or value == "NULL" (the literal
// four-character string, not the SQL keyword). Both must be treated as null
// per AGENTS.md; anything else is a real value.
func isSetColumnToNullValue(value string) bool {
	return value == "" || value == "NULL"
}

// buildUpdateColumnMigrateValue recovers the column's Fivetran DataType by
// describing the destination table, then formats value accordingly. When the
// column's ClickHouse type can't be mapped back to a Fivetran type (e.g. the
// table was created outside this connector), it falls back to the pre-fix
// behavior of treating value as an opaque quoted literal.
func buildUpdateColumnMigrateValue(
	ctx context.Context,
	conn *db.ClickHouseConnection,
	schema string,
	table string,
	col string,
	value string,
	isNull bool,
) (values.MigrateValue, error) {
	if isNull {
		return values.NewMigrateValueNull(), nil
	}
	tableDesc, err := conn.DescribeTable(ctx, schema, table)
	if err != nil {
		return values.MigrateValue{}, err
	}
	colDef := tableDesc.Mapping[col]
	if colDef == nil {
		return values.MigrateValue{}, fmt.Errorf("column %s does not exist in %s.%s", col, schema, table)
	}
	colType, _, typeErr := dt.ToFivetranDataType(colDef.Type, colDef.Comment, colDef.DecimalParams)
	if typeErr != nil {
		log.Warn(fmt.Sprintf(
			"[Migrate] Could not recover Fivetran type for %s.%s.%s (%s); falling back to untyped literal: %v",
			schema, table, col, colDef.Type, typeErr))
		return values.NewMigrateValueQuoted(value), nil
	}
	return values.NewMigrateValue(colType, value)
}

func handleTableSyncModeMigration(
	ctx context.Context,
	conn *db.ClickHouseConnection,
	schema string,
	table string,
	op *pb.TableSyncModeMigrationOperation,
) (*pb.MigrateResponse, error) {
	migrationType := op.GetType()
	log.Info(fmt.Sprintf("[Migrate] Sync mode migration %s for %s.%s", migrationType.String(), schema, table))

	switch migrationType {
	case pb.TableSyncModeMigrationType_SOFT_DELETE_TO_HISTORY:
		softDeletedCol := resolveSoftDeletedColumn(op.GetSoftDeletedColumn())
		err := conn.MigrateSoftDeleteToHistory(ctx, schema, table, softDeletedCol)
		if err != nil {
			return FailedMigrateResponse(schema, table, err), nil
		}
		log.Info(fmt.Sprintf("[Migrate] Converted %s.%s from soft-delete to history mode", schema, table))
		return SuccessfulMigrateResponse(), nil

	case pb.TableSyncModeMigrationType_HISTORY_TO_SOFT_DELETE:
		softDeletedCol := resolveSoftDeletedColumn(op.GetSoftDeletedColumn())
		keepDeletedRows := op.GetKeepDeletedRows()
		err := conn.MigrateHistoryToSoftDelete(ctx, schema, table, softDeletedCol, keepDeletedRows)
		if err != nil {
			return FailedMigrateResponse(schema, table, err), nil
		}
		log.Info(fmt.Sprintf("[Migrate] Converted %s.%s from history to soft-delete mode", schema, table))
		return SuccessfulMigrateResponse(), nil

	case pb.TableSyncModeMigrationType_SOFT_DELETE_TO_LIVE,
		pb.TableSyncModeMigrationType_HISTORY_TO_LIVE,
		pb.TableSyncModeMigrationType_LIVE_TO_SOFT_DELETE,
		pb.TableSyncModeMigrationType_LIVE_TO_HISTORY:
		log.Info(fmt.Sprintf("[Migrate] Unsupported sync mode migration: %s", migrationType.String()))
		return UnsupportedMigrateResponse(), nil

	default:
		err := fmt.Errorf("unknown sync mode migration type: %s", migrationType.String())
		return FailedMigrateResponse(schema, table, err), nil
	}
}

// TableSyncModeMigrationOperation.soft_deleted_column is optional in
// the proto; when the producer omits it, we fall back to the canonical Fivetran
// metadata column name.
func resolveSoftDeletedColumn(softDeletedCol string) string {
	if softDeletedCol == "" {
		return constants.FivetranDeleted
	}
	return softDeletedCol
}

// migrateColumnType converts a Fivetran DataType to a Nullable ClickHouse type for migration columns.
// Migration-added columns are always Nullable since they are not primary keys.
func migrateColumnType(colType pb.DataType) (dt.ClickHouseType, error) {
	chType, ok := dt.FivetranToClickHouseType[colType]
	if !ok {
		chType, ok = dt.FivetranToClickHouseTypeWithComment[colType]
		if !ok {
			return dt.ClickHouseType{}, fmt.Errorf("unknown datatype %s", colType.String())
		}
	}
	if !strings.HasPrefix(chType.Type, "Nullable(") {
		chType.Type = fmt.Sprintf("Nullable(%s)", chType.Type)
	}
	return chType, nil
}

// parseTimestampToNanos parses an ISO 8601 timestamp string and returns nanoseconds since epoch as a string.
func parseTimestampToNanos(ts string) (string, error) {
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		t, err = time.Parse("2006-01-02T15:04:05Z", ts)
		if err != nil {
			return "", fmt.Errorf("failed to parse operation timestamp %s: %w", ts, err)
		}
	}
	return strconv.FormatInt(t.UnixNano(), 10), nil
}
