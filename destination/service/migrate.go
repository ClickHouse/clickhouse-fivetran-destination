package service

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	dt "fivetran.com/fivetran_sdk/destination/common/data_types"
	"fivetran.com/fivetran_sdk/destination/common/log"
	"fivetran.com/fivetran_sdk/destination/db"
	"fivetran.com/fivetran_sdk/destination/db/config"
	"fivetran.com/fivetran_sdk/destination/db/sql"
	pb "fivetran.com/fivetran_sdk/proto"
)


func (s *Server) Migrate(ctx context.Context, in *pb.MigrateRequest) (*pb.MigrateResponse, error) {
	details := in.GetDetails()
	schema := details.GetSchema()
	table := details.GetTable()

	log.Info(fmt.Sprintf("[Migrate] Starting for %s.%s", schema, table))

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
		err := conn.MigrateCopyColumn(ctx, schema, table, fromCol, toCol)
		if err != nil {
			return FailedMigrateResponse(schema, table, err), nil
		}
		log.Info(fmt.Sprintf("[Migrate] Copied column %s to %s in %s.%s", fromCol, toCol, schema, table))
		return SuccessfulMigrateResponse(), nil

	case *pb.CopyOperation_CopyTableToHistoryMode:
		fromTable := entity.CopyTableToHistoryMode.GetFromTable()
		toTable := entity.CopyTableToHistoryMode.GetToTable()
		softDeletedCol := entity.CopyTableToHistoryMode.GetSoftDeletedColumn()
		log.Info(fmt.Sprintf("[Migrate] Copying table %s.%s to history mode %s.%s", schema, fromTable, schema, toTable))
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
		defaultValue := entity.AddColumnWithDefaultValue.GetDefaultValue()
		log.Info(fmt.Sprintf("[Migrate] Adding column %s (%s) with default '%s' to %s.%s",
			col, colType.String(), defaultValue, schema, table))

		chType, err := migrateColumnType(colType)
		if err != nil {
			return FailedMigrateResponse(schema, table, err), nil
		}
		formattedValue := formatMigrateValue(defaultValue, colType)
		err = conn.MigrateAddColumnWithDefault(ctx, schema, table, col, chType.Type, chType.Comment, formattedValue)
		if err != nil {
			return FailedMigrateResponse(schema, table, err), nil
		}
		log.Info(fmt.Sprintf("[Migrate] Added column %s to %s.%s", col, schema, table))
		return SuccessfulMigrateResponse(), nil

	case *pb.AddOperation_AddColumnInHistoryMode:
		col := entity.AddColumnInHistoryMode.GetColumn()
		colType := entity.AddColumnInHistoryMode.GetColumnType()
		defaultValue := entity.AddColumnInHistoryMode.GetDefaultValue()
		opTS := entity.AddColumnInHistoryMode.GetOperationTimestamp()
		log.Info(fmt.Sprintf("[Migrate] Adding column %s in history mode to %s.%s at %s", col, schema, table, opTS))

		chType, err := migrateColumnType(colType)
		if err != nil {
			return FailedMigrateResponse(schema, table, err), nil
		}
		tsNanos, err := parseTimestampToNanos(opTS)
		if err != nil {
			return FailedMigrateResponse(schema, table, err), nil
		}
		err = conn.MigrateAddColumnInHistoryMode(ctx, schema, table, col, chType.Type, chType.Comment, defaultValue, tsNanos)
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
	value := op.GetValue()
	isNull := value == "" || value == "NULL"
	if isNull {
		log.Info(fmt.Sprintf("[Migrate] Setting column %s to NULL in %s.%s", col, schema, table))
	} else {
		log.Info(fmt.Sprintf("[Migrate] Updating column %s to '%s' in %s.%s", col, value, schema, table))
	}
	err := conn.UpdateColumnValue(ctx, schema, table, col, value, isNull)
	if err != nil {
		return FailedMigrateResponse(schema, table, err), nil
	}
	log.Info(fmt.Sprintf("[Migrate] Updated column %s in %s.%s", col, schema, table))
	return SuccessfulMigrateResponse(), nil
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
		softDeletedCol := op.GetSoftDeletedColumn()
		if softDeletedCol == "" {
			softDeletedCol = "_fivetran_deleted"
		}
		err := conn.MigrateSoftDeleteToHistory(ctx, schema, table, softDeletedCol)
		if err != nil {
			return FailedMigrateResponse(schema, table, err), nil
		}
		log.Info(fmt.Sprintf("[Migrate] Converted %s.%s from soft-delete to history mode", schema, table))
		return SuccessfulMigrateResponse(), nil

	case pb.TableSyncModeMigrationType_HISTORY_TO_SOFT_DELETE:
		softDeletedCol := op.GetSoftDeletedColumn()
		if softDeletedCol == "" {
			softDeletedCol = "_fivetran_deleted"
		}
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

// formatMigrateValue formats a value for use in SQL UPDATE statements based on the Fivetran DataType.
// For datetime types, converts ISO 8601 to nanosecond timestamp format.
func formatMigrateValue(value string, colType pb.DataType) string {
	switch colType {
	case pb.DataType_UTC_DATETIME:
		t, err := time.Parse(time.RFC3339, value)
		if err != nil {
			// Try alternative format
			t, err = time.Parse("2006-01-02T15:04:05Z", value)
			if err != nil {
				return value
			}
		}
		return strconv.FormatInt(t.UnixNano(), 10)
	case pb.DataType_NAIVE_DATETIME:
		t, err := time.Parse("2006-01-02T15:04:05", value)
		if err != nil {
			return value
		}
		return t.Format("2006-01-02 15:04:05")
	case pb.DataType_NAIVE_DATE:
		t, err := time.Parse("2006-01-02", value)
		if err != nil {
			return value
		}
		return t.Format("2006-01-02")
	default:
		return value
	}
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
