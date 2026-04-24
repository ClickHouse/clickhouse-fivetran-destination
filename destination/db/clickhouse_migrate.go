package db

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"fivetran.com/fivetran_sdk/destination/common/constants"
	"fivetran.com/fivetran_sdk/destination/common/log"
	"fivetran.com/fivetran_sdk/destination/common/types"
	"fivetran.com/fivetran_sdk/destination/db/sql"
	"fivetran.com/fivetran_sdk/destination/db/values"
)

const (
	migrateRenameColumn      connectionOpType = "Migrate(RenameColumn)"
	migrateCopyTableCreate   connectionOpType = "Migrate(CopyTable, Create)"
	migrateCopyTableInsert   connectionOpType = "Migrate(CopyTable, Insert)"
	migrateCopyColumn        connectionOpType = "Migrate(CopyColumn)"
	migrateCopyColumnUpdate  connectionOpType = "Migrate(CopyColumn, Update)"
	migrateUpdateColumnValue connectionOpType = "Migrate(UpdateColumnValue)"
	migrateAddColumnDefault  connectionOpType = "Migrate(AddColumnDefault)"
	migrateHistoryInsert     connectionOpType = "Migrate(History, Insert)"
	migrateHistoryUpdate     connectionOpType = "Migrate(History, Update)"
	migrateHistoryClose      connectionOpType = "Migrate(History, Close)"
	migrateSyncModeInsert    connectionOpType = "Migrate(SyncMode, Insert)"
)

// execInsertNewActiveVersions runs the "insert new active history rows" INSERT used by the
// history-mode add/drop-column migrations.
func (conn *ClickHouseConnection) execInsertNewActiveVersions(
	ctx context.Context,
	schemaName string,
	tableName string,
	columns []*types.ColumnDefinition,
	column string,
	value values.MigrateValue,
	operationTimestampNanos string,
) error {
	stmt, err := sql.GetInsertNewActiveVersionsStatement(
		schemaName, tableName, columns, column, value, operationTimestampNanos)
	if err != nil {
		return err
	}
	return conn.ExecStatement(ctx, stmt, migrateHistoryInsert, true)
}

// execInsertFromSelectWithHistoryColumns runs the INSERT...SELECT used by sync-mode
// transitions that copy a source table into a history-mode target.
func (conn *ClickHouseConnection) execInsertFromSelectWithHistoryColumns(
	ctx context.Context,
	schemaName string,
	fromTable string,
	toTable string,
	colNames []string,
	softDeletedColumn string,
) error {
	stmt, err := sql.GetInsertFromSelectWithHistoryColumnsStatement(
		schemaName, fromTable, toTable, colNames, softDeletedColumn)
	if err != nil {
		return err
	}
	return conn.ExecStatement(ctx, stmt, migrateSyncModeInsert, true)
}

// closeActiveRows runs the ALTER TABLE UPDATE that closes old active history rows
// (setting _fivetran_active=FALSE and _fivetran_end=operation_timestamp-1). This is a
// true mutation, so it uses the full execMutation envelope. Pass column="" to close
// all active rows, or a column name to only close rows where that column IS NOT NULL.
func (conn *ClickHouseConnection) closeActiveRows(
	ctx context.Context,
	schemaName string,
	tableName string,
	operationTimestampNanos string,
	column string,
) error {
	stmt, err := sql.GetCloseActiveRowsStatement(schemaName, tableName, operationTimestampNanos, column)
	if err != nil {
		return err
	}
	return conn.execMutation(ctx, stmt, schemaName, tableName, migrateHistoryClose)
}

func (conn *ClickHouseConnection) RenameColumn(
	ctx context.Context,
	schemaName string,
	tableName string,
	fromColumn string,
	toColumn string,
) error {
	statement, err := sql.GetRenameColumnStatement(schemaName, tableName, fromColumn, toColumn)
	if err != nil {
		return err
	}
	return conn.ExecStatement(ctx, statement, migrateRenameColumn, false)
}

// UpdateColumnValue updates all rows in a column to the given value (which may be SQL NULL).
func (conn *ClickHouseConnection) UpdateColumnValue(
	ctx context.Context,
	schemaName string,
	tableName string,
	column string,
	value values.MigrateValue,
) error {
	statement, err := sql.GetUpdateColumnValueStatement(schemaName, tableName, column, value)
	if err != nil {
		return err
	}
	return conn.execMutation(ctx, statement, schemaName, tableName, migrateUpdateColumnValue)
}

func (conn *ClickHouseConnection) CopyColumnData(
	ctx context.Context,
	schemaName string,
	tableName string,
	fromColumn string,
	toColumn string,
) error {
	statement, err := sql.GetCopyColumnUpdateStatement(schemaName, tableName, toColumn, fromColumn)
	if err != nil {
		return err
	}
	return conn.execMutation(ctx, statement, schemaName, tableName, migrateCopyColumnUpdate)
}

// MigrateCopyTable implements the COPY_TABLE schema migration: create `toTable`
// with the same structure as `fromTable` and populate it from the source.
// This may use just CLONE in the future once these this is fixed:
// https://github.com/ClickHouse/ClickHouse/issues/78870
func (conn *ClickHouseConnection) MigrateCopyTable(
	ctx context.Context,
	schemaName string,
	fromTable string,
	toTable string,
) error {
	tableDesc, err := conn.DescribeTable(ctx, schemaName, fromTable)
	if err != nil {
		return err
	}
	colNames := make([]string, len(tableDesc.Columns))
	for i, col := range tableDesc.Columns {
		colNames[i] = col.Name
	}
	toTableQualified, err := sql.GetQualifiedTableName(schemaName, toTable)
	if err != nil {
		return err
	}
	if err = conn.DropTable(ctx, toTableQualified); err != nil {
		return err
	}
	createStmt, err := sql.GetCreateTableAsStatement(schemaName, fromTable, toTable)
	if err != nil {
		return err
	}
	if err = conn.ExecStatement(ctx, createStmt, migrateCopyTableCreate, false); err != nil {
		return err
	}
	return conn.execInsertFromSelect(ctx, schemaName, fromTable, toTable, colNames, migrateCopyTableInsert)
}

func (conn *ClickHouseConnection) MigrateCopyColumn(
	ctx context.Context,
	schemaName string,
	tableName string,
	fromColumn string,
	toColumn string,
) error {
	// Get the type of the source column
	tableDesc, err := conn.DescribeTable(ctx, schemaName, tableName)
	if err != nil {
		return err
	}
	srcCol, ok := tableDesc.Mapping[fromColumn]
	if !ok {
		return fmt.Errorf("source column %s not found in table %s.%s", fromColumn, schemaName, tableName)
	}
	// Add the target column with same type
	addOp := &types.AlterTableOp{
		Op:     types.AlterTableAdd,
		Column: toColumn,
		Type:   &srcCol.Type,
	}
	if srcCol.Comment != "" {
		addOp.Comment = &srcCol.Comment
	}
	if err := conn.execAlterTableOps(ctx, schemaName, tableName, []*types.AlterTableOp{addOp}, migrateCopyColumn); err != nil {
		return err
	}
	// Copy data
	return conn.CopyColumnData(ctx, schemaName, tableName, fromColumn, toColumn)
}

func (conn *ClickHouseConnection) MigrateAddColumnWithDefault(
	ctx context.Context,
	schemaName string,
	tableName string,
	column string,
	chType string,
	comment string,
	defaultValue values.MigrateValue,
) error {
	// Step 1: Add column. GetAlterTableStatement always emits ADD COLUMN IF NOT EXISTS,
	// which matches the Schema Migration Helper spec's requirement that re-sent
	// ADD_COLUMN_WITH_DEFAULT_VALUE requests skip the ADD but still run the UPDATE below.
	addOp := &types.AlterTableOp{
		Op:     types.AlterTableAdd,
		Column: column,
		Type:   &chType,
	}
	if comment != "" {
		addOp.Comment = &comment
	}
	if err := conn.execAlterTableOps(ctx, schemaName, tableName, []*types.AlterTableOp{addOp}, migrateAddColumnDefault); err != nil {
		return err
	}
	// Step 2: Set default value
	return conn.UpdateColumnValue(ctx, schemaName, tableName, column, defaultValue)
}

func (conn *ClickHouseConnection) MigrateUpdateRowsAtOperationTimestamp(
	ctx context.Context,
	schemaName string,
	tableName string,
	column string,
	value values.MigrateValue,
	operationTimestampNanos string,
) error {
	statement, err := sql.GetUpdateRowsAtOperationTimestampStatement(
		schemaName, tableName, column, value, operationTimestampNanos)
	if err != nil {
		return err
	}
	return conn.execMutation(ctx, statement, schemaName, tableName, migrateHistoryUpdate)
}

// validateHistoryModeTable checks preconditions for the history-tracking side of a schema
// migration — the INSERT new-active-versions + close-old-rows dance that records the DDL as a
// history entry. It gates those data steps only; callers that perform an unconditional schema
// change (e.g. ADD COLUMN IN HISTORY MODE) must run that step BEFORE consulting this function,
// because the spec's empty-table short-circuit applies to history tracking, not to the DDL
// itself.
//
// Returns valid=true when both preconditions hold:
//  1. Table is non-empty (history tracking has something to record).
//  2. max(_fivetran_start) for existing rows <= operation_timestamp (no stale operation).
//
// Returns valid=false, err=nil when the table is empty — the caller should skip its
// history-tracking steps. Returns valid=false, err!=nil when preconditions fail with an error
// (e.g. operation_timestamp is older than max _fivetran_start).
func (conn *ClickHouseConnection) validateHistoryModeTable(
	ctx context.Context,
	schemaName string,
	tableName string,
	operationTimestampNanos string,
) (bool, error) {
	// Check if table is empty
	countQuery, err := sql.GetTableRowCountQuery(schemaName, tableName)
	if err != nil {
		return false, err
	}
	rows, err := conn.ExecQuery(ctx, countQuery, describeTable, false)
	if err != nil {
		return false, err
	}
	defer rows.Close() //nolint:errcheck
	if !rows.Next() {
		return false, nil // empty result, skip
	}
	var count uint64
	if err = rows.Scan(&count); err != nil {
		return false, err
	}
	if count == 0 {
		log.Info(fmt.Sprintf("Table %s.%s is empty, skipping history mode operation", schemaName, tableName))
		return false, nil
	}

	// Validate max(_fivetran_start) <= operation_timestamp
	maxStartQuery, err := sql.GetMaxFivetranStartQuery(schemaName, tableName)
	if err != nil {
		return false, err
	}
	maxRows, err := conn.ExecQuery(ctx, maxStartQuery, describeTable, false)
	if err != nil {
		return false, err
	}
	defer maxRows.Close() //nolint:errcheck
	if maxRows.Next() {
		var maxStart time.Time
		if err = maxRows.Scan(&maxStart); err != nil {
			// If scan fails (e.g., no active rows), that's fine — skip validation
			log.Warn(fmt.Sprintf("Could not validate max _fivetran_start for %s.%s: %v", schemaName, tableName, err))
			return true, nil
		}
		opNanos, parseErr := strconv.ParseInt(operationTimestampNanos, 10, 64)
		if parseErr != nil {
			return false, fmt.Errorf("invalid operation timestamp: %w", parseErr)
		}
		if maxStart.UnixNano() > opNanos {
			return false, fmt.Errorf(
				"operation_timestamp (%s) is before max _fivetran_start (%s) in table %s.%s",
				operationTimestampNanos, maxStart.Format(time.RFC3339Nano), schemaName, tableName)
		}
	}
	return true, nil
}

// MigrateAddColumnInHistoryMode adds a column to a history-mode table and, when the table has
// existing active rows, closes them off and opens new versions carrying the default value so
// the column addition is reflected in the history.
func (conn *ClickHouseConnection) MigrateAddColumnInHistoryMode(
	ctx context.Context,
	schemaName string,
	tableName string,
	column string,
	chType string,
	comment string,
	defaultValue values.MigrateValue,
	operationTimestampNanos string,
) error {
	// Step 1: Add the column. This must happen regardless of whether the table has data.
	addOp := &types.AlterTableOp{
		Op:     types.AlterTableAdd,
		Column: column,
		Type:   &chType,
	}
	if comment != "" {
		addOp.Comment = &comment
	}
	if err := conn.execAlterTableOps(ctx, schemaName, tableName, []*types.AlterTableOp{addOp}, migrateAddColumnDefault); err != nil {
		return err
	}
	// Validate preconditions for the history-tracking steps below. An empty table has no rows
	// to maintain history for — we stop here, the schema change alone is the full migration.
	valid, err := conn.validateHistoryModeTable(ctx, schemaName, tableName, operationTimestampNanos)
	if err != nil {
		return err
	}
	if !valid {
		return nil
	}
	// Step 2: Column list for new active versions (filtering inside SQL builder)
	tableDesc, err := conn.DescribeTable(ctx, schemaName, tableName)
	if err != nil {
		return err
	}
	// Step 3: INSERT new active rows with default value for the new column
	if err := conn.execInsertNewActiveVersions(
		ctx, schemaName, tableName, tableDesc.Columns, column, defaultValue, operationTimestampNanos); err != nil {
		return err
	}
	// Step 4: Update rows at operation timestamp.
	// This follows the helper guide for same-timestamp composability.
	if err := conn.MigrateUpdateRowsAtOperationTimestamp(
		ctx, schemaName, tableName, column, defaultValue, operationTimestampNanos); err != nil {
		return err
	}
	// Step 5: Close old active rows
	return conn.closeActiveRows(ctx, schemaName, tableName, operationTimestampNanos, "")
}

// MigrateDropColumnInHistoryMode creates new history versions for all active rows with the column set to NULL,
// then closes the old active rows.
func (conn *ClickHouseConnection) MigrateDropColumnInHistoryMode(
	ctx context.Context,
	schemaName string,
	tableName string,
	column string,
	operationTimestampNanos string,
) error {
	// Validate preconditions: table must be non-empty, max(_fivetran_start) <= operation_timestamp
	valid, err := conn.validateHistoryModeTable(ctx, schemaName, tableName, operationTimestampNanos)
	if err != nil {
		return err
	}
	if !valid {
		return nil
	}
	// Step 1: Column list for insert (filtering inside SQL builder)
	tableDesc, err := conn.DescribeTable(ctx, schemaName, tableName)
	if err != nil {
		return err
	}
	// Step 2: INSERT new active rows with column = NULL (only for rows where column IS NOT NULL)
	if err := conn.execInsertNewActiveVersions(
		ctx, schemaName, tableName, tableDesc.Columns, column, values.NewMigrateValueNull(), operationTimestampNanos); err != nil {
		return err
	}
	// Step 3: Update rows at operation timestamp.
	// This handles chained migrations with the same timestamp where active rows may already
	// exist at _fivetran_start = operation_timestamp.
	if err := conn.MigrateUpdateRowsAtOperationTimestamp(
		ctx, schemaName, tableName, column, values.NewMigrateValueNull(), operationTimestampNanos); err != nil {
		return err
	}
	// Step 4: Close old active rows (only where column IS NOT NULL).
	// Note: per the spec, the column is NOT physically dropped — it is preserved to maintain
	// historical values. New active rows will have NULL for this column.
	return conn.closeActiveRows(ctx, schemaName, tableName, operationTimestampNanos, column)
}

// MigrateCopyTableToHistoryMode copies a soft-delete table to a new history mode table.
func (conn *ClickHouseConnection) MigrateCopyTableToHistoryMode(
	ctx context.Context,
	schemaName string,
	fromTable string,
	toTable string,
	softDeletedColumn string,
) error {
	// Step 1: Describe source table
	srcDesc, err := conn.DescribeTable(ctx, schemaName, fromTable)
	if err != nil {
		return err
	}
	// Step 2: Build new TableDescription for history mode.
	// Only reference softDeletedColumn if it actually exists in the source — tables
	// created with history_mode:false may not carry _fivetran_deleted at all, and
	// the map lookup also handles an empty softDeletedColumn naturally (Mapping[""]
	// cannot match because ClickHouse column names are never empty).
	actualSoftDeletedCol := ""
	if _, exists := srcDesc.Mapping[softDeletedColumn]; exists {
		actualSoftDeletedCol = softDeletedColumn
	}
	var newCols []*types.ColumnDefinition
	var colNames []string
	for _, col := range srcDesc.Columns {
		if col.Name == constants.FivetranDeleted || col.Name == actualSoftDeletedCol {
			continue // exclude _fivetran_deleted from the new table
		}
		if col.Name == constants.FivetranSynced {
			newCols = append(newCols, col)
			continue // don't add to colNames, handled separately
		}
		newCols = append(newCols, col)
		colNames = append(colNames, col.Name)
	}
	// Add history columns
	newCols = append(newCols,
		&types.ColumnDefinition{Name: constants.FivetranStart, Type: constants.DateTimeUTC, IsPrimaryKey: true},
		&types.ColumnDefinition{Name: constants.FivetranEnd, Type: fmt.Sprintf("%s(%s)", constants.Nullable, constants.DateTimeUTC)},
		&types.ColumnDefinition{Name: constants.FivetranActive, Type: fmt.Sprintf("%s(%s)", constants.Nullable, constants.Bool)},
	)
	newTableDesc := types.MakeTableDescription(newCols)
	// Step 3: Pre-drop any leftover and create the target table.
	toTableQualified, err := sql.GetQualifiedTableName(schemaName, toTable)
	if err != nil {
		return err
	}
	if err = conn.DropTable(ctx, toTableQualified); err != nil {
		return err
	}
	err = conn.CreateTable(ctx, schemaName, toTable, newTableDesc)
	if err != nil {
		return err
	}
	// Step 4: INSERT...SELECT with history columns
	return conn.execInsertFromSelectWithHistoryColumns(
		ctx, schemaName, fromTable, toTable, colNames, actualSoftDeletedCol)
}

// MigrateSoftDeleteToHistory converts a soft-delete table to history mode.
// This requires a full table rebuild because _fivetran_start must be added to ORDER BY.
func (conn *ClickHouseConnection) MigrateSoftDeleteToHistory(
	ctx context.Context,
	schemaName string,
	tableName string,
	softDeletedColumn string,
) error {
	unixMilli := time.Now().UnixMilli()
	newTableName := fmt.Sprintf("%s_new_%d", tableName, unixMilli)
	backupTableName := fmt.Sprintf("%s_backup_%d", tableName, unixMilli)

	// Step 1: Describe current table
	srcDesc, err := conn.DescribeTable(ctx, schemaName, tableName)
	if err != nil {
		return err
	}
	// Step 2: Build new TableDescription.
	// Only reference softDeletedColumn if it actually exists in the source. Per
	// the Partner SDK spec, this field is optional; a table created with
	// history_mode:false may not carry it at all, and Mapping[""] cannot match
	// because ClickHouse column names are never empty.
	actualSoftDeletedCol := ""
	if _, exists := srcDesc.Mapping[softDeletedColumn]; exists {
		actualSoftDeletedCol = softDeletedColumn
	}
	var newCols []*types.ColumnDefinition
	var colNames []string
	for _, col := range srcDesc.Columns {
		if col.Name == constants.FivetranDeleted || col.Name == actualSoftDeletedCol {
			continue
		}
		if col.Name == constants.FivetranSynced {
			newCols = append(newCols, col)
			continue
		}
		newCols = append(newCols, col)
		colNames = append(colNames, col.Name)
	}
	newCols = append(newCols,
		&types.ColumnDefinition{Name: constants.FivetranStart, Type: constants.DateTimeUTC, IsPrimaryKey: true},
		&types.ColumnDefinition{Name: constants.FivetranEnd, Type: fmt.Sprintf("%s(%s)", constants.Nullable, constants.DateTimeUTC)},
		&types.ColumnDefinition{Name: constants.FivetranActive, Type: fmt.Sprintf("%s(%s)", constants.Nullable, constants.Bool)},
	)
	newTableDesc := types.MakeTableDescription(newCols)

	// Step 3: Create new table
	err = conn.CreateTable(ctx, schemaName, newTableName, newTableDesc)
	if err != nil {
		return err
	}
	// Step 4: INSERT...SELECT with computed history columns
	if err := conn.execInsertFromSelectWithHistoryColumns(
		ctx, schemaName, tableName, newTableName, colNames, actualSoftDeletedCol); err != nil {
		return err
	}
	// Step 5: Rename tables
	if err := conn.RenameTable(ctx, schemaName, tableName, backupTableName); err != nil {
		return err
	}
	return conn.RenameTable(ctx, schemaName, newTableName, tableName)
}

// MigrateHistoryToSoftDelete converts a history mode table to soft-delete mode.
// This requires a full table rebuild because _fivetran_start must be removed from ORDER BY.
func (conn *ClickHouseConnection) MigrateHistoryToSoftDelete(
	ctx context.Context,
	schemaName string,
	tableName string,
	softDeletedColumn string,
	keepDeletedRows bool,
) error {
	unixMilli := time.Now().UnixMilli()
	newTableName := fmt.Sprintf("%s_new_%d", tableName, unixMilli)
	backupTableName := fmt.Sprintf("%s_backup_%d", tableName, unixMilli)

	// Step 1: Describe current table
	srcDesc, err := conn.DescribeTable(ctx, schemaName, tableName)
	if err != nil {
		return err
	}
	// Step 2: Build new TableDescription
	var newCols []*types.ColumnDefinition
	var colNames []string
	var pkColNames []string
	for _, col := range srcDesc.Columns {
		// Skip history columns
		if col.Name == constants.FivetranStart || col.Name == constants.FivetranEnd || col.Name == constants.FivetranActive {
			continue
		}
		if col.Name == constants.FivetranSynced {
			newCols = append(newCols, col)
			continue
		}
		newCols = append(newCols, col)
		colNames = append(colNames, col.Name)
		if col.IsPrimaryKey {
			pkColNames = append(pkColNames, col.Name)
		}
	}
	newCols = append(newCols, &types.ColumnDefinition{Name: softDeletedColumn, Type: constants.Bool})
	newTableDesc := types.MakeTableDescription(newCols)

	// Step 3: Create new table
	err = conn.CreateTable(ctx, schemaName, newTableName, newTableDesc)
	if err != nil {
		return err
	}
	// Step 4: INSERT...SELECT
	insertStmt, err := sql.GetInsertFromSelectHistoryToSoftDeleteStatement(
		schemaName, tableName, newTableName, colNames, pkColNames, softDeletedColumn, keepDeletedRows)
	if err != nil {
		return err
	}
	if err := conn.ExecStatement(ctx, insertStmt, migrateSyncModeInsert, true); err != nil {
		return err
	}
	// Step 5: Rename tables
	err = conn.RenameTable(ctx, schemaName, tableName, backupTableName)
	if err != nil {
		return err
	}
	return conn.RenameTable(ctx, schemaName, newTableName, tableName)
}
