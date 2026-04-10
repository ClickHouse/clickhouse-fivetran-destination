package db

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"fivetran.com/fivetran_sdk/destination/common/constants"
	"fivetran.com/fivetran_sdk/destination/common/flags"
	"fivetran.com/fivetran_sdk/destination/common/log"
	"fivetran.com/fivetran_sdk/destination/common/retry"
	"fivetran.com/fivetran_sdk/destination/common/types"
	"fivetran.com/fivetran_sdk/destination/db/sql"
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
	migrateHistoryClose      connectionOpType = "Migrate(History, Close)"
	migrateSyncModeInsert    connectionOpType = "Migrate(SyncMode, Insert)"
)

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

// UpdateColumnValue updates all rows in a column to the given value (or NULL if isNull).
// This is a mutation operation. The error handling pattern matches TruncateTable:
// if ExecStatement returns an incomplete-mutation error, WaitAllMutationsCompleted
// polls until the mutation finishes — returning nil means the mutation completed successfully.
func (conn *ClickHouseConnection) UpdateColumnValue(
	ctx context.Context,
	schemaName string,
	tableName string,
	column string,
	value string,
	isNull bool,
) error {
	statement, err := sql.GetUpdateColumnValueStatement(schemaName, tableName, column, value, isNull)
	if err != nil {
		return err
	}
	err = conn.WaitAllNodesAvailable(ctx, schemaName, tableName)
	if err != nil {
		log.Warn(fmt.Sprintf("Not all nodes available for %s.%s: %v", schemaName, tableName, err))
	}
	err = conn.ExecStatement(ctx, statement, migrateUpdateColumnValue, true)
	if err != nil {
		waitErr := conn.WaitAllMutationsCompleted(ctx, err, schemaName, tableName)
		if waitErr != nil {
			return waitErr
		}
	}
	return nil
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
	err = conn.WaitAllNodesAvailable(ctx, schemaName, tableName)
	if err != nil {
		log.Warn(fmt.Sprintf("Not all nodes available for %s.%s: %v", schemaName, tableName, err))
	}
	err = conn.ExecStatement(ctx, statement, migrateCopyColumnUpdate, true)
	if err != nil {
		waitErr := conn.WaitAllMutationsCompleted(ctx, err, schemaName, tableName)
		if waitErr != nil {
			return waitErr
		}
	}
	return nil
}

func (conn *ClickHouseConnection) MigrateCopyTable(
	ctx context.Context,
	schemaName string,
	fromTable string,
	toTable string,
) error {
	// Create target table with same structure
	createStmt, err := sql.GetCreateTableAsStatement(schemaName, fromTable, toTable)
	if err != nil {
		return err
	}
	err = conn.ExecStatement(ctx, createStmt, migrateCopyTableCreate, false)
	if err != nil {
		return err
	}
	// Get column names for the INSERT...SELECT
	tableDesc, err := conn.DescribeTable(ctx, schemaName, fromTable)
	if err != nil {
		return err
	}
	colNames := make([]string, len(tableDesc.Columns))
	for i, col := range tableDesc.Columns {
		colNames[i] = col.Name
	}
	insertStmt, err := sql.GetInsertFromSelectStatement(schemaName, fromTable, toTable, colNames)
	if err != nil {
		return err
	}
	err = conn.WaitAllNodesAvailable(ctx, schemaName, toTable)
	if err != nil {
		log.Warn(fmt.Sprintf("Not all nodes available for %s.%s: %v", schemaName, toTable, err))
	}
	return conn.ExecStatement(ctx, insertStmt, migrateCopyTableInsert, true)
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
	alterStmt, err := sql.GetAlterTableStatement(schemaName, tableName, []*types.AlterTableOp{addOp})
	if err != nil {
		return err
	}
	err = conn.ExecStatement(ctx, alterStmt, migrateCopyColumn, false)
	if err != nil {
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
	defaultValue string,
) error {
	// Step 1: Add column
	addOp := &types.AlterTableOp{
		Op:     types.AlterTableAdd,
		Column: column,
		Type:   &chType,
	}
	if comment != "" {
		addOp.Comment = &comment
	}
	alterStmt, err := sql.GetAlterTableStatement(schemaName, tableName, []*types.AlterTableOp{addOp})
	if err != nil {
		return err
	}
	err = conn.ExecStatement(ctx, alterStmt, migrateAddColumnDefault, false)
	if err != nil {
		return err
	}
	// Step 2: Set default value
	return conn.UpdateColumnValue(ctx, schemaName, tableName, column, defaultValue, false)
}

// waitMutationsOnTable polls system.mutations until all mutations on the given table are complete.
// Uses system.mutations directly (not clusterAllReplicas) so it works on both local Docker and ClickHouse Cloud.
func (conn *ClickHouseConnection) waitMutationsOnTable(
	ctx context.Context,
	schemaName string,
	tableName string,
) error {
	query, err := sql.GetLocalMutationsCompletedQuery(schemaName, tableName)
	if err != nil {
		return err
	}
	return retry.OnFalseWithFixedDelay(func() (bool, error) {
		allDone, queryErr := conn.ExecBoolQuery(ctx, query, allMutationsCompleted, false)
		if queryErr != nil {
			return false, queryErr
		}
		return allDone, nil
	}, ctx, query, *flags.MaxAsyncMutationsCheckRetries, *flags.AsyncMutationsCheckInterval)
}

// validateHistoryModeTable checks preconditions for history mode operations:
// 1. Table must not be empty (skip the operation if it is)
// 2. max(_fivetran_start) for active rows must be <= operation_timestamp
// Returns (valid, error). If valid is true, the table passes all preconditions and the caller should proceed.
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

// MigrateAddColumnInHistoryMode adds a column and creates new history versions for all active rows.
func (conn *ClickHouseConnection) MigrateAddColumnInHistoryMode(
	ctx context.Context,
	schemaName string,
	tableName string,
	column string,
	chType string,
	comment string,
	defaultValue string,
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
	// Step 1: Add column
	addOp := &types.AlterTableOp{
		Op:     types.AlterTableAdd,
		Column: column,
		Type:   &chType,
	}
	if comment != "" {
		addOp.Comment = &comment
	}
	alterStmt, err := sql.GetAlterTableStatement(schemaName, tableName, []*types.AlterTableOp{addOp})
	if err != nil {
		return err
	}
	err = conn.ExecStatement(ctx, alterStmt, migrateAddColumnDefault, false)
	if err != nil {
		return err
	}
	// Step 2: Column list for new active versions (filtering inside SQL builder)
	tableDesc, err := conn.DescribeTable(ctx, schemaName, tableName)
	if err != nil {
		return err
	}
	// Step 3: INSERT new active rows with default value for the new column
	insertStmt, err := sql.GetInsertNewActiveVersionsStatement(
		schemaName, tableName, tableDesc.Columns, column, &defaultValue, operationTimestampNanos)
	if err != nil {
		return err
	}
	err = conn.WaitAllNodesAvailable(ctx, schemaName, tableName)
	if err != nil {
		log.Warn(fmt.Sprintf("Not all nodes available for %s.%s: %v", schemaName, tableName, err))
	}
	err = conn.ExecStatement(ctx, insertStmt, migrateHistoryInsert, true)
	if err != nil {
		return err
	}
	// Step 4: Close old active rows
	closeStmt, err := sql.GetCloseActiveRowsStatement(schemaName, tableName, operationTimestampNanos, "")
	if err != nil {
		return err
	}
	err = conn.WaitAllNodesAvailable(ctx, schemaName, tableName)
	if err != nil {
		log.Warn(fmt.Sprintf("Not all nodes available for %s.%s: %v", schemaName, tableName, err))
	}
	err = conn.ExecStatement(ctx, closeStmt, migrateHistoryClose, true)
	if err != nil {
		waitErr := conn.WaitAllMutationsCompleted(ctx, err, schemaName, tableName)
		if waitErr != nil {
			return waitErr
		}
	}
	return nil
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
	insertStmt, err := sql.GetInsertNewActiveVersionsStatement(
		schemaName, tableName, tableDesc.Columns, column, nil, operationTimestampNanos)
	if err != nil {
		return err
	}
	err = conn.WaitAllNodesAvailable(ctx, schemaName, tableName)
	if err != nil {
		log.Warn(fmt.Sprintf("Not all nodes available for %s.%s: %v", schemaName, tableName, err))
	}
	err = conn.ExecStatement(ctx, insertStmt, migrateHistoryInsert, true)
	if err != nil {
		return err
	}
	// Note: Documentation mentions that the next thing to do is to "Update the newly added row with the operation_timestamp",
	// but we are already doing that within the replacing merge Tree logic.

	// Step 3: Close old active rows (only where column IS NOT NULL)
	closeStmt, err := sql.GetCloseActiveRowsStatement(schemaName, tableName, operationTimestampNanos, column)
	if err != nil {
		return err
	}
	err = conn.WaitAllNodesAvailable(ctx, schemaName, tableName)
	if err != nil {
		log.Warn(fmt.Sprintf("Not all nodes available for %s.%s: %v", schemaName, tableName, err))
	}
	err = conn.ExecStatement(ctx, closeStmt, migrateHistoryClose, true)
	if err != nil {
		waitErr := conn.WaitAllMutationsCompleted(ctx, err, schemaName, tableName)
		if waitErr != nil {
			return waitErr
		}
	}
	// Note: per the spec, the column is NOT physically dropped — it is preserved to maintain historical values.
	// New active rows will have NULL for this column.
	return nil
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
	// Step 2: Build new TableDescription for history mode
	// Check if softDeletedColumn actually exists in the source table
	actualSoftDeletedCol := ""
	if softDeletedColumn != "" {
		if _, exists := srcDesc.Mapping[softDeletedColumn]; exists {
			actualSoftDeletedCol = softDeletedColumn
		}
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
	// Step 3: Create target table
	err = conn.CreateTable(ctx, schemaName, toTable, newTableDesc)
	if err != nil {
		return err
	}
	// Step 4: INSERT...SELECT with history columns
	insertStmt, err := sql.GetInsertFromSelectWithHistoryColumnsStatement(
		schemaName, fromTable, toTable, colNames, actualSoftDeletedCol)
	if err != nil {
		return err
	}
	err = conn.WaitAllNodesAvailable(ctx, schemaName, toTable)
	if err != nil {
		log.Warn(fmt.Sprintf("Not all nodes available for %s.%s: %v", schemaName, toTable, err))
	}
	return conn.ExecStatement(ctx, insertStmt, migrateSyncModeInsert, true)
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
	// Step 2: Build new TableDescription
	// Check if softDeletedColumn actually exists in the source table
	actualSoftDeletedCol := ""
	if softDeletedColumn != "" {
		if _, exists := srcDesc.Mapping[softDeletedColumn]; exists {
			actualSoftDeletedCol = softDeletedColumn
		}
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
	insertStmt, err := sql.GetInsertFromSelectWithHistoryColumnsStatement(
		schemaName, tableName, newTableName, colNames, actualSoftDeletedCol)
	if err != nil {
		return err
	}
	err = conn.WaitAllNodesAvailable(ctx, schemaName, newTableName)
	if err != nil {
		log.Warn(fmt.Sprintf("Not all nodes available for %s.%s: %v", schemaName, newTableName, err))
	}
	err = conn.ExecStatement(ctx, insertStmt, migrateSyncModeInsert, true)
	if err != nil {
		return err
	}
	// Step 5: Rename tables
	err = conn.RenameTable(ctx, schemaName, tableName, backupTableName)
	if err != nil {
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
	}
	// Add _fivetran_deleted
	if softDeletedColumn == "" {
		softDeletedColumn = constants.FivetranDeleted
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
		schemaName, tableName, newTableName, colNames, softDeletedColumn, keepDeletedRows)
	if err != nil {
		return err
	}
	err = conn.WaitAllNodesAvailable(ctx, schemaName, newTableName)
	if err != nil {
		log.Warn(fmt.Sprintf("Not all nodes available for %s.%s: %v", schemaName, newTableName, err))
	}
	err = conn.ExecStatement(ctx, insertStmt, migrateSyncModeInsert, true)
	if err != nil {
		return err
	}
	// Step 5: Rename tables
	err = conn.RenameTable(ctx, schemaName, tableName, backupTableName)
	if err != nil {
		return err
	}
	return conn.RenameTable(ctx, schemaName, newTableName, tableName)
}
