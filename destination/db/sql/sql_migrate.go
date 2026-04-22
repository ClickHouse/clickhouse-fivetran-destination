package sql

import (
	"fmt"
	"strconv"
	"strings"

	constants "fivetran.com/fivetran_sdk/destination/common/constants"
	"fivetran.com/fivetran_sdk/destination/common/types"
	"fivetran.com/fivetran_sdk/destination/db/values"
)

// GetRenameColumnStatement generates: ALTER TABLE `schema`.`table` RENAME COLUMN `from` TO `to`
// This is an instant metadata-only operation in ClickHouse.
func GetRenameColumnStatement(schemaName string, tableName string, fromColumn string, toColumn string) (string, error) {
	fullTableName, err := GetQualifiedTableName(schemaName, tableName)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", fullTableName, identifier(fromColumn), identifier(toColumn)), nil
}

// GetUpdateColumnValueStatement generates: ALTER TABLE `schema`.`table` UPDATE `column` = <value> WHERE true
// If isNull is true, the value is set to NULL (unquoted).
// Otherwise the value is used as a quoted string literal.
// This is a mutation (background rewrite) in ClickHouse.
func GetUpdateColumnValueStatement(schemaName string, tableName string, column string, value string, isNull bool) (string, error) {
	fullTableName, err := GetQualifiedTableName(schemaName, tableName)
	if err != nil {
		return "", err
	}
	var sqlValue string
	if isNull {
		sqlValue = "NULL"
	} else {
		sqlValue = fmt.Sprintf("'%s'", escapeSQLString(value))
	}
	return fmt.Sprintf("ALTER TABLE %s UPDATE %s = %s WHERE true", fullTableName, identifier(column), sqlValue), nil
}

// GetUpdateRowsAtOperationTimestampStatement generates:
// ALTER TABLE `schema`.`table`
// UPDATE `column` = <value>
// WHERE `_fivetran_start` = '<operation_timestamp>'
//
// This follows the Schema Migration Helper guide step that updates rows at operation_timestamp
// to make same-timestamp history operations composable.
func GetUpdateRowsAtOperationTimestampStatement(
	schemaName string,
	tableName string,
	column string,
	value string,
	isNull bool,
	operationTimestampNanos string,
) (string, error) {
	fullTableName, err := GetQualifiedTableName(schemaName, tableName)
	if err != nil {
		return "", err
	}
	var sqlValue string
	if isNull {
		sqlValue = "NULL"
	} else {
		sqlValue = fmt.Sprintf("'%s'", escapeSQLString(value))
	}
	return fmt.Sprintf(
		"ALTER TABLE %s UPDATE %s = %s WHERE %s = '%s'",
		fullTableName,
		identifier(column),
		sqlValue,
		identifier(constants.FivetranStart),
		operationTimestampNanos,
	), nil
}

// GetCopyColumnUpdateStatement generates: ALTER TABLE `schema`.`table` UPDATE `toColumn` = `fromColumn` WHERE true
// Used for copying data from one column to another within the same table.
func GetCopyColumnUpdateStatement(schemaName string, tableName string, toColumn string, fromColumn string) (string, error) {
	fullTableName, err := GetQualifiedTableName(schemaName, tableName)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("ALTER TABLE %s UPDATE %s = %s WHERE true", fullTableName, identifier(toColumn), identifier(fromColumn)), nil
}

// GetCreateTableAsStatement generates: CREATE TABLE `schema`.`toTable` AS `schema`.`fromTable`
// Clones the table structure and engine settings.
func GetCreateTableAsStatement(schemaName string, fromTable string, toTable string) (string, error) {
	fromIdentifier := fmt.Sprintf("%s.%s", identifier(schemaName), identifier(fromTable))
	toIdentifier := fmt.Sprintf("%s.%s", identifier(schemaName), identifier(toTable))
	return fmt.Sprintf("CREATE TABLE %s AS %s", toIdentifier, fromIdentifier), nil
}

// GetCloseActiveRowsStatement generates:
//
//		ALTER TABLE `schema`.`table`
//	   UPDATE `_fivetran_active` = FALSE,
//	          `_fivetran_end` = '<timestamp - 1ms>'
//		WHERE `_fivetran_active` = true
//	   AND `_fivetran_start` < '<timestamp>'
//	   AND `columnFilter` IS NOT NULL -- appended if columnFilter is non-empty (used for DROP_COLUMN_IN_HISTORY_MODE)
//
// The _fivetran_end is set to operation_timestamp - 1 millisecond (1,000,000 nanoseconds),
// matching the Fivetran SDK spec and the existing WriteHistoryBatch behavior.
// Used for history mode operations to close out active rows before an operation timestamp.
func GetCloseActiveRowsStatement(
	schemaName string,
	tableName string,
	operationTimestampNanos string,
	columnFilter string,
) (string, error) {
	fullTableName, err := GetQualifiedTableName(schemaName, tableName)
	if err != nil {
		return "", err
	}
	endTimestampNanos, err := subtractOneMillisecond(operationTimestampNanos)
	if err != nil {
		return "", fmt.Errorf("invalid operation timestamp %s: %w", operationTimestampNanos, err)
	}
	whereSuffix := ""
	if columnFilter != "" {
		whereSuffix = fmt.Sprintf(" AND %s IS NOT NULL", identifier(columnFilter))
	}
	return fmt.Sprintf(
		"ALTER TABLE %s UPDATE %s = FALSE, %s = '%s' WHERE %s = true AND %s < '%s'%s",
		fullTableName,
		identifier(constants.FivetranActive),
		identifier(constants.FivetranEnd),
		endTimestampNanos,
		identifier(constants.FivetranActive),
		identifier(constants.FivetranStart),
		operationTimestampNanos,
		whereSuffix,
	), nil
}

// GetInsertNewActiveVersionsStatement generates a statement that inserts new active versions
// of all currently active rows. Used for history mode add/drop column operations.
//
// Query to build:
// INSERT INTO {schema.table} (<column_list>)
// (
//
//	SELECT
//	  <unchanged_cols>,
//	  {override_value} as {override_column_name},
//	  {operation_timestamp} as _fivetran_start
//	FROM {schema.table}
//	WHERE
//	    _fivetran_active
//	    AND {column_name} IS NOT NULL
//	    AND _fivetran_start < {operation_timestamp}
//
// );
//
// overrideColumn is the single column being added or dropped:
//   - For ADD_COLUMN_IN_HISTORY_MODE: override_value is the default value (quoted literal).
//   - For DROP_COLUMN_IN_HISTORY_MODE: override_value is nil (NULL), and the WHERE clause
//     adds AND `overrideColumn` IS NOT NULL to skip rows already lacking the column value.
//
// columns is the full ordered column list from DescribeTable (including history columns);
// Fivetran history metadata columns are filtered out internally.
//
// All other columns are copied as-is from the existing active rows.
func GetInsertNewActiveVersionsStatement(
	schemaName string,
	tableName string,
	columns []*types.ColumnDefinition,
	overrideColumn string,
	overrideValue *string,
	operationTimestampNanos string,
) (string, error) {
	fullTableName, err := GetQualifiedTableName(schemaName, tableName)
	if err != nil {
		return "", err
	}
	// Preparation: Get only table columns, excluding columns we'll re-add with new values:
	var tableColNames []string
	for _, col := range columns {
		if col == nil {
			continue
		}
		n := col.Name
		if n == constants.FivetranSynced || n == constants.FivetranStart ||
			n == constants.FivetranEnd || n == constants.FivetranActive {
			continue
		}
		tableColNames = append(tableColNames, n)
	}
	if len(tableColNames) == 0 {
		return "", fmt.Errorf("column names list is empty")
	}

	// Step 1: Build the INSERT INTO part: INSERT INTO {schema.table} (<column_list>)
	var insertColumnList []string
	for _, col := range tableColNames {
		if col != overrideColumn {
			insertColumnList = append(insertColumnList, identifier(col))
		}
	}
	insertColumnList = append(insertColumnList,
		identifier(overrideColumn),
		identifier(constants.FivetranSynced),
		identifier(constants.FivetranStart),
		identifier(constants.FivetranEnd),
		identifier(constants.FivetranActive),
	)

	insertColumns := strings.Join(insertColumnList, ",")

	insertIntoPart := fmt.Sprintf("INSERT INTO %s (%s)", fullTableName, insertColumns)

	// Step 2: Build the SELECT and FROM parts:
	// SELECT <unchanged_cols>, {override_value} as {override_column_name}, {operation_timestamp} as _fivetran_start FROM {schema.table}

	var selectParts []string
	for _, col := range tableColNames {
		if col != overrideColumn {
			selectParts = append(selectParts, identifier(col))
		}
	}

	var overrideValueText string
	if overrideValue == nil {
		overrideValueText = "NULL"
	} else {
		overrideValueText = fmt.Sprintf("'%s'", escapeSQLString(*overrideValue))
	}

	selectParts = append(selectParts,
		overrideValueText, // expected position for override value.
		identifier(constants.FivetranSynced),
		fmt.Sprintf("'%s'", operationTimestampNanos),   // FivetranStart
		fmt.Sprintf("'%s'", values.MaxDateTime64Nanos), // FivetranEnd
		"true", // FivetranActive
	)

	selectExprs := strings.Join(selectParts, ",")

	selectFromPart := fmt.Sprintf("SELECT %s FROM %s FINAL",
		selectExprs,
		fullTableName,
	)

	// Step 3: Build the WHERE part: WHERE _fivetran_active  AND _fivetran_start < {operation_timestamp}
	// For DROP: only insert new versions for rows where the column actually has a value (AND {column_name} IS NOT NULL)

	wherePart := fmt.Sprintf("WHERE `_fivetran_active` AND `_fivetran_start` < '%s'", operationTimestampNanos)

	if overrideValue == nil {
		wherePart += fmt.Sprintf(" AND %s IS NOT NULL", identifier(overrideColumn))
	}

	return fmt.Sprintf(
		"%s %s %s",
		insertIntoPart,
		selectFromPart,
		wherePart,
	), nil
}

// GetInsertFromSelectWithHistoryColumnsStatement generates a statement for copying a table
// from soft-delete mode to history mode. It computes _fivetran_start, _fivetran_end, _fivetran_active
// from the source columns.
func GetInsertFromSelectWithHistoryColumnsStatement(
	schemaName string,
	fromTable string,
	toTable string,
	colNames []string,
	softDeletedColumn string,
) (string, error) {
	if len(colNames) == 0 {
		return "", fmt.Errorf("column names list is empty")
	}

	fromIdentifier := fmt.Sprintf("%s.%s", identifier(schemaName), identifier(fromTable))
	toIdentifier := fmt.Sprintf("%s.%s", identifier(schemaName), identifier(toTable))

	var colsList strings.Builder
	for i, col := range colNames {
		colsList.WriteString(identifier(col))
		if i < len(colNames)-1 {
			colsList.WriteString(",")
		}
	}
	cols := colsList.String()

	// Build the _fivetran_active expression
	var activeExpr string
	if softDeletedColumn != "" {
		activeExpr = fmt.Sprintf("if(%s = 0, true, false)", identifier(softDeletedColumn))
	} else {
		activeExpr = "true"
	}

	return fmt.Sprintf(
		"INSERT INTO %s (%s,%s,%s,%s,%s) SELECT %s,%s,%s,'%s',%s FROM %s FINAL",
		toIdentifier,
		cols,
		identifier(constants.FivetranSynced),
		identifier(constants.FivetranStart),
		identifier(constants.FivetranEnd),
		identifier(constants.FivetranActive),
		cols,
		identifier(constants.FivetranSynced),
		identifier(constants.FivetranSynced), // _fivetran_start = _fivetran_synced
		values.MaxDateTime64Nanos,            // _fivetran_end = max
		activeExpr,                           // _fivetran_active
		fromIdentifier,
	), nil
}

// GetInsertFromSelectHistoryToSoftDeleteStatement generates SQL for HISTORY_TO_SOFT_DELETE.
//
// Why the "latest row per PK" subquery is needed:
// A history-mode table stores multiple versions per primary key (different _fivetran_start values).
// During conversion to soft-delete mode, we must keep only one row per PK (the latest version),
// then derive the soft-delete flag from that row's _fivetran_active value.
//
// If we selected directly from the history table, keepDeletedRows=true would copy all versions
// for each PK, which violates the migration contract (latest version only).
//
// ClickHouse adaptation:
// We use "ORDER BY <pk>, _fivetran_start DESC LIMIT 1 BY <pk>" over FINAL to select the latest
// version per PK in a single statement.
//
// Shape of generated SQL:
//
//	INSERT INTO <schema.to_table> (<cols>, `_fivetran_synced`, <soft_deleted_column>)
//	SELECT <cols>, `_fivetran_synced`, if(`_fivetran_active` = true, false, true)
//	FROM (
//	    SELECT <cols>, `_fivetran_synced`, `_fivetran_active`
//	    FROM <schema.from_table> FINAL
//	    ORDER BY <pk_cols>, `_fivetran_start` DESC
//	    LIMIT 1 BY <pk_cols>
//	)
//	[WHERE `_fivetran_active` = true] -- appended only when keepDeletedRows = false
func GetInsertFromSelectHistoryToSoftDeleteStatement(
	schemaName string,
	fromTable string,
	toTable string,
	colNames []string,
	pkColNames []string,
	softDeletedColumn string,
	keepDeletedRows bool,
) (string, error) {
	if len(colNames) == 0 {
		return "", fmt.Errorf("column names list is empty")
	}
	if len(pkColNames) == 0 {
		return "", fmt.Errorf("primary key column names list is empty")
	}

	fromIdentifier := fmt.Sprintf("%s.%s", identifier(schemaName), identifier(fromTable))
	toIdentifier := fmt.Sprintf("%s.%s", identifier(schemaName), identifier(toTable))

	var colsList strings.Builder
	for i, col := range colNames {
		colsList.WriteString(identifier(col))
		if i < len(colNames)-1 {
			colsList.WriteString(",")
		}
	}
	cols := colsList.String()

	var pkColsList strings.Builder
	for i, col := range pkColNames {
		pkColsList.WriteString(identifier(col))
		if i < len(pkColNames)-1 {
			pkColsList.WriteString(",")
		}
	}
	pkCols := pkColsList.String()

	deletedExpr := fmt.Sprintf("if(%s = true, false, true)", identifier(constants.FivetranActive))

	var whereClause string
	if !keepDeletedRows {
		whereClause = fmt.Sprintf(" WHERE %s = true", identifier(constants.FivetranActive))
	}

	latestPerPKSubquery := fmt.Sprintf(
		"SELECT %s,%s,%s FROM %s FINAL ORDER BY %s,%s DESC LIMIT 1 BY %s",
		cols,
		identifier(constants.FivetranSynced),
		identifier(constants.FivetranActive),
		fromIdentifier,
		pkCols,
		identifier(constants.FivetranStart),
		pkCols,
	)

	return fmt.Sprintf(
		"INSERT INTO %s (%s,%s,%s) SELECT %s,%s,%s FROM (%s)%s",
		toIdentifier,
		cols,
		identifier(constants.FivetranSynced),
		identifier(softDeletedColumn),
		cols,
		identifier(constants.FivetranSynced),
		deletedExpr,
		latestPerPKSubquery,
		whereClause,
	), nil
}

// subtractOneMillisecond subtracts 1 millisecond (1,000,000 nanoseconds) from a nanosecond timestamp string.
func subtractOneMillisecond(nanosStr string) (string, error) {
	nanos, err := strconv.ParseInt(nanosStr, 10, 64)
	if err != nil {
		return "", err
	}
	return strconv.FormatInt(nanos-1_000_000, 10), nil
}

// GetTableRowCountQuery generates a query to check if a table has any rows.
func GetTableRowCountQuery(schemaName string, tableName string) (string, error) {
	fullTableName, err := GetQualifiedTableName(schemaName, tableName)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("SELECT count() FROM %s LIMIT 1", fullTableName), nil
}

// GetMaxFivetranStartQuery generates a query to get the max _fivetran_start value for active rows.
func GetMaxFivetranStartQuery(schemaName string, tableName string) (string, error) {
	fullTableName, err := GetQualifiedTableName(schemaName, tableName)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("SELECT max(%s) FROM %s WHERE %s = true",
		identifier(constants.FivetranStart), fullTableName, identifier(constants.FivetranActive)), nil
}
