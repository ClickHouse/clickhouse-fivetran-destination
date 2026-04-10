package sql

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	constants "fivetran.com/fivetran_sdk/destination/common/constants"
	"fivetran.com/fivetran_sdk/destination/common/types"
	"fivetran.com/fivetran_sdk/destination/db/values"
	pb "fivetran.com/fivetran_sdk/proto"
)

type QualifiedTableName string

func GetQualifiedTableName(schemaName string, tableName string) (QualifiedTableName, error) {
	if tableName == "" {
		return "", fmt.Errorf("table name is empty")
	}
	if schemaName == "" {
		return "", fmt.Errorf("schema name for table %s is empty", tableName)
	} else {
		return QualifiedTableName(fmt.Sprintf("%s.%s", identifier(schemaName), identifier(tableName))), nil
	}
}

// GetAlterTableStatement sample generated query:
//
//	ALTER TABLE `foo`.`bar`
//	ADD COLUMN `c1` String COMMENT 'foobar',
//	DROP COLUMN `c2`,
//	MODIFY COLUMN `c3` Int32 COMMENT ''
//
// Comments are added to distinguish certain Fivetran data types, see data_types.FivetranToClickHouseTypeWithComment.
func GetAlterTableStatement(schemaName string, tableName string, ops []*types.AlterTableOp) (string, error) {
	fullTableName, err := GetQualifiedTableName(schemaName, tableName)
	if err != nil {
		return "", err
	}
	if len(ops) == 0 {
		return "", fmt.Errorf("no statements to execute for altering table %s", fullTableName)
	}

	count := 0
	var statementsBuilder strings.Builder
	for _, op := range ops {
		switch op.Op {
		case types.AlterTableAdd:
			if op.Type == nil {
				return "", fmt.Errorf("type for column %s is not specified", op.Column)
			}
			statementsBuilder.WriteString(fmt.Sprintf("ADD COLUMN %s %s", identifier(op.Column), *op.Type))
			if op.Comment != nil {
				statementsBuilder.WriteString(fmt.Sprintf(" COMMENT '%s'", *op.Comment))
			}
		case types.AlterTableModify:
			if op.Type == nil {
				return "", fmt.Errorf("type for column %s is not specified", op.Column)
			}
			statementsBuilder.WriteString(fmt.Sprintf("MODIFY COLUMN %s %s", identifier(op.Column), *op.Type))
			if op.Comment != nil {
				statementsBuilder.WriteString(fmt.Sprintf(" COMMENT '%s'", *op.Comment))
			}
		case types.AlterTableDrop:
			statementsBuilder.WriteString(fmt.Sprintf("DROP COLUMN %s", identifier(op.Column)))
		}
		if count < len(ops)-1 {
			statementsBuilder.WriteString(",")
		}
		count++
	}

	statements := statementsBuilder.String()
	query := fmt.Sprintf("ALTER TABLE %s %s", fullTableName, statements)
	return query, nil
}

func GetCheckDatabaseExistsStatement(schemaName string) (string, error) {
	if schemaName == "" {
		return "", fmt.Errorf("schema name is empty")
	}
	return fmt.Sprintf("SELECT COUNT(*) FROM system.databases WHERE `name` = '%s'", schemaName), nil
}

func GetCreateDatabaseStatement(schemaName string) (string, error) {
	if schemaName == "" {
		return "", fmt.Errorf("schema name is empty")
	}
	return fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", identifier(schemaName)), nil
}

func GetDropTableStatement(tableName QualifiedTableName) (string, error) {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName), nil
}

func GetSelectFromSystemGrantsQuery(username string) (string, error) {
	if username == "" {
		return "", fmt.Errorf("username is empty")
	}
	return fmt.Sprintf(
		"SELECT `access_type`, `database`, `table`, `column` FROM system.grants WHERE `user_name` = '%s'",
		username,
	), nil
}

// GetCreateTableStatement sample generated query:
//
//	CREATE TABLE `foo`.`bar`
//	(`id` Int64, `c2` Nullable(String), `_fivetran_synced` DateTime64(9, 'UTC'), `_fivetran_deleted` Bool)
//	ENGINE = ReplacingMergeTree(`_fivetran_synced`)
//	ORDER BY (`id`)
func GetCreateTableStatement(
	schemaName string,
	tableName string,
	tableDescription *types.TableDescription,
) (string, error) {
	fullName, err := GetQualifiedTableName(schemaName, tableName)
	if err != nil {
		return "", err
	}
	if tableDescription == nil || len(tableDescription.Columns) == 0 {
		return "", fmt.Errorf("no columns to create table %s", fullName)
	}
	if len(tableDescription.PrimaryKeys) == 0 {
		return "", fmt.Errorf("no primary keys for table %s", fullName)
	}
	if tableDescription.Mapping[constants.FivetranSynced] == nil {
		return "", fmt.Errorf("no %s column for table %s", constants.FivetranSynced, fullName)
	}
	var orderByCols []string
	var columnsBuilder strings.Builder
	count := 0
	for _, col := range tableDescription.Columns {
		columnsBuilder.WriteString(fmt.Sprintf("%s %s", identifier(col.Name), col.Type))
		if col.Comment != "" {
			columnsBuilder.WriteString(fmt.Sprintf(" COMMENT '%s'", col.Comment))
		}
		if col.IsPrimaryKey {
			orderByCols = append(orderByCols, identifier(col.Name))
		}
		if count < len(tableDescription.Columns)-1 {
			columnsBuilder.WriteString(",")
		}
		count++
	}
	columns := columnsBuilder.String()

	query := fmt.Sprintf(
		"CREATE TABLE %s (%s) ENGINE = ReplacingMergeTree(%s) ORDER BY (%s)",
		fullName, columns, identifier(constants.FivetranSynced), strings.Join(orderByCols, ","))
	return query, nil
}

// GetTruncateTableStatement generates a query for either "soft" (ALTER TABLE UPDATE) or "hard" (ALTER TABLE DELETE) table truncation.
//
// Important: Fivetran uses milliseconds (not nanos) precision for the _fivetran_synced column.
//
// Even though syncedColumn and softDeletedColumn are known constants (_fivetran_synced, _fivetran_deleted)
// and it is guaranteed that they were present in CreateTableRequest (and, consequently, GetCreateTableStatement),
// TruncateTableRequest also defines their names.
//
// Additionally, softDeletedColumn is used to switch between "hard" (nil) and "soft" (not nil) truncation.
//
// Sample generated query (soft truncate):
//
//	ALTER TABLE `foo`.`bar` UPDATE `_fivetran_deleted` = 1
//	WHERE toUnixTimestamp64Milli(`_fivetran_synced`) <= '<truncateBeforeMilli>'
//
// Sample generated query (hard truncate):
//
//	ALTER TABLE `foo`.`bar` DELETE
//	WHERE toUnixTimestamp64Milli(`_fivetran_synced`) <= '<truncateBeforeMilli>'
func GetTruncateTableStatement(
	schemaName string,
	tableName string,
	syncedColumn string,
	truncateBefore time.Time,
	softDeletedColumn *string,
) (string, error) {
	fullName, err := GetQualifiedTableName(schemaName, tableName)
	if err != nil {
		return "", err
	}

	if syncedColumn == "" {
		return "", fmt.Errorf("synced column name is empty")
	}

	if truncateBefore.IsZero() {
		return "", fmt.Errorf("truncate before time is zero")
	}

	var query string

	syncedColumnMilli := toUnixTimestamp64Milli(identifier(syncedColumn))
	truncateBeforeMilli := truncateBefore.UnixMilli()

	if softDeletedColumn != nil && *softDeletedColumn != "" {
		query = fmt.Sprintf("ALTER TABLE %s UPDATE %s = 1 WHERE %s <= '%d'",
			fullName, identifier(*softDeletedColumn), syncedColumnMilli, truncateBeforeMilli)
	} else {
		query = fmt.Sprintf("ALTER TABLE %s DELETE WHERE %s <= '%d'",
			fullName, syncedColumnMilli, truncateBeforeMilli)
	}

	return query, nil
}

// GetColumnTypesQuery generates a query that will always return zero records.
// However, even though the result set is empty, column types still can be obtained via driver.Rows.ColumnTypes method.
func GetColumnTypesQuery(schemaName string, tableName string) (string, error) {
	fullName, err := GetQualifiedTableName(schemaName, tableName)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE false", fullName), nil
}

func GetDescribeTableQuery(schemaName string, tableName string) (string, error) {
	if schemaName == "" {
		return "", fmt.Errorf("schema name for table %s is empty", tableName)
	}
	if tableName == "" {
		return "", fmt.Errorf("table name is empty")
	}
	return fmt.Sprintf(
		"SELECT name, type, comment, is_in_primary_key, numeric_precision, numeric_scale FROM system.columns WHERE database = '%s' AND table = '%s'",
		schemaName, tableName), nil
}

// GetSelectByPrimaryKeysQuery converts a CSV slice + known primary key columns and their CSV cell indices to a SELECT FINAL query using values from CSV rows.
// Sample generated query:
//
//	SELECT * FROM `foo`.`bar` FINAL WHERE (`id`, `name`) IN ((42, 'foo'), (144, 'bar')) ORDER BY (`id`, `name`) LIMIT N
//
// Where N is the number of rows in the CSV slice.
func GetSelectByPrimaryKeysQuery(
	csv [][]string,
	csvColumns *types.CSVColumns,
	qualifiedTableName QualifiedTableName,
	isHistoryMode bool,
) (string, error) {
	if qualifiedTableName == "" {
		return "", fmt.Errorf("table name is empty")
	}
	if len(csv) == 0 {
		return "", fmt.Errorf("expected non-empty CSV slice for table %s", qualifiedTableName)
	}
	if csvColumns == nil || len(csvColumns.PrimaryKeys) == 0 {
		return "", fmt.Errorf("expected non-empty primary keys for table %s", qualifiedTableName)
	}
	var orderByBuilder strings.Builder
	orderByBuilder.WriteRune('(')
	var clauseBuilder strings.Builder
	clauseBuilder.WriteString(fmt.Sprintf("SELECT * FROM %s FINAL WHERE(", qualifiedTableName))

	if isHistoryMode {
		csvColumns.RemovePrimaryKey(constants.FivetranStart)
		orderByBuilder.WriteString(fmt.Sprintf("`%s`,", constants.FivetranSynced))
	}
	for i, col := range csvColumns.PrimaryKeys {
		clauseBuilder.WriteString(identifier(col.Name))
		orderByBuilder.WriteString(identifier(col.Name))
		if i < len(csvColumns.PrimaryKeys)-1 {
			clauseBuilder.WriteString(",")
			orderByBuilder.WriteString(",")
		}
	}
	orderByBuilder.WriteRune(')')
	clauseBuilder.WriteString(")IN(")
	for i, csvRow := range csv {
		clauseBuilder.WriteRune('(')
		for j, col := range csvColumns.PrimaryKeys {
			if col.Index > uint(len(csvRow)) {
				return "", fmt.Errorf("can't find matching value for primary key with index %d", col.Index)
			}
			value, err := values.Value(col.Type, csvRow[col.Index])
			if err != nil {
				return "", err
			}
			clauseBuilder.WriteString(value)
			if j < len(csvColumns.PrimaryKeys)-1 {
				clauseBuilder.WriteString(",")
			}
		}
		clauseBuilder.WriteRune(')')
		if i < len(csv)-1 {
			clauseBuilder.WriteString(",")
		}
	}
	clauseBuilder.WriteString(")ORDER BY")
	clauseBuilder.WriteString(orderByBuilder.String())
	clauseBuilder.WriteString(fmt.Sprintf("LIMIT %d", len(csv)))
	return clauseBuilder.String(), nil
}

// GetHardDeleteStatement generates statements such as:
//
//	DELETE FROM `foo`.`bar` WHERE (`id`, `name`) IN ((42, 'foo'), (43, 'bar'))
//
// See also: https://clickhouse.com/docs/en/guides/developer/lightweight-delete
func GetHardDeleteStatement(
	csv [][]string,
	csvColumns *types.CSVColumns,
	qualifiedTableName QualifiedTableName,
) (string, error) {
	if qualifiedTableName == "" {
		return "", fmt.Errorf("table name is empty")
	}
	if len(csv) == 0 {
		return "", fmt.Errorf("expected non-empty CSV slice for table %s", qualifiedTableName)
	}
	if csvColumns == nil || len(csvColumns.PrimaryKeys) == 0 {
		return "", fmt.Errorf("expected non-empty primary keys for table %s", qualifiedTableName)
	}
	var clauseBuilder strings.Builder
	clauseBuilder.WriteString(fmt.Sprintf("DELETE FROM %s WHERE(", qualifiedTableName))
	for i, col := range csvColumns.PrimaryKeys {
		clauseBuilder.WriteString(identifier(col.Name))
		if i < len(csvColumns.PrimaryKeys)-1 {
			clauseBuilder.WriteString(",")
		}
	}
	clauseBuilder.WriteString(")IN(")
	for i, csvRow := range csv {
		clauseBuilder.WriteRune('(')
		for j, col := range csvColumns.PrimaryKeys {
			if col.Index > uint(len(csvRow)) {
				return "", fmt.Errorf("can't find matching value for primary key with index %d", col.Index)
			}
			value, err := values.Value(col.Type, csvRow[col.Index])
			if err != nil {
				return "", err
			}
			clauseBuilder.WriteString(value)
			if j < len(csvColumns.PrimaryKeys)-1 {
				clauseBuilder.WriteString(",")
			}
		}
		clauseBuilder.WriteRune(')')
		if i < len(csv)-1 {
			clauseBuilder.WriteString(",")
		}
	}
	clauseBuilder.WriteRune(')')
	return clauseBuilder.String(), nil
}

// GetHardDeleteWithTimestampStatement generates statements such as:
//
//	DELETE FROM `foo`.`bar` WHERE
//	    (`id` = 1 AND `_fivetran_start` >= '1646455512123456789')
//	    OR (`id` = 2 AND `_fivetran_start` >= '1680784200234567890')
//	    OR (`id` = 3 AND `_fivetran_start` >= '1680784300234567890')
//
// This function combines primary key equality checks with a timestamp comparison for each row,
// matching the behavior of the Java writeDelete method which uses AND conditions between
// primary keys and the timestamp filter.
//
// The timestampColumn parameter specifies which column to use for the timestamp comparison (e.g., "_fivetran_start").
// The timestampIndex parameter specifies the index of the timestamp column in the CSV rows.
//
// See also: https://clickhouse.com/docs/en/guides/developer/lightweight-delete
func GetHardDeleteWithTimestampStatement(
	csv [][]string,
	csvColumns *types.CSVColumns,
	qualifiedTableName QualifiedTableName,
	timestampColumn string,
	timestampIndex uint,
	timestampType pb.DataType,
) (string, error) {
	if qualifiedTableName == "" {
		return "", fmt.Errorf("table name is empty")
	}
	if len(csv) == 0 {
		return "", fmt.Errorf("expected non-empty CSV slice for table %s", qualifiedTableName)
	}
	if csvColumns == nil || len(csvColumns.PrimaryKeys) == 0 {
		return "", fmt.Errorf("expected non-empty primary keys for table %s", qualifiedTableName)
	}
	if timestampColumn == "" {
		return "", fmt.Errorf("timestamp column name is empty")
	}

	var clauseBuilder strings.Builder
	clauseBuilder.WriteString(fmt.Sprintf("DELETE FROM %s WHERE", qualifiedTableName))

	for i, csvRow := range csv {
		if timestampIndex >= uint(len(csvRow)) {
			return "", fmt.Errorf("can't find matching value for timestamp column with index %d", timestampIndex)
		}

		// Start parentheses for each row's condition
		clauseBuilder.WriteRune('(')

		// Build primary key equality conditions with AND between them
		for _, col := range csvColumns.PrimaryKeys {
			if col.Index >= uint(len(csvRow)) {
				return "", fmt.Errorf("can't find matching value for primary key with index %d", col.Index)
			}
			value, err := values.Value(col.Type, csvRow[col.Index])
			if err != nil {
				return "", err
			}
			clauseBuilder.WriteString(fmt.Sprintf("%s=%s", identifier(col.Name), value))

			// Add AND after each primary key condition (including the last one)
			clauseBuilder.WriteString(" AND")
		}

		// Add timestamp condition
		timestampValue, err := values.Value(timestampType, csvRow[timestampIndex])
		if err != nil {
			return "", err
		}
		clauseBuilder.WriteString(fmt.Sprintf("%s>=%s", identifier(timestampColumn), timestampValue))

		// Close parentheses for this row's condition
		clauseBuilder.WriteRune(')')

		// Add OR between row conditions (except after the last one)
		if i < len(csv)-1 {
			clauseBuilder.WriteString("OR")
		}
	}

	statement := clauseBuilder.String()
	return statement, nil
}

// GetUpdateHistoryActiveStatement generates UPDATE statements such as:
//
//	ALTER TABLE `foo`.`bar`
//	UPDATE
//	    `_fivetran_active` = FALSE,
//	    `_fivetran_end` = CASE
//	        WHEN `id` = 1 THEN '1646455512123456788'
//	        WHEN `id` = 2 THEN '1680784200234567889'
//	        WHEN `id` = 3 THEN '1680786000345678900'
//	    END
//	WHERE `id` IN (1, 2, 3)
//	    AND `_fivetran_active` = TRUE
//
// This function updates history records by setting _fivetran_active to FALSE and
// _fivetran_end to the timestamp value from the CSV (typically _fivetran_start - 1).
//
// The endTimestampIndex parameter specifies the index of the column containing the end timestamp value.
// For each row, the CASE statement maps primary key values to their corresponding end timestamps.
//
// See also: https://clickhouse.com/docs/en/sql-reference/statements/alter/update
func GetUpdateHistoryActiveStatement(
	csv [][]string,
	csvColumns *types.CSVColumns,
	qualifiedTableName QualifiedTableName,
	endTimestampIndex uint,
	endTimestampType pb.DataType,
) (string, error) {
	if qualifiedTableName == "" {
		return "", fmt.Errorf("table name is empty")
	}
	if len(csv) == 0 {
		return "", fmt.Errorf("expected non-empty CSV slice for table %s", qualifiedTableName)
	}
	if csvColumns == nil || len(csvColumns.PrimaryKeys) == 0 {
		return "", fmt.Errorf("expected non-empty primary keys for table %s", qualifiedTableName)
	}

	var queryBuilder strings.Builder
	queryBuilder.WriteString(fmt.Sprintf("ALTER TABLE %s UPDATE ", qualifiedTableName))

	// SET clause: _fivetran_active = FALSE
	queryBuilder.WriteString(identifier(constants.FivetranActive))
	queryBuilder.WriteString("=FALSE,")

	// SET clause: _fivetran_end = CASE ... END
	queryBuilder.WriteString(identifier(constants.FivetranEnd))
	queryBuilder.WriteString("=CASE")

	// Build CASE WHEN statements for each row
	for _, csvRow := range csv {
		if endTimestampIndex >= uint(len(csvRow)) {
			return "", fmt.Errorf("can't find matching value for end timestamp column with index %d", endTimestampIndex)
		}

		queryBuilder.WriteString(" WHEN")

		// Build condition for primary keys (excluding _fivetran_start)
		pkConditionCount := 0
		for _, col := range csvColumns.PrimaryKeys {
			if col.Name == constants.FivetranStart {
				continue
			}
			if col.Index >= uint(len(csvRow)) {
				return "", fmt.Errorf("can't find matching value for primary key with index %d", col.Index)
			}
			value, err := values.Value(col.Type, csvRow[col.Index])
			if err != nil {
				return "", err
			}

			if pkConditionCount > 0 {
				queryBuilder.WriteString(" AND")
			}
			queryBuilder.WriteString(fmt.Sprintf("%s=%s", identifier(col.Name), value))
			pkConditionCount++
		}

		// THEN clause with end timestamp value
		endTimestampValue, err := values.Value(endTimestampType, csvRow[endTimestampIndex])
		if err != nil {
			return "", err
		}
		queryBuilder.WriteString(fmt.Sprintf(" THEN %s", endTimestampValue))
	}

	queryBuilder.WriteString("END ")

	// WHERE clause: primary keys IN (...) AND _fivetran_active = TRUE
	queryBuilder.WriteString("WHERE")

	// Filter out _fivetran_start from primary keys
	var filteredPKs []*types.CSVColumn
	for _, col := range csvColumns.PrimaryKeys {
		if col.Name != constants.FivetranStart {
			filteredPKs = append(filteredPKs, col)
		}
	}

	// Handle single vs composite primary keys
	if len(filteredPKs) == 1 {
		// Single PK: id IN (1, 2, 3)
		queryBuilder.WriteString(identifier(filteredPKs[0].Name))
		queryBuilder.WriteString("IN(")

		for i, csvRow := range csv {
			col := filteredPKs[0]
			if col.Index >= uint(len(csvRow)) {
				return "", fmt.Errorf("can't find matching value for primary key with index %d", col.Index)
			}
			value, err := values.Value(col.Type, csvRow[col.Index])
			if err != nil {
				return "", err
			}
			queryBuilder.WriteString(value)
			if i < len(csv)-1 {
				queryBuilder.WriteString(",")
			}
		}
		queryBuilder.WriteString(")")
	} else {
		// Composite PK: (id, name) IN ((1, 'foo'), (2, 'bar'))
		queryBuilder.WriteRune('(')
		for i, col := range filteredPKs {
			queryBuilder.WriteString(identifier(col.Name))
			if i < len(filteredPKs)-1 {
				queryBuilder.WriteString(",")
			}
		}
		queryBuilder.WriteString(")IN(")

		for i, csvRow := range csv {
			queryBuilder.WriteRune('(')
			for j, col := range filteredPKs {
				if col.Index >= uint(len(csvRow)) {
					return "", fmt.Errorf("can't find matching value for primary key with index %d", col.Index)
				}
				value, err := values.Value(col.Type, csvRow[col.Index])
				if err != nil {
					return "", err
				}
				queryBuilder.WriteString(value)
				if j < len(filteredPKs)-1 {
					queryBuilder.WriteString(",")
				}
			}
			queryBuilder.WriteRune(')')
			if i < len(csv)-1 {
				queryBuilder.WriteString(",")
			}
		}
		queryBuilder.WriteString(")")
	}

	queryBuilder.WriteString("AND")
	queryBuilder.WriteString(identifier(constants.FivetranActive))
	queryBuilder.WriteString("=TRUE")

	statement := queryBuilder.String()
	return statement, nil
}

// GetAllReplicasActiveQuery generates a query to check if there are no inactive replicas.
// Excludes Hydra Read Only instances which have disable_insertion_and_mutation = '1'
func GetAllReplicasActiveQuery(
	schemaName string,
	tableName string,
) (string, error) {
	if tableName == "" {
		return "", fmt.Errorf("table name is empty")
	}
	if schemaName == "" {
		return "", fmt.Errorf("schema name for table %s is empty", tableName)
	}
	return fmt.Sprintf(
		`SELECT toBool(mapExists((k, v) -> (v = 0 AND k IN (
           SELECT replica_host FROM (
               SELECT hostName() as replica_host, value 
               FROM clusterAllReplicas(default, system, server_settings) 
               WHERE name = 'disable_insertion_and_mutation' AND value = '0'
               UNION ALL
               SELECT hostName() as replica_host, value 
               FROM clusterAllReplicas(all_groups.default, system, server_settings) 
               WHERE name = 'disable_insertion_and_mutation' AND value = '0'
           )
       )), replica_is_active) = 0) AS all_replicas_active 
       FROM system.replicas 
       WHERE database = '%s' AND table = '%s' 
       LIMIT 1`,
		schemaName, tableName,
	), nil
}

// GetAllMutationsCompletedQuery generates a query to check that all mutations over a particular table on all cluster replicas are completed.
func GetAllMutationsCompletedQuery(
	schemaName string,
	tableName string,
) (string, error) {
	if tableName == "" {
		return "", fmt.Errorf("table name is empty")
	}
	if schemaName == "" {
		return "", fmt.Errorf("schema name for table %s is empty", tableName)
	}
	return fmt.Sprintf(
		"SELECT toBool(count(*) = 0) FROM clusterAllReplicas(default, system.mutations) WHERE database = '%s' AND table = '%s' AND is_done = 0",
		schemaName, tableName,
	), nil
}

func GetInsertFromSelectStatement(
	schemaName string,
	tableName string,
	newTableName string,
	colNames []string,
) (string, error) {
	if tableName == "" {
		return "", fmt.Errorf("current table name is empty")
	}
	if newTableName == "" {
		return "", fmt.Errorf("new table name is empty")
	}
	if schemaName == "" {
		return "", fmt.Errorf("schema name for tables %s/%s is empty", tableName, newTableName)
	}
	if len(colNames) == 0 {
		return "", fmt.Errorf("column names list is empty")
	}
	b := strings.Builder{}
	for i, colName := range colNames {
		b.WriteString(identifier(colName))
		if i < len(colNames)-1 {
			b.WriteString(",")
		}
	}
	joinedColNames := b.String()
	tableIdentifier := fmt.Sprintf("%s.%s", identifier(schemaName), identifier(tableName))
	newTableIdentifier := fmt.Sprintf("%s.%s", identifier(schemaName), identifier(newTableName))
	return fmt.Sprintf(
		"INSERT INTO %s (%s) SELECT %s FROM %s",
		newTableIdentifier, joinedColNames, joinedColNames, tableIdentifier), nil
}

func GetRenameTableStatement(
	schemaName string,
	fromTableName string,
	toTableName string,
) (string, error) {
	if fromTableName == "" {
		return "", fmt.Errorf("from table name is empty")
	}
	if toTableName == "" {
		return "", fmt.Errorf("to table name is empty")
	}
	if schemaName == "" {
		return "", fmt.Errorf("schema name for tables %s/%s is empty", fromTableName, toTableName)
	}
	fromTableIdentifier := fmt.Sprintf("%s.%s", identifier(schemaName), identifier(fromTableName))
	toTableIdentifier := fmt.Sprintf("%s.%s", identifier(schemaName), identifier(toTableName))
	return fmt.Sprintf("RENAME TABLE %s TO %s",
		fromTableIdentifier, toTableIdentifier), nil
}

// GetRenameColumnStatement generates: ALTER TABLE `schema`.`table` RENAME COLUMN `from` TO `to`
// This is an instant metadata-only operation in ClickHouse.
func GetRenameColumnStatement(schemaName string, tableName string, fromColumn string, toColumn string) (string, error) {
	fullTableName, err := GetQualifiedTableName(schemaName, tableName)
	if err != nil {
		return "", err
	}
	if fromColumn == "" {
		return "", fmt.Errorf("from column name is empty")
	}
	if toColumn == "" {
		return "", fmt.Errorf("to column name is empty")
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
	if column == "" {
		return "", fmt.Errorf("column name is empty")
	}
	var sqlValue string
	if isNull {
		sqlValue = "NULL"
	} else {
		sqlValue = fmt.Sprintf("'%s'", escapeSQLString(value))
	}
	return fmt.Sprintf("ALTER TABLE %s UPDATE %s = %s WHERE true", fullTableName, identifier(column), sqlValue), nil
}

// GetCopyColumnUpdateStatement generates: ALTER TABLE `schema`.`table` UPDATE `toColumn` = `fromColumn` WHERE true
// Used for copying data from one column to another within the same table.
func GetCopyColumnUpdateStatement(schemaName string, tableName string, toColumn string, fromColumn string) (string, error) {
	fullTableName, err := GetQualifiedTableName(schemaName, tableName)
	if err != nil {
		return "", err
	}
	if fromColumn == "" {
		return "", fmt.Errorf("from column name is empty")
	}
	if toColumn == "" {
		return "", fmt.Errorf("to column name is empty")
	}
	return fmt.Sprintf("ALTER TABLE %s UPDATE %s = %s WHERE true", fullTableName, identifier(toColumn), identifier(fromColumn)), nil
}

// GetCreateTableAsStatement generates: CREATE TABLE `schema`.`toTable` AS `schema`.`fromTable`
// Clones the table structure and engine settings.
func GetCreateTableAsStatement(schemaName string, fromTable string, toTable string) (string, error) {
	if schemaName == "" {
		return "", fmt.Errorf("schema name is empty")
	}
	if fromTable == "" {
		return "", fmt.Errorf("from table name is empty")
	}
	if toTable == "" {
		return "", fmt.Errorf("to table name is empty")
	}
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
	if operationTimestampNanos == "" {
		return "", fmt.Errorf("operation timestamp is empty")
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
	if operationTimestampNanos == "" {
		return "", fmt.Errorf("operation timestamp is empty")
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

	var insertIntoPart string = fmt.Sprintf("INSERT INTO %s (%s)", fullTableName, insertColumns)

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

	var selectFromPart string = fmt.Sprintf("SELECT %s FROM %s FINAL",
		selectExprs,
		fullTableName,
	)

	// Step 3: Build the WHERE part: WHERE _fivetran_active  AND _fivetran_start < {operation_timestamp}
	// For DROP: only insert new versions for rows where the column actually has a value (AND {column_name} IS NOT NULL)

	var wherePart string = fmt.Sprintf("WHERE `_fivetran_active` AND `_fivetran_start` < '%s'", operationTimestampNanos)

	if overrideValue == nil && overrideColumn != "" {
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
	if schemaName == "" {
		return "", fmt.Errorf("schema name is empty")
	}
	if fromTable == "" {
		return "", fmt.Errorf("from table name is empty")
	}
	if toTable == "" {
		return "", fmt.Errorf("to table name is empty")
	}
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
		"INSERT INTO %s (%s,%s,%s,%s,%s) SELECT %s,%s,%s,'%s',%s FROM %s",
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

// GetInsertFromSelectHistoryToSoftDeleteStatement generates a statement for converting
// a history mode table to soft-delete mode.
func GetInsertFromSelectHistoryToSoftDeleteStatement(
	schemaName string,
	fromTable string,
	toTable string,
	colNames []string,
	softDeletedColumn string,
	keepDeletedRows bool,
) (string, error) {
	if schemaName == "" {
		return "", fmt.Errorf("schema name is empty")
	}
	if fromTable == "" {
		return "", fmt.Errorf("from table name is empty")
	}
	if toTable == "" {
		return "", fmt.Errorf("to table name is empty")
	}
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

	deletedExpr := fmt.Sprintf("if(%s = true, false, true)", identifier(constants.FivetranActive))

	var whereClause string
	if !keepDeletedRows {
		whereClause = fmt.Sprintf(" WHERE %s = true", identifier(constants.FivetranActive))
	}

	return fmt.Sprintf(
		"INSERT INTO %s (%s,%s,%s) SELECT %s,%s,%s FROM %s FINAL%s",
		toIdentifier,
		cols,
		identifier(constants.FivetranSynced),
		identifier(softDeletedColumn),
		cols,
		identifier(constants.FivetranSynced),
		deletedExpr,
		fromIdentifier,
		whereClause,
	), nil
}

// GetLocalMutationsCompletedQuery generates a query to check if all mutations on a table are complete.
// Unlike GetAllMutationsCompletedQuery, this queries system.mutations directly (no clusterAllReplicas)
// and works on both local Docker ClickHouse and ClickHouse Cloud.
func GetLocalMutationsCompletedQuery(schemaName string, tableName string) (string, error) {
	if tableName == "" {
		return "", fmt.Errorf("table name is empty")
	}
	if schemaName == "" {
		return "", fmt.Errorf("schema name for table %s is empty", tableName)
	}
	return fmt.Sprintf(
		"SELECT toBool(count(*) = 0) FROM system.mutations WHERE database = %s AND table = %s AND is_done = 0",
		singleQuoted(schemaName), singleQuoted(tableName),
	), nil
}

// escapeSQLString escapes single quotes in a string for use in SQL literals.
func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// singleQuoted wraps a string in single quotes with escaping for SQL string literals.
func singleQuoted(s string) string {
	return fmt.Sprintf("'%s'", escapeSQLString(s))
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

func identifier(s string) string {
	return fmt.Sprintf("`%s`", s)
}

func toUnixTimestamp64Milli(arg string) string {
	return fmt.Sprintf("toUnixTimestamp64Milli(%s)", arg)
}
