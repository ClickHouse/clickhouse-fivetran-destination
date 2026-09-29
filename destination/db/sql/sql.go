package sql

import (
	"fmt"
	"slices"
	"strings"
	"time"

	constants "fivetran.com/fivetran_sdk/destination/common/constants"
	"fivetran.com/fivetran_sdk/destination/common/types"
	"fivetran.com/fivetran_sdk/destination/db/values"
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
//	ADD COLUMN IF NOT EXISTS `c1` String COMMENT 'foobar',
//	DROP COLUMN IF EXISTS `c2`,
//	MODIFY COLUMN IF EXISTS `c3` Int32 COMMENT ''
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
			statementsBuilder.WriteString(fmt.Sprintf("ADD COLUMN IF NOT EXISTS %s %s", identifier(op.Column), *op.Type))
			if op.Comment != nil {
				statementsBuilder.WriteString(fmt.Sprintf(" COMMENT '%s'", *op.Comment))
			}
		case types.AlterTableModify:
			if op.Type == nil {
				return "", fmt.Errorf("type for column %s is not specified", op.Column)
			}
			statementsBuilder.WriteString(fmt.Sprintf("MODIFY COLUMN IF EXISTS %s %s", identifier(op.Column), *op.Type))
			if op.Comment != nil {
				statementsBuilder.WriteString(fmt.Sprintf(" COMMENT '%s'", *op.Comment))
			}
		case types.AlterTableDrop:
			statementsBuilder.WriteString(fmt.Sprintf("DROP COLUMN IF EXISTS %s", identifier(op.Column)))
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
	return fmt.Sprintf("EXISTS DATABASE %s", identifier(schemaName)), nil
}

func GetCheckTableExistsStatement(schemaName string, tableName string) (string, error) {
	fullName, err := GetQualifiedTableName(schemaName, tableName)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("EXISTS TABLE %s", fullName), nil
}

func GetCreateDatabaseStatement(schemaName string) (string, error) {
	if schemaName == "" {
		return "", fmt.Errorf("schema name is empty")
	}
	return fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", identifier(schemaName)), nil
}

func GetDropTableStatement(tableName QualifiedTableName) (string, error) {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s SYNC", tableName), nil
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
//	CREATE TABLE IF NOT EXISTS `foo`.`bar`
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
		"CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = ReplacingMergeTree(%s) ORDER BY (%s)",
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

// GetCreateStagingTableStatement generates statements such as:
//
//	CREATE TABLE `foo`.`bar_tmp_earliest_start_1700000000000`
//	(`id` Int64, `_fivetran_start` DateTime64(9, 'UTC')) ENGINE = MergeTree ORDER BY (`id`)
//
// columns are typed as in the destination table (driverColumns). The statement fails if the table exists:
// a leftover helper table would mix its rows into this batch, so it must not be reused.
func GetCreateStagingTableStatement(
	qualifiedStagingTableName QualifiedTableName,
	columns []*types.CSVColumn,
	orderBy []string,
	driverColumns *types.DriverColumns,
) string {
	columnDefs := make([]string, 0, len(columns))
	for _, col := range columns {
		columnDefs = append(columnDefs, fmt.Sprintf("%s %s", identifier(col.Name), driverColumns.Mapping[col.Name].DatabaseType))
	}
	return fmt.Sprintf("CREATE TABLE %s (%s) ENGINE = MergeTree ORDER BY (%s)",
		qualifiedStagingTableName, strings.Join(columnDefs, ","), joinIdentifiers("", orderBy))
}

// GetDeleteOverlappingHistoryStatement removes the versions overlapping an earliest-start file staged in
// qualifiedStagingTableName (same primary key, _fivetran_start >= staged start). Sample generated query:
//
//	DELETE FROM `foo`.`bar` WHERE (`id`,`_fivetran_start`) IN (
//	    SELECT tgt.`id`,tgt.`_fivetran_start` FROM `foo`.`bar` AS tgt
//	    INNER JOIN `foo`.`bar_tmp_earliest_start_1700000000000` AS stg ON tgt.`id`=stg.`id`
//	    WHERE tgt.`_fivetran_start`>=stg.`_fivetran_start` SETTINGS select_sequential_consistency = 1)
//	SETTINGS allow_nondeterministic_mutations = 1
//
// primaryKeys are the table primary keys as Fivetran defines them; see historyPrimaryKeys.
//
// Replicated tables reject mutations with subqueries unless allow_nondeterministic_mutations is set (the
// staging table is immutable while the mutation runs, so the result is deterministic). The subquery runs
// in the background on the replica applying the mutation, outside the session that set
// select_sequential_consistency on the connection, hence that setting is repeated inside the subquery.
func GetDeleteOverlappingHistoryStatement(
	qualifiedTableName QualifiedTableName,
	qualifiedStagingTableName QualifiedTableName,
	primaryKeys []string,
) string {
	keys := historyPrimaryKeys(primaryKeys)
	keysWithStart := append(slices.Clone(keys), constants.FivetranStart)
	return fmt.Sprintf("DELETE FROM %s WHERE (%s) IN (SELECT %s FROM %s AS tgt INNER JOIN %s AS stg ON %s WHERE tgt.%s>=stg.%s SETTINGS select_sequential_consistency = 1) SETTINGS allow_nondeterministic_mutations = 1",
		qualifiedTableName,
		joinIdentifiers("", keysWithStart),
		joinIdentifiers("tgt.", keysWithStart),
		qualifiedTableName,
		qualifiedStagingTableName,
		historyStagingJoinCondition(keys),
		identifier(constants.FivetranStart),
		identifier(constants.FivetranStart),
	)
}

// GetCloseActiveHistoryRowsStatement closes the active row of every key in an earliest-start file staged in
// qualifiedStagingTableName by inserting a new version of it. columnNames are all table columns, in table
// order; primaryKeys are the table primary keys as Fivetran defines them, see historyPrimaryKeys.
// Sample generated query:
//
//	INSERT INTO `foo`.`bar` (`id`,`name`,`_fivetran_synced`,`_fivetran_start`,`_fivetran_end`,`_fivetran_active`)
//	SELECT tgt.`id`,tgt.`name`,tgt.`_fivetran_synced` + toIntervalNanosecond(1),
//	    tgt.`_fivetran_start`,stg.`_fivetran_start`,FALSE
//	FROM `foo`.`bar` AS tgt FINAL
//	INNER JOIN `foo`.`bar_tmp_earliest_start_1700000000000` AS stg ON tgt.`id`=stg.`id`
//	WHERE tgt.`_fivetran_active`=TRUE
//
// The new version carries _fivetran_synced + 1 nanosecond:
//   - Fivetran defines this step as an in-place update that leaves _fivetran_synced unchanged, and the
//     earliest-start file has no _fivetran_synced column; the only value available is the active row's own.
//   - ReplacingMergeTree keeps the highest _fivetran_synced, so the closing row must be strictly newer:
//     an equal value would leave the winner to merge order, a default value would make it lose.
//   - now() is not used: _fivetran_synced is the start of Fivetran's sync, truncate compares it against
//     utc_delete_before, and it would depend on the ClickHouse clock.
//   - One nanosecond is invisible at the millisecond precision Fivetran writes and compares at.
//
// Later operations cannot undo the close:
//   - a re-delivered version arrives with a newer Fivetran sync time and wins;
//   - the same batch cannot contain the closed version, its start precedes the earliest start;
//   - a retry finds the row inactive and inserts nothing;
//   - update files pick the last row per key by _fivetran_synced, so the closed row wins over same-sync siblings;
//   - delete files and later earliest-start files only touch active rows;
//   - truncate compares _fivetran_synced at millisecond precision.
func GetCloseActiveHistoryRowsStatement(
	qualifiedTableName QualifiedTableName,
	qualifiedStagingTableName QualifiedTableName,
	columnNames []string,
	primaryKeys []string,
) (string, error) {
	closingValues := map[string]string{
		constants.FivetranEnd:    "stg." + identifier(constants.FivetranStart),
		constants.FivetranActive: "FALSE",
		constants.FivetranSynced: fmt.Sprintf("tgt.%s + toIntervalNanosecond(1)", identifier(constants.FivetranSynced)),
	}
	for _, required := range []string{constants.FivetranEnd, constants.FivetranActive, constants.FivetranSynced} {
		if !slices.Contains(columnNames, required) {
			return "", fmt.Errorf("column %s not found in table %s", required, qualifiedTableName)
		}
	}
	selectCols := make([]string, 0, len(columnNames))
	for _, col := range columnNames {
		if value, ok := closingValues[col]; ok {
			selectCols = append(selectCols, value)
		} else {
			selectCols = append(selectCols, "tgt."+identifier(col))
		}
	}
	return fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s AS tgt FINAL INNER JOIN %s AS stg ON %s WHERE tgt.%s=TRUE",
		qualifiedTableName,
		joinIdentifiers("", columnNames),
		strings.Join(selectCols, ","),
		qualifiedTableName,
		qualifiedStagingTableName,
		historyStagingJoinCondition(historyPrimaryKeys(primaryKeys)),
		identifier(constants.FivetranActive),
	), nil
}

// historyPrimaryKeys returns the primary keys without _fivetran_start. Fivetran marks it as a primary key in
// history mode, but keeping it in a key filter turns the earliest-start range into an exact match; see
// https://github.com/fivetran/fivetran_partner_sdk/blob/main/how-to-handle-history-mode-batch-files.md#earliest_start_files
func historyPrimaryKeys(primaryKeys []string) []string {
	return slices.DeleteFunc(slices.Clone(primaryKeys), func(key string) bool { return key == constants.FivetranStart })
}

func historyStagingJoinCondition(primaryKeys []string) string {
	conditions := make([]string, 0, len(primaryKeys))
	for _, pk := range primaryKeys {
		conditions = append(conditions, fmt.Sprintf("tgt.%s=stg.%s", identifier(pk), identifier(pk)))
	}
	return strings.Join(conditions, " AND ")
}

func joinIdentifiers(prefix string, names []string) string {
	quoted := make([]string, 0, len(names))
	for _, name := range names {
		quoted = append(quoted, prefix+identifier(name))
	}
	return strings.Join(quoted, ",")
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
// The endTimestampColumn parameter names the CSV column containing the end timestamp value
// (e.g., _fivetran_start for earliest-start files, _fivetran_end for delete files); it is looked up in csvColumns.
// For each row, the CASE statement maps primary key values to their corresponding end timestamps.
//
// See also: https://clickhouse.com/docs/en/sql-reference/statements/alter/update
func GetUpdateHistoryActiveStatement(
	csv [][]string,
	csvColumns *types.CSVColumns,
	qualifiedTableName QualifiedTableName,
	endTimestampColumn string,
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
	endTimestampCol, err := csvColumns.FindColumn(endTimestampColumn)
	if err != nil {
		return "", err
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
		queryBuilder.WriteString(" WHEN")

		// Build condition for primary keys (excluding _fivetran_start)
		pkConditionCount := 0
		for _, col := range csvColumns.PrimaryKeys {
			if col.Name == constants.FivetranStart {
				continue
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
		endTimestampValue, err := values.Value(endTimestampCol.Type, csvRow[endTimestampCol.Index])
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

// GetInsertFromSelectStatement generates an INSERT ... SELECT FROM ... FINAL statement
// used to rebuild a table during migrations (PK change in AlterTable, MigrateCopyTable).
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
		"INSERT INTO %s (%s) SELECT %s FROM %s FINAL",
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

func identifier(s string) string {
	return fmt.Sprintf("`%s`", s)
}

func toUnixTimestamp64Milli(arg string) string {
	return fmt.Sprintf("toUnixTimestamp64Milli(%s)", arg)
}
