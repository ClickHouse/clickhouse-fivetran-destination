package sql

import (
	"fmt"
	"strings"
	"time"

	"fivetran.com/fivetran_sdk/destination/common/constants"
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
			statementsBuilder.WriteString(", ")
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
			columnsBuilder.WriteString(", ")
		}
		count++
	}
	columns := columnsBuilder.String()

	query := fmt.Sprintf(
		"CREATE TABLE %s (%s) ENGINE = ReplacingMergeTree(%s) ORDER BY (%s)",
		fullName, columns, identifier(constants.FivetranSynced), strings.Join(orderByCols, ", "))
	return query, nil
}

// GetTruncateTableStatement
// generates a query for either "soft" (ALTER TABLE UPDATE) or "hard" (ALTER TABLE DELETE) table truncation.
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
	if truncateBefore.Second() == 0 && truncateBefore.Nanosecond() == 0 {
		return "", fmt.Errorf("truncate before time is zero")
	}
	var query string
	truncateBeforeMilli := truncateBefore.UnixMilli()
	syncedColumnMilli := toUnixTimestamp64Milli(identifier(syncedColumn))
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

// GetSelectByPrimaryKeysQuery
// CSV slice + known primary key columns and their CSV cell indices -> SELECT FINAL query using values from CSV rows.
// Sample generated query:
//
//	SELECT * FROM `foo`.`bar` FINAL WHERE (`id`, `name`) IN ((42, 'foo'), (144, 'bar')) ORDER BY (`id`, `name`) LIMIT N
//
// Where N is the number of rows in the CSV slice.
func GetSelectByPrimaryKeysQuery(
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
	var orderByBuilder strings.Builder
	orderByBuilder.WriteRune('(')
	var clauseBuilder strings.Builder
	clauseBuilder.WriteString(fmt.Sprintf("SELECT * FROM %s FINAL WHERE (", qualifiedTableName))
	for i, col := range csvColumns.PrimaryKeys {
		clauseBuilder.WriteString(identifier(col.Name))
		orderByBuilder.WriteString(identifier(col.Name))
		if i < len(csvColumns.PrimaryKeys)-1 {
			clauseBuilder.WriteString(", ")
			orderByBuilder.WriteString(", ")
		}
	}
	orderByBuilder.WriteRune(')')
	clauseBuilder.WriteString(") IN (")
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
				clauseBuilder.WriteString(", ")
			}
		}
		clauseBuilder.WriteRune(')')
		if i < len(csv)-1 {
			clauseBuilder.WriteString(", ")
		}
	}
	clauseBuilder.WriteString(") ORDER BY ")
	clauseBuilder.WriteString(orderByBuilder.String())
	clauseBuilder.WriteString(fmt.Sprintf(" LIMIT %d", len(csv)))
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
	clauseBuilder.WriteString(fmt.Sprintf("DELETE FROM %s WHERE (", qualifiedTableName))
	for i, col := range csvColumns.PrimaryKeys {
		clauseBuilder.WriteString(identifier(col.Name))
		if i < len(csvColumns.PrimaryKeys)-1 {
			clauseBuilder.WriteString(", ")
		}
	}
	clauseBuilder.WriteString(") IN (")
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
				clauseBuilder.WriteString(", ")
			}
		}
		clauseBuilder.WriteRune(')')
		if i < len(csv)-1 {
			clauseBuilder.WriteString(", ")
		}
	}
	clauseBuilder.WriteRune(')')
	return clauseBuilder.String(), nil
}

// GetAllReplicasActiveQuery
// generates a query to check if there are no inactive replicas.
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
		"SELECT toBool(mapExists((k, v) -> (v = 0), replica_is_active) = 0) AS all_replicas_active FROM system.replicas WHERE database = '%s' AND table = '%s' AND is_readonly != 1 LIMIT 1",
		schemaName, tableName,
	), nil
}

// GetAllMutationsCompletedQuery
// generates a query to check that all mutations over a particular table on all cluster replicas are completed.
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
	if colNames == nil || len(colNames) == 0 {
		return "", fmt.Errorf("column names list is empty")
	}
	b := strings.Builder{}
	for i, colName := range colNames {
		b.WriteString(identifier(colName))
		if i < len(colNames)-1 {
			b.WriteString(", ")
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

func identifier(s string) string {
	return fmt.Sprintf("`%s`", s)
}

func toUnixTimestamp64Milli(arg string) string {
	return fmt.Sprintf("toUnixTimestamp64Milli(%s)", arg)
}
