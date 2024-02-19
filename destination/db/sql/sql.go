package sql

import (
	"fmt"
	"strings"

	"fivetran.com/fivetran_sdk/destination/common/constants"
	"fivetran.com/fivetran_sdk/destination/common/types"
	"fivetran.com/fivetran_sdk/destination/db/values"
)

func GetQualifiedTableName(schemaName string, tableName string) (string, error) {
	if tableName == "" {
		return "", fmt.Errorf("table name is empty")
	}
	if schemaName == "" {
		return fmt.Sprintf("`%s`", tableName), nil
	} else {
		return fmt.Sprintf("`%s`.`%s`", schemaName, tableName), nil
	}
}

// GetAlterTableStatement
// If clusterMacros values are nil -> On-premise single node or ClickHouse Cloud deployment.
// Sample generated query:
//
//	ALTER TABLE `foo`.`bar`
//	ADD COLUMN c1 String COMMENT 'foobar',
//	DROP COLUMN c2,
//	MODIFY COLUMN c3 Int32 COMMENT ''
//
// If clusterMacros values are not nil -> On-premise cluster deployment.
// Sample generated query, given types.ClusterMacros{Cluster: "my_cluster"}:
//
//	ALTER TABLE `foo`.`bar` ON CLUSTER 'my_cluster'
//	ADD COLUMN c1 Int32
//
// Comments are added to distinguish certain Fivetran data types, see data_types.FivetranToClickHouseTypeWithComment.
// `ON CLUSTER` part is added only if macros values are not nil.
func GetAlterTableStatement(
	schemaName string,
	tableName string,
	ops []*types.AlterTableOp,
	clusterMacros *types.ClusterMacros,
) (string, error) {
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
			statementsBuilder.WriteString(fmt.Sprintf("ADD COLUMN %s %s", op.Column, *op.Type))
			if op.Comment != nil {
				statementsBuilder.WriteString(fmt.Sprintf(" COMMENT '%s'", *op.Comment))
			}
		case types.AlterTableModify:
			if op.Type == nil {
				return "", fmt.Errorf("type for column %s is not specified", op.Column)
			}
			statementsBuilder.WriteString(fmt.Sprintf("MODIFY COLUMN %s %s", op.Column, *op.Type))
			if op.Comment != nil {
				statementsBuilder.WriteString(fmt.Sprintf(" COMMENT '%s'", *op.Comment))
			}
		case types.AlterTableDrop:
			statementsBuilder.WriteString(fmt.Sprintf("DROP COLUMN %s", op.Column))
		}
		if count < len(ops)-1 {
			statementsBuilder.WriteString(", ")
		}
		count++
	}

	statements := statementsBuilder.String()
	var query string
	if clusterMacros != nil {
		query = fmt.Sprintf("ALTER TABLE %s ON CLUSTER '%s' %s", fullTableName, clusterMacros.Cluster, statements)
	} else {
		query = fmt.Sprintf("ALTER TABLE %s %s", fullTableName, statements)
	}
	return query, nil
}

// GetCreateTableStatement
// If clusterMacros values are nil -> On-premise single node or ClickHouse Cloud deployment.
// Sample generated query:
//
//	CREATE TABLE `foo`.`bar`
//	(id Int64, c2 Nullable(String), _fivetran_synced DateTime64(9, 'UTC'), _fivetran_deleted Bool)
//	ENGINE = ReplacingMergeTree(_fivetran_synced)
//	ORDER BY (id)
//
// If clusterMacros values are not nil -> On-premise cluster deployment.
// Sample generated query, given types.ClusterMacros{Cluster: "my_cluster", Replica: "clickhouse1", Shard: "1"}:
//
//	CREATE TABLE `foo`.`bar` ON CLUSTER 'my_cluster'
//	(id Int64, c2 Nullable(String), _fivetran_synced DateTime64(9, 'UTC'), _fivetran_deleted Bool)
//	ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/foo/bar', 'clickhouse1', '1', 'my_cluster', '1', 'UTC')
func GetCreateTableStatement(
	schemaName string,
	tableName string,
	tableDescription *types.TableDescription,
	clusterMacros *types.ClusterMacros,
) (string, error) {
	fullName, err := GetQualifiedTableName(schemaName, tableName)
	if err != nil {
		return "", err
	}
	if tableDescription == nil || len(tableDescription.Columns) == 0 {
		return "", fmt.Errorf("no columns to create table %s", fullName)
	}
	var orderByCols []string
	var columnsBuilder strings.Builder
	count := 0
	for _, col := range tableDescription.Columns {
		columnsBuilder.WriteString(fmt.Sprintf("%s %s", col.Name, col.Type))
		if col.Comment != "" {
			columnsBuilder.WriteString(fmt.Sprintf(" COMMENT '%s'", col.Comment))
		}
		if col.IsPrimaryKey {
			orderByCols = append(orderByCols, col.Name)
		}
		if count < len(tableDescription.Columns)-1 {
			columnsBuilder.WriteString(", ")
		}
		count++
	}
	columns := columnsBuilder.String()

	var query string
	if clusterMacros != nil {
		query = fmt.Sprintf("CREATE TABLE %s ON CLUSTER '%s' (%s) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/%s/table_name', '%s', %s) ORDER BY (%s)",
			fullName, columns, clusterMacros.Cluster, clusterMacros.Shard, clusterMacros.Replica, constants.FivetranSynced, strings.Join(orderByCols, ", "))
	} else {
		query = fmt.Sprintf("CREATE TABLE %s (%s) ENGINE = ReplacingMergeTree(%s) ORDER BY (%s)",
			fullName, columns, constants.FivetranSynced, strings.Join(orderByCols, ", "))
	}
	return query, nil
}

func GetTruncateTableStatement(schemaName string, tableName string) (string, error) {
	fullName, err := GetQualifiedTableName(schemaName, tableName)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("TRUNCATE TABLE %s", fullName), nil
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
		return "", fmt.Errorf("schema name is empty")
	}
	if tableName == "" {
		return "", fmt.Errorf("table name is empty")
	}
	return fmt.Sprintf("SELECT name, type, comment, is_in_primary_key, numeric_precision, numeric_scale FROM system.columns WHERE database = '%s' AND table = '%s'",
		schemaName, tableName), nil
}

// GetSelectByPrimaryKeysQuery
// CSV slice + known primary key columns and their CSV cell indices -> SELECT FINAL query using values from CSV rows.
// Sample generated query:
//
//	SELECT * FROM `foo`.`bar` FINAL WHERE (id, name) IN ((42, 'foo'), (144, 'bar')) ORDER BY (id, name) LIMIT N
//
// Where N is the number of rows in the CSV slice.
func GetSelectByPrimaryKeysQuery(
	csv [][]string,
	fullTableName string,
	pkCols []*types.PrimaryKeyColumn,
) (string, error) {
	if len(pkCols) == 0 {
		return "", fmt.Errorf("expected non-empty list of primary keys columns")
	}
	if fullTableName == "" {
		return "", fmt.Errorf("table name is empty")
	}
	if len(csv) == 0 {
		return "", fmt.Errorf("expected non-empty CSV slice")
	}
	var orderByBuilder strings.Builder
	orderByBuilder.WriteRune('(')
	var clauseBuilder strings.Builder
	clauseBuilder.WriteString(fmt.Sprintf("SELECT * FROM %s FINAL WHERE (", fullTableName))
	for i, col := range pkCols {
		clauseBuilder.WriteString(col.Name)
		orderByBuilder.WriteString(col.Name)
		if i < len(pkCols)-1 {
			clauseBuilder.WriteString(", ")
			orderByBuilder.WriteString(", ")
		}
	}
	orderByBuilder.WriteRune(')')
	clauseBuilder.WriteString(") IN (")
	for i, row := range csv {
		clauseBuilder.WriteRune('(')
		for j, col := range pkCols {
			if col.Index > uint(len(row)) {
				return "", fmt.Errorf("can't find matching value for primary key with index %d", col.Index)
			}
			clauseBuilder.WriteString(values.Quote(col.Type, row[col.Index]))
			if j < len(pkCols)-1 {
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
