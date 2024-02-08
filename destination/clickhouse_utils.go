package main

import (
	"fmt"
	"strings"
)

func GetWithDefault(configuration map[string]string, key string, default_ string) string {
	value, ok := configuration[key]
	if !ok || value == "" {
		return default_
	}
	return value
}

func GetFullTableName(schemaName string, tableName string) (string, error) {
	if tableName == "" {
		return "", fmt.Errorf("table name is empty")
	}
	if schemaName == "" {
		return fmt.Sprintf("`%s`", tableName), nil
	} else {
		return fmt.Sprintf("`%s`.`%s`", schemaName, tableName), nil
	}
}

func GetAlterTableStatement(schemaName string, tableName string, ops []*AlterTableOp) (string, error) {
	fullTableName, err := GetFullTableName(schemaName, tableName)
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
		case Add:
			statementsBuilder.WriteString(fmt.Sprintf("ADD COLUMN %s %s", op.Column, *op.Type))
		case Modify:
			statementsBuilder.WriteString(fmt.Sprintf("MODIFY COLUMN %s %s", op.Column, *op.Type))
		case Drop:
			statementsBuilder.WriteString(fmt.Sprintf("DROP COLUMN %s", op.Column))
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

func GetCreateTableStatement(schemaName string, tableName string, tableDescription *TableDescription) (string, error) {
	fullName, err := GetFullTableName(schemaName, tableName)
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
		if col.IsPrimaryKey {
			orderByCols = append(orderByCols, col.Name)
		}
		if count < len(tableDescription.Columns)-1 {
			columnsBuilder.WriteString(", ")
		}
		count++
	}
	columns := columnsBuilder.String()

	query := fmt.Sprintf("CREATE TABLE %s (%s) ENGINE = ReplacingMergeTree(%s) ORDER BY (%s)",
		fullName, columns, FivetranSynced, strings.Join(orderByCols, ", "))
	return query, nil
}

func GetTruncateTableStatement(schemaName string, tableName string) (string, error) {
	fullName, err := GetFullTableName(schemaName, tableName)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("TRUNCATE TABLE %s", fullName), nil
}

func GetColumnTypesQuery(schemaName string, tableName string) (string, error) {
	fullName, err := GetFullTableName(schemaName, tableName)
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
	return fmt.Sprintf("SELECT name, type, is_in_primary_key FROM system.columns WHERE database = '%s' AND table = '%s'",
		schemaName, tableName), nil
}
