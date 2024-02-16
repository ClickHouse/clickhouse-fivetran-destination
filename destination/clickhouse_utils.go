package main

import (
	"fmt"
	"strings"

	pb "fivetran.com/fivetran_sdk/proto"
)

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
			if op.Type == nil {
				return "", fmt.Errorf("type for column %s is not specified", op.Column)
			}
			statementsBuilder.WriteString(fmt.Sprintf("ADD COLUMN %s %s", op.Column, *op.Type))
			if op.Comment != nil {
				statementsBuilder.WriteString(fmt.Sprintf(" COMMENT '%s'", *op.Comment))
			}
		case Modify:
			if op.Type == nil {
				return "", fmt.Errorf("type for column %s is not specified", op.Column)
			}
			statementsBuilder.WriteString(fmt.Sprintf("MODIFY COLUMN %s %s", op.Column, *op.Type))
			if op.Comment != nil {
				statementsBuilder.WriteString(fmt.Sprintf(" COMMENT '%s'", *op.Comment))
			}
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
	return fmt.Sprintf("SELECT name, type, comment, is_in_primary_key, numeric_precision, numeric_scale FROM system.columns WHERE database = '%s' AND table = '%s'",
		schemaName, tableName), nil
}

func MergeUpdatedRows(
	batch CSV,
	selectRows RowsByPrimaryKeyValue,
	pkCols []*PrimaryKeyColumn,
	table *pb.Table,
	nullStr string,
	unmodifiedStr string,
) (insertRows [][]interface{}, skipIdx map[int]bool, err error) {
	insertRows = make([][]interface{}, len(batch))
	skipIdx = make(map[int]bool)
	for j, csvRow := range batch {
		mappingKey, err := GetCSVRowMappingKey(csvRow, pkCols)
		if err != nil {
			return nil, nil, err
		}
		dbRow, exists := selectRows[mappingKey]
		if exists {
			updatedRow, err := CSVRowToUpdatedDBRow(csvRow, dbRow, table, nullStr, unmodifiedStr)
			if err != nil {
				return nil, nil, err
			}
			insertRows[j] = updatedRow
		} else {
			// Shouldn't happen
			LogWarn(fmt.Sprintf("[MergeUpdatedRows] Row with PK mapping %s does not exist", mappingKey))
			skipIdx[j] = true
			continue
		}
	}
	return insertRows, skipIdx, nil
}

func MergeSoftDeletedRows(
	batch CSV,
	selectRows RowsByPrimaryKeyValue,
	pkCols []*PrimaryKeyColumn,
	fivetranSyncedIdx uint,
	fivetranDeletedIdx uint,
) (insertRows [][]interface{}, skipIdx map[int]bool, err error) {
	insertRows = make([][]interface{}, len(batch))
	skipIdx = make(map[int]bool)
	for j, csvRow := range batch {
		mappingKey, err := GetCSVRowMappingKey(csvRow, pkCols)
		if err != nil {
			return nil, nil, err
		}
		dbRow, exists := selectRows[mappingKey]
		if exists {
			softDeletedRow, err := CSVRowToSoftDeletedRow(csvRow, dbRow, fivetranSyncedIdx, fivetranDeletedIdx)
			if err != nil {
				return nil, nil, err
			}
			insertRows[j] = softDeletedRow
		} else {
			// Shouldn't happen
			LogWarn(fmt.Sprintf("[MergeSoftDeletedRows] Row with PK mapping %s does not exist", mappingKey))
			skipIdx[j] = true
			continue
		}
	}
	return insertRows, skipIdx, nil
}
