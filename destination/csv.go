package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/shopspring/decimal"
)

type (
	PrimaryKeyColumn struct {
		Index int
		Name  string
		Type  pb.DataType
	}
	CSVRow []string
	CSV    [][]string
)

func CSVRowToInsertValues(row CSVRow, table *pb.Table, nullStr string) ([]any, error) {
	if nullStr == "" {
		return nil, fmt.Errorf("nullStr can't be empty")
	}
	if table == nil {
		return nil, fmt.Errorf("table can't be nil")
	}
	if len(row) != len(table.Columns) {
		return nil, fmt.Errorf("expected %d columns, but row contains %d", len(table.Columns), len(row))
	}
	result := make([]any, len(row))
	for i, col := range table.Columns {
		if row[i] == nullStr {
			if col.Type == pb.DataType_JSON {
				result[i] = "{}" // JSON can't be nullable, so we use an empty object instead
			} else {
				result[i] = nil
			}
			continue
		}
		value, err := ParseValue(col.Name, col.Type, row[i])
		if err != nil {
			return nil, err
		}
		result[i] = value
	}
	return result, nil
}

// CSVRowsToSelectQuery
// CSV slice + known primary key columns and their CSV cell indices -> SELECT query using values from CSV rows
// Sample generated query:
// SELECT * FROM `foo`.`bar` FINAL WHERE (id, name) IN ((44, 'qaz'), (43, 'qux')) ORDER BY (id, name) LIMIT batch_size
func CSVRowsToSelectQuery(batch CSV, fullTableName string, pkCols []*PrimaryKeyColumn) (string, error) {
	if len(pkCols) == 0 {
		return "", fmt.Errorf("expected non-empty list of primary keys columns")
	}
	if fullTableName == "" {
		return "", fmt.Errorf("table name is empty")
	}
	if len(batch) == 0 {
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
	for i, row := range batch {
		clauseBuilder.WriteRune('(')
		for j, col := range pkCols {
			if col.Index > len(row) {
				return "", fmt.Errorf("can't find matching value for primary key with index %d", col.Index)
			}
			clauseBuilder.WriteString(QuoteValue(col.Type, row[col.Index]))
			if j < len(pkCols)-1 {
				clauseBuilder.WriteString(", ")
			}
		}
		clauseBuilder.WriteRune(')')
		if i < len(batch)-1 {
			clauseBuilder.WriteString(", ")
		}
	}
	clauseBuilder.WriteString(") ORDER BY ")
	clauseBuilder.WriteString(orderByBuilder.String())
	clauseBuilder.WriteString(fmt.Sprintf(" LIMIT %d", len(batch)))
	return clauseBuilder.String(), nil
}

func CSVRowToUpdatedDBRow(csvRow CSVRow, dbRow []any, table *pb.Table, nullStr string, unmodifiedStr string) ([]any, error) {
	if unmodifiedStr == "" {
		return nil, fmt.Errorf("unmodifiedStr can't be empty")
	}
	if nullStr == "" {
		return nil, fmt.Errorf("nullStr can't be empty")
	}
	if table == nil {
		return nil, fmt.Errorf("table can't be nil")
	}
	if len(csvRow) != len(dbRow) || len(dbRow) != len(table.Columns) {
		return nil, fmt.Errorf("expected CSV, table definition and ClickHouse row to contain the same number of columns, but got %d, %d and %d", len(csvRow), len(table.Columns), len(dbRow))
	}
	updatedRow := make([]any, len(dbRow))
	for i, value := range csvRow {
		if value == nullStr {
			updatedRow[i] = nil
			continue
		}
		if value == unmodifiedStr {
			updatedRow[i] = dbRow[i]
			continue
		}
		value, err := ParseValue(table.Columns[i].Name, table.Columns[i].Type, value)
		if err != nil {
			return nil, err
		}
		updatedRow[i] = value
	}
	return updatedRow, nil
}

func CSVRowToSoftDeletedRow(csvRow CSVRow, dbRow []any, fivetranSyncedIdx int, fivetranDeletedIdx int) ([]any, error) {
	if fivetranDeletedIdx == -1 || fivetranDeletedIdx >= len(csvRow) {
		return nil, fmt.Errorf("can't find column %s with index %d in a CSV row", FivetranDeleted, fivetranDeletedIdx)
	}
	if fivetranSyncedIdx == -1 || fivetranSyncedIdx >= len(csvRow) {
		return nil, fmt.Errorf("can't find column %s with index %d in a CSV row", FivetranSynced, fivetranSyncedIdx)
	}
	if len(dbRow) < 2 {
		return nil, fmt.Errorf("expected ClickHouse row to contain at least 2 columns, but got %d", len(dbRow))
	}
	updatedRow := make([]any, len(dbRow))
	copy(updatedRow, dbRow)
	// we only need to update _fivetran_deleted and _fivetran_synced values, keeping the rest
	updatedRow[fivetranDeletedIdx] = true
	fivetranSynced, err := time.Parse("2006-01-02T15:04:05.000000000Z", csvRow[fivetranSyncedIdx])
	if err != nil {
		return nil, fmt.Errorf("can't parse value %s as UTC datetime for column %s: %w", csvRow[fivetranSyncedIdx], FivetranSynced, err)
	}
	updatedRow[fivetranSyncedIdx] = fivetranSynced
	return updatedRow, nil
}

func QuoteValue(colType pb.DataType, value string) string {
	switch colType {
	case // quote types that we can pass as a string
		pb.DataType_NAIVE_DATE,
		pb.DataType_NAIVE_DATETIME,
		pb.DataType_UTC_DATETIME,
		pb.DataType_STRING,
		pb.DataType_BINARY,
		pb.DataType_XML,
		pb.DataType_JSON:
		return fmt.Sprintf("'%s'", value)
	default:
		return value
	}
}

func ParseValue(colName string, colType pb.DataType, val string) (any, error) {
	switch colType {
	case pb.DataType_BOOLEAN:
		result, err := strconv.ParseBool(val)
		if err != nil {
			return nil, fmt.Errorf("can't parse value %s as boolean for column %s: %w", val, colName, err)
		}
		return result, nil
	case pb.DataType_SHORT:
		result, err := strconv.ParseInt(val, 10, 16)
		if err != nil {
			return nil, fmt.Errorf("can't parse value %s as int16 for column %s: %w", val, colName, err)
		}
		return int16(result), nil
	case pb.DataType_INT:
		result, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("can't parse value %s as int32 for column %s: %w", val, colName, err)
		}
		return int32(result), nil
	case pb.DataType_LONG:
		result, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("can't parse value %s as int64 for column %s: %w", val, colName, err)
		}
		return result, nil
	case pb.DataType_FLOAT:
		result, err := strconv.ParseFloat(val, 32)
		if err != nil {
			return nil, fmt.Errorf("can't parse value %s as float32 for column %s: %w", val, colName, err)
		}
		return result, nil
	case pb.DataType_DOUBLE:
		result, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil, fmt.Errorf("can't parse value %s as float64 for column %s: %w", val, colName, err)
		}
		return result, nil
	case pb.DataType_DECIMAL:
		result, err := decimal.NewFromString(val)
		if err != nil {
			return nil, fmt.Errorf("can't parse value %s as decimal for column %s: %w", val, colName, err)
		}
		return result, nil
	case pb.DataType_NAIVE_DATE:
		result, err := time.Parse("2006-01-02", val)
		if err != nil {
			return nil, fmt.Errorf("can't parse value %s as naive date for column %s: %w", val, colName, err)
		}
		return result, nil
	case pb.DataType_NAIVE_DATETIME:
		result, err := time.Parse("2006-01-02T15:04:05", val)
		if err != nil {
			return nil, fmt.Errorf("can't parse value %s as naive datetime for column %s: %w", val, colName, err)
		}
		return result, nil
	case pb.DataType_UTC_DATETIME:
		result, err := time.Parse("2006-01-02T15:04:05.000000000Z", val)
		if err != nil {
			return nil, fmt.Errorf("can't parse value %s as UTC datetime for column %s: %w", val, colName, err)
		}
		return result, nil
	case // "string" types work as-is
		pb.DataType_BINARY,
		pb.DataType_XML,
		pb.DataType_STRING,
		pb.DataType_JSON:
		return val, nil
	default:
		return nil, fmt.Errorf("no target type for column %s with type %s", colName, colType.String())
	}
}
