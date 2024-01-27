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

func CSVRowToSelectQuery(row CSVRow, fullTableName string, columns []*PrimaryKeyColumn) (string, error) {
	if len(columns) == 0 {
		return "", fmt.Errorf("expected non-empty list of primary keys columns")
	}
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("SELECT * FROM %s WHERE ", fullTableName))
	for i, col := range columns {
		if col.Index > len(row) {
			return "", fmt.Errorf("can't find matching value for primary key with index %d", col.Index)
		}
		builder.WriteString(fmt.Sprintf("%s = %s", col.Name, QuoteValue(col.Type, row[col.Index])))
		if i < len(columns)-1 {
			builder.WriteString(" AND ")
		}
	}
	builder.WriteString(" LIMIT 1")
	return builder.String(), nil
}

func CSVRowToDeleteStatement(row CSVRow, fullTableName string, columns []*PrimaryKeyColumn) (string, error) {
	if len(columns) == 0 {
		return "", fmt.Errorf("expected non-empty list of primary keys columns")
	}
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("DELETE FROM %s WHERE ", fullTableName))
	for i, col := range columns {
		if col.Index > len(row) {
			return "", fmt.Errorf("can't find matching value for primary key with index %d", col.Index)
		}
		builder.WriteString(fmt.Sprintf("%s = %s", col.Name, QuoteValue(col.Type, row[col.Index])))
		if i < len(columns)-1 {
			builder.WriteString(" AND ")
		}
	}
	return builder.String(), nil
}

func CSVRowToUpdatedDBRow(csvRow CSVRow, dbRow []any, table *pb.Table, nullStr string, unmodifiedStr string) ([]any, error) {
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
	if fivetranSyncedIdx == -1 || fivetranSyncedIdx >= len(csvRow) {
		return nil, fmt.Errorf("can't find %s column in CSV row", FivetranSynced)
	}
	if fivetranDeletedIdx == -1 || fivetranDeletedIdx >= len(csvRow) {
		return nil, fmt.Errorf("can't find %s column in CSV row", FivetranDeleted)
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
		return nil, err
	}
	updatedRow[fivetranSyncedIdx] = fivetranSynced
	return updatedRow, nil
}

func QuoteValue(colType pb.DataType, value any) any {
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
			return nil, err
		}
		return result, nil
	case pb.DataType_SHORT:
		result, err := strconv.ParseInt(val, 10, 16)
		if err != nil {
			return nil, err
		}
		return int16(result), nil
	case pb.DataType_INT:
		result, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			return nil, err
		}
		return int32(result), nil
	case pb.DataType_LONG:
		result, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil, err
		}
		return result, nil
	case pb.DataType_FLOAT:
		result, err := strconv.ParseFloat(val, 32)
		if err != nil {
			return nil, err
		}
		return result, nil
	case pb.DataType_DOUBLE:
		result, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil, err
		}
		return result, nil
	case pb.DataType_DECIMAL:
		result, err := decimal.NewFromString(val)
		if err != nil {
			return nil, err
		}
		return result, nil
	case pb.DataType_NAIVE_DATE:
		result, err := time.Parse("2006-01-02", val)
		if err != nil {
			return nil, err
		}
		return result, nil
	case pb.DataType_NAIVE_DATETIME:
		result, err := time.Parse("2006-01-02T15:04:05", val)
		if err != nil {
			return nil, err
		}
		return result, nil
	case pb.DataType_UTC_DATETIME:
		result, err := time.Parse("2006-01-02T15:04:05.000000000Z", val)
		if err != nil {
			return nil, err
		}
		return result, nil
	case // "string" types work as-is
		pb.DataType_BINARY,
		pb.DataType_XML,
		pb.DataType_STRING,
		pb.DataType_JSON:
		return val, nil
	case pb.DataType_UNSPECIFIED:
		return nil, fmt.Errorf("column %s has unspecified type", colName)
	default:
		return nil, fmt.Errorf("no target type for column %s with type %s", colName, colType.String())
	}
}
