package db

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"fivetran.com/fivetran_sdk/destination/common/constants"
	"fivetran.com/fivetran_sdk/destination/common/log"
	"fivetran.com/fivetran_sdk/destination/common/types"
	"fivetran.com/fivetran_sdk/destination/db/values"
	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/shopspring/decimal"
)

// RowsByPrimaryKeyValue contains existing ClickHouse rows mapped using GetDatabaseRowMappingKey.
// It is used to ensure that we don't have records with different position in the selected DB rows vs CSV,
// which could lead to corrupted updates as the records will be mixed up.
// TODO: maybe add CSV index to the SELECT query to avoid all this?
type RowsByPrimaryKeyValue map[string][]interface{}

// ColumnTypesToEmptyScanRows creates a slice of N empty rows to scan the selected data into,
// with correct types at every index based on the introspected table column types from ClickHouse.
func ColumnTypesToEmptyScanRows(columnTypes []driver.ColumnType, n uint) [][]interface{} {
	dbRows := make([][]interface{}, n)
	for i := range dbRows {
		dbRow := make([]interface{}, len(columnTypes))
		for j := range dbRow {
			dbRow[j] = reflect.New(columnTypes[j].ScanType()).Interface()
		}
		dbRows[i] = dbRow
	}
	return dbRows
}

// GetDatabaseRowMappingKey creates a string key like "id:42" for a row that we got from ClickHouse,
// where `id` is the primary key column name and `42` is its value.
// If there are multiple primary keys, then the key will be formatted like "id:42,name:foo".
// Important: PK values string format should be exactly the same way as they arrive in CSVs,
// and the overall mapping key should match its GetCSVRowMappingKey counterpart.
func GetDatabaseRowMappingKey(row []interface{}, pkCols []*types.PrimaryKeyColumn) (string, error) {
	if len(pkCols) == 0 {
		return "", fmt.Errorf("expected non-empty list of primary keys columns")
	}
	var key strings.Builder
	for i, col := range pkCols {
		key.WriteString(col.Name)
		key.WriteRune(':')
		p := row[col.Index]
		switch p := p.(type) {
		case *string:
			key.WriteString(fmt.Sprint(*p))
		case *int16:
			key.WriteString(fmt.Sprint(*p))
		case *int32:
			key.WriteString(fmt.Sprint(*p))
		case *int64:
			key.WriteString(fmt.Sprint(*p))
		case *float32:
			key.WriteString(fmt.Sprint(*p))
		case *float64:
			key.WriteString(fmt.Sprint(*p))
		case *bool:
			key.WriteString(fmt.Sprint(*p))
		case *time.Time:
			// format UTC datetime as nanos (due to possibly variable precision),
			// the rest should actually match CSV datetime format
			if col.Type == pb.DataType_UTC_DATETIME {
				key.WriteString(fmt.Sprint(p.UnixNano()))
			} else if col.Type == pb.DataType_NAIVE_DATETIME {
				key.WriteString(p.Format(constants.NaiveDateTimeFormat))
			} else {
				key.WriteString(p.Format(constants.NaiveDateFormat))
			}
		case *decimal.Decimal:
			key.WriteString(p.String())
		default:
			return "", fmt.Errorf("can't use type %T as mapping key", p)
		}
		if i < len(pkCols)-1 {
			key.WriteString(",")
		}
	}
	return key.String(), nil
}

// GetCSVRowMappingKey is similar to GetDatabaseRowMappingKey, but for CSV rows.
func GetCSVRowMappingKey(row []string, pkCols []*types.PrimaryKeyColumn) (string, error) {
	if len(pkCols) == 0 {
		return "", fmt.Errorf("expected non-empty list of primary keys columns")
	}
	var key strings.Builder
	for i, col := range pkCols {
		key.WriteString(col.Name)
		key.WriteRune(':')
		// reformat UTC datetime as nanos (due to possibly variable precision)
		if col.Type == pb.DataType_UTC_DATETIME {
			t, err := time.Parse(constants.UTCDateTimeFormat, row[col.Index])
			if err != nil {
				return "", fmt.Errorf("can't parse value %s as UTC datetime for column %s: %w",
					row[col.Index], col.Name, err)
			}
			key.WriteString(fmt.Sprint(t.UnixNano()))
		} else {
			key.WriteString(row[col.Index])
		}
		if i < len(pkCols)-1 {
			key.WriteRune(',')
		}
	}
	return key.String(), nil
}

// MergeUpdatedRows
// merges a CSV with existing ClickHouse rows to create a batch of rows to insert back to the database.
// `selectRows` are fetched from ClickHouse in advance using primary key values from CSV records.
// See also: ToUpdatedRow.
func MergeUpdatedRows(
	csv [][]string,
	selectRows RowsByPrimaryKeyValue,
	pkCols []*types.PrimaryKeyColumn,
	table *pb.Table,
	nullStr string,
	unmodifiedStr string,
) (insertRows [][]interface{}, skipIdx map[int]bool, err error) {
	insertRows = make([][]interface{}, len(csv))
	skipIdx = make(map[int]bool)
	for j, csvRow := range csv {
		mappingKey, err := GetCSVRowMappingKey(csvRow, pkCols)
		if err != nil {
			return nil, nil, err
		}
		dbRow, exists := selectRows[mappingKey]
		if exists {
			updatedRow, err := ToUpdatedRow(csvRow, dbRow, table, nullStr, unmodifiedStr)
			if err != nil {
				return nil, nil, err
			}
			insertRows[j] = updatedRow
		} else {
			// Shouldn't happen
			log.Warn(fmt.Sprintf("[MergeUpdatedRows] Row with PK mapping %s does not exist", mappingKey))
			skipIdx[j] = true
			continue
		}
	}
	return insertRows, skipIdx, nil
}

// MergeSoftDeletedRows is the same as MergeUpdatedRows, but for rows that are marked as deleted by Fivetran.
// See also: ToSoftDeletedRow.
func MergeSoftDeletedRows(
	csv [][]string,
	selectRows RowsByPrimaryKeyValue,
	pkCols []*types.PrimaryKeyColumn,
	fivetranSyncedIdx uint,
	fivetranDeletedIdx uint,
) (insertRows [][]interface{}, skipIdx map[int]bool, err error) {
	insertRows = make([][]interface{}, len(csv))
	skipIdx = make(map[int]bool)
	for j, csvRow := range csv {
		mappingKey, err := GetCSVRowMappingKey(csvRow, pkCols)
		if err != nil {
			return nil, nil, err
		}
		dbRow, exists := selectRows[mappingKey]
		if exists {
			softDeletedRow, err := ToSoftDeletedRow(csvRow, dbRow, fivetranSyncedIdx, fivetranDeletedIdx)
			if err != nil {
				return nil, nil, err
			}
			insertRows[j] = softDeletedRow
		} else {
			// Shouldn't happen
			log.Warn(fmt.Sprintf("[MergeSoftDeletedRows] Row with PK mapping %s does not exist", mappingKey))
			skipIdx[j] = true
			continue
		}
	}
	return insertRows, skipIdx, nil
}

// ToInsertRow converts a CSV row to a ClickHouse row, parsing strings and converting them to the correct types.
func ToInsertRow(
	csvRow []string,
	table *pb.Table,
	nullStr string,
) ([]any, error) {
	if nullStr == "" {
		return nil, fmt.Errorf("nullStr can't be empty")
	}
	if table == nil {
		return nil, fmt.Errorf("table can't be nil")
	}
	if len(table.Columns) != len(csvRow) {
		return nil, fmt.Errorf("expected %d columns, but CSV row contains %d", len(table.Columns), len(csvRow))
	}
	result := make([]any, len(csvRow))
	for i, col := range table.Columns {
		if csvRow[i] == nullStr {
			result[i] = nil
			continue
		}
		value, err := values.Parse(col.Name, col.Type, csvRow[i])
		if err != nil {
			return nil, err
		}
		result[i] = value
	}
	return result, nil
}

// ToUpdatedRow merges an existing ClickHouse row with the CSV row values.
// Fields that are equal to unmodifiedStr are not updated.
func ToUpdatedRow(
	csvRow []string,
	dbRow []any,
	table *pb.Table,
	nullStr string,
	unmodifiedStr string,
) ([]any, error) {
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
		return nil, fmt.Errorf(
			"expected CSV, table definition and ClickHouse row to contain the same number of columns, "+
				"but got %d, %d and %d", len(csvRow), len(table.Columns), len(dbRow))
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
		parsedValue, err := values.Parse(table.Columns[i].Name, table.Columns[i].Type, value)
		if err != nil {
			return nil, err
		}
		updatedRow[i] = parsedValue
	}
	return updatedRow, nil
}

// ToSoftDeletedRow updates an existing ClickHouse row with _fivetran_deleted and _fivetran_synced values from the CSV.
// The rest of the fields are not updated (all other fields are marked as nullStr in the CSV).
func ToSoftDeletedRow(
	csvRow []string,
	dbRow []any,
	fivetranSyncedIdx uint,
	fivetranDeletedIdx uint,
) ([]any, error) {
	if fivetranDeletedIdx >= uint(len(csvRow)) {
		return nil, fmt.Errorf("can't find column %s with index %d in a CSV row",
			constants.FivetranDeleted, fivetranDeletedIdx)
	}
	if fivetranSyncedIdx >= uint(len(csvRow)) {
		return nil, fmt.Errorf("can't find column %s with index %d in a CSV row",
			constants.FivetranSynced, fivetranSyncedIdx)
	}
	if len(dbRow) < 2 {
		return nil, fmt.Errorf("expected ClickHouse row to contain at least 2 columns, but got %d", len(dbRow))
	}
	updatedRow := make([]any, len(dbRow))
	copy(updatedRow, dbRow)
	updatedRow[fivetranDeletedIdx] = true
	fivetranSynced, err := time.Parse(constants.UTCDateTimeFormat, csvRow[fivetranSyncedIdx])
	if err != nil {
		return nil, fmt.Errorf("can't parse value %s as UTC datetime for column %s: %w",
			csvRow[fivetranSyncedIdx], constants.FivetranSynced, err)
	}
	updatedRow[fivetranSyncedIdx] = fivetranSynced
	return updatedRow, nil
}
