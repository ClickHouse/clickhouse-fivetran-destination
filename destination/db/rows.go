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
	"github.com/shopspring/decimal"
)

// RowsByPrimaryKeyValue contains existing ClickHouse rows mapped using GetDatabaseRowMappingKey.
// It is used to ensure that we don't have records with different position in the selected DB rows vs CSV,
// which could lead to corrupted updates as the records will be mixed up.
// TODO: maybe add CSV index to the SELECT query to avoid all this?
type RowsByPrimaryKeyValue map[string][]interface{}

// ColumnTypesToEmptyScanRows creates a slice of N empty rows to scan the selected data into,
// with correct types at every index based on the introspected table column types from ClickHouse.
func ColumnTypesToEmptyScanRows(driverColumns *types.DriverColumns, n uint) [][]interface{} {
	dbRows := make([][]interface{}, n)
	for i := range dbRows {
		dbRow := make([]interface{}, len(driverColumns.Columns))
		for j := range dbRow {
			dbRow[j] = reflect.New(driverColumns.Columns[j].ScanType).Interface()
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
func GetDatabaseRowMappingKey(row []interface{}, csvCols *types.CSVColumns) (string, error) {
	if csvCols == nil || len(csvCols.PrimaryKeys) == 0 {
		return "", fmt.Errorf("expected non-empty list of primary keys columns")
	}
	var key strings.Builder
	for i, col := range csvCols.PrimaryKeys {
		key.WriteString(col.Name)
		key.WriteRune(':')
		p := row[col.TableIndex]
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
			switch col.Type {
			case pb.DataType_UTC_DATETIME:
				key.WriteString(fmt.Sprint(p.UnixNano()))
			case pb.DataType_NAIVE_DATETIME:
				key.WriteString(p.Format(constants.NaiveDateTimeFormat))
			default:
				key.WriteString(p.Format(constants.NaiveDateFormat))
			}
		case *decimal.Decimal:
			key.WriteString(p.String())
		default:
			return "", fmt.Errorf("can't use type %T as mapping key", p)
		}
		if i < len(csvCols.PrimaryKeys)-1 {
			key.WriteString(",")
		}
	}
	return key.String(), nil
}

// GetCSVRowMappingKey is similar to GetDatabaseRowMappingKey, but for CSV rows.
func GetCSVRowMappingKey(csvRow []string, csvCols *types.CSVColumns, isHistoryMode bool) (string, error) {
	if csvCols == nil || len(csvCols.PrimaryKeys) == 0 {
		return "", fmt.Errorf("expected non-empty list of primary keys columns")
	}
	if isHistoryMode {
		csvCols.RemovePrimaryKey(constants.FivetranStart)
	}
	var key strings.Builder
	for i, col := range csvCols.PrimaryKeys {
		key.WriteString(col.Name)
		key.WriteRune(':')
		// reformat UTC datetime as nanos (due to possibly variable precision)
		if col.Type == pb.DataType_UTC_DATETIME {
			t, err := time.Parse(constants.UTCDateTimeFormat, csvRow[col.Index])
			if err != nil {
				return "", fmt.Errorf("can't parse value %s as UTC datetime for column %s: %w",
					csvRow[col.Index], col.Name, err)
			}
			key.WriteString(fmt.Sprint(t.UnixNano()))
		} else {
			key.WriteString(csvRow[col.Index])
		}
		if i < len(csvCols.PrimaryKeys)-1 {
			key.WriteRune(',')
		}
	}
	return key.String(), nil
}

// MergeUpdatedRows merges a CSV with existing ClickHouse rows to create a batch of rows to insert back to the database.
// `selectRows` are fetched from ClickHouse in advance using primary key values from CSV records.
// See also: ToUpdatedRow.
func MergeUpdatedRows(
	csv [][]string,
	selectRows RowsByPrimaryKeyValue,
	csvCols *types.CSVColumns,
	nullStr string,
	unmodifiedStr string,
	isHistoryMode bool,
) (insertRows [][]interface{}, skipIdx map[int]bool, err error) {
	insertRows = make([][]interface{}, len(csv))
	skipIdx = make(map[int]bool)
	for j, csvRow := range csv {
		mappingKey, err := GetCSVRowMappingKey(csvRow, csvCols, isHistoryMode)
		if err != nil {
			return nil, nil, err
		}
		dbRow, exists := selectRows[mappingKey]
		if exists {
			updatedRow, err := ToUpdatedRow(csvRow, dbRow, csvCols, nullStr, unmodifiedStr)
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

// ToInsertRow converts a CSV row to a ClickHouse row, parsing strings and converting them to the correct types.
func ToInsertRow(
	csvRow []string,
	csvColumns *types.CSVColumns,
	nullStr string,
) ([]any, error) {
	if nullStr == "" {
		return nil, fmt.Errorf("nullStr can't be empty")
	}
	if csvColumns == nil {
		return nil, fmt.Errorf("table can't be nil")
	}
	if len(csvColumns.All) != len(csvRow) {
		return nil, fmt.Errorf("expected %d columns, but CSV row contains %d", len(csvColumns.All), len(csvRow))
	}
	insertRow := make([]any, len(csvRow))
	for i, col := range csvColumns.All {
		// as a CSV may come in "shuffled", get the correct ClickHouse column index
		if csvRow[i] == nullStr {
			insertRow[col.TableIndex] = nil
			continue
		}
		value, err := values.Parse(col.Name, col.Type, csvRow[i])
		if err != nil {
			return nil, err
		}
		insertRow[col.TableIndex] = value
	}
	return insertRow, nil
}

// ToUpdatedRow merges an existing ClickHouse row with the CSV row values.
// Fields that are equal to unmodifiedStr are not updated.
// csvColumns - all CSV columns (not just primary keys).
func ToUpdatedRow(
	csvRow []string,
	dbRow []any,
	csvColumns *types.CSVColumns,
	nullStr string,
	unmodifiedStr string,
) ([]any, error) {
	if unmodifiedStr == "" {
		return nil, fmt.Errorf("unmodifiedStr can't be empty")
	}
	if nullStr == "" {
		return nil, fmt.Errorf("nullStr can't be empty")
	}
	if csvColumns == nil {
		return nil, fmt.Errorf("CSV columns can't be empty")
	}
	if len(csvRow) != len(dbRow) || len(dbRow) != len(csvColumns.All) {
		return nil, fmt.Errorf(
			"expected CSV, table definition and ClickHouse row to contain the same number of columns, "+
				"but got %d, %d and %d", len(csvRow), len(csvColumns.All), len(dbRow))
	}
	updatedRow := make([]any, len(dbRow))
	for i, value := range csvRow {
		// as a CSV may come in "shuffled", get the correct ClickHouse column index
		tableColIndex := csvColumns.All[i].TableIndex
		if value == nullStr {
			updatedRow[tableColIndex] = nil
			continue
		}
		if value == unmodifiedStr {
			updatedRow[tableColIndex] = dbRow[tableColIndex]
			continue
		}
		parsedValue, err := values.Parse(csvColumns.All[i].Name, csvColumns.All[i].Type, value)
		if err != nil {
			return nil, err
		}
		updatedRow[tableColIndex] = parsedValue
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
