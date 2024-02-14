package main

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/shopspring/decimal"
)

type RowsByPrimaryKeyValue map[string][]interface{}

func ColumnTypesToEmptyRows(columnTypes []driver.ColumnType, n uint) [][]interface{} {
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

func GetDatabaseRowMappingKey(row []interface{}, pkCols []*PrimaryKeyColumn) (string, error) {
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
			// should exactly match CSV datetime format
			if col.Type == pb.DataType_UTC_DATETIME {
				key.WriteString(p.Format("2006-01-02T15:04:05.000000000Z"))
			} else if col.Type == pb.DataType_NAIVE_DATETIME {
				key.WriteString(p.Format("2006-01-02T15:04:05"))
			} else {
				key.WriteString(p.Format("2006-01-02"))
			}
		case *decimal.Decimal:
			key.WriteString(p.String())
		default: // JSON is not supported as a primary key atm
			return "", fmt.Errorf("can't use type %T as mapping key", p)
		}
		if i < len(pkCols)-1 {
			key.WriteString(",")
		}
	}
	return key.String(), nil
}

func GetCSVRowMappingKey(row CSVRow, pkCols []*PrimaryKeyColumn) (string, error) {
	if len(pkCols) == 0 {
		return "", fmt.Errorf("expected non-empty list of primary keys columns")
	}
	var key strings.Builder
	for i, col := range pkCols {
		key.WriteString(col.Name)
		key.WriteRune(':')
		key.WriteString(row[col.Index])
		if i < len(pkCols)-1 {
			key.WriteRune(',')
		}
	}
	return key.String(), nil
}
