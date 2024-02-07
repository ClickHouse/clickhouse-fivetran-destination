package main

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/shopspring/decimal"
)

type RowsByPrimaryKeyValue map[string][]interface{}

func ColumnTypesToEmptyRows(columnTypes []driver.ColumnType, n uint32) [][]interface{} {
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
	for _, col := range pkCols {
		str, err := ToString(row[col.Index])
		if err != nil {
			return "", err
		}
		key.WriteString(str)
	}
	res := key.String()
	return res, nil
}

func GetCSVRowMappingKey(row CSVRow, pkCols []*PrimaryKeyColumn) (string, error) {
	if len(pkCols) == 0 {
		return "", fmt.Errorf("expected non-empty list of primary keys columns")
	}
	var key strings.Builder
	for i, col := range pkCols {
		key.WriteString(row[col.Index])
		if i < len(pkCols)-1 {
			key.WriteString("_")
		}
	}
	return key.String(), nil
}

func ToString(p interface{}) (string, error) {
	switch p.(type) {
	case *string:
		return *p.(*string), nil
	case *int:
		return fmt.Sprint(*p.(*int)), nil
	case *int16:
		return fmt.Sprint(*p.(*int16)), nil
	case *int32:
		return fmt.Sprint(*p.(*int32)), nil
	case *int64:
		return fmt.Sprint(*p.(*int64)), nil
	case *float32:
		return fmt.Sprint(*p.(*float32)), nil
	case *float64:
		return fmt.Sprint(*p.(*float64)), nil
	case *bool:
		return fmt.Sprint(*p.(*bool)), nil
	case *time.Time:
		return fmt.Sprint(p.(*time.Time).Nanosecond()), nil
	case *decimal.Decimal:
		return p.(*decimal.Decimal).String(), nil
	default:
		return "", fmt.Errorf("can't call ToString on type %T", p)
	}
}
