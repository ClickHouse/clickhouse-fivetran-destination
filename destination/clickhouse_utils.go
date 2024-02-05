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

func GetFullTableName(schemaName string, tableName string) string {
	var fullName string
	if schemaName == "" {
		fullName = fmt.Sprintf("`%s`", tableName)
	} else {
		fullName = fmt.Sprintf("`%s`.`%s`", schemaName, tableName)
	}
	return fullName
}

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

func GetDatabaseRowMappingKey(row []interface{}, pkCols []*PrimaryKeyColumn) string {
	var key strings.Builder
	for _, col := range pkCols {
		key.WriteString(ToString(row[col.Index]))
	}
	res := key.String()
	return res
}

func GetCSVRowMappingKey(row CSVRow, pkCols []*PrimaryKeyColumn) (string, error) {
	var key strings.Builder
	for _, col := range pkCols {
		val, err := ParseValue(col.Name, col.Type, row[col.Index])
		if err != nil {
			return "", err
		}
		key.WriteString(ToString(val))
	}
	return key.String(), nil
}

func ToString(p interface{}) string {
	switch p.(type) {
	case *string:
		return *p.(*string)
	case *int:
		return fmt.Sprint(*p.(*int))
	case *int16:
		return fmt.Sprint(*p.(*int16))
	case *int32:
		return fmt.Sprint(*p.(*int32))
	case *int64:
		return fmt.Sprint(*p.(*int64))
	case *float32:
		return fmt.Sprint(*p.(*float32))
	case *float64:
		return fmt.Sprint(*p.(*float64))
	case *bool:
		return fmt.Sprint(*p.(*bool))
	case *time.Time:
		return fmt.Sprint(p.(*time.Time).Nanosecond())
	case *decimal.Decimal:
		return p.(*decimal.Decimal).String()
	default:
		return fmt.Sprint(p)
	}
}
