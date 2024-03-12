package types

import "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

func MakeDriverColumnMap(colTypes []driver.ColumnType) map[string]*DriverColumn {
	m := make(map[string]*DriverColumn, len(colTypes))
	for i, colType := range colTypes {
		m[colType.Name()] = &DriverColumn{
			Index:        uint(i),
			Name:         colType.Name(),
			ScanType:     colType.ScanType(),
			DatabaseType: colType.DatabaseTypeName(),
		}
	}
	return m
}
