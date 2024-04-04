package types

import "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

func MakeDriverColumns(colTypes []driver.ColumnType) *DriverColumns {
	mapping := make(map[string]*DriverColumn, len(colTypes))
	columns := make([]*DriverColumn, len(colTypes))
	for i, colType := range colTypes {
		col := &DriverColumn{
			Index:        uint(i),
			Name:         colType.Name(),
			ScanType:     colType.ScanType(),
			DatabaseType: colType.DatabaseTypeName(),
		}
		mapping[colType.Name()] = col
		columns[i] = col
	}
	return &DriverColumns{
		Mapping: mapping,
		Columns: columns,
	}
}
