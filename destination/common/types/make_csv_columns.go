package types

import (
	"fmt"
	"strings"

	pb "fivetran.com/fivetran_sdk/proto"
)

// MakeCSVColumns
// See CSVColumns for more details.
// Additionally, will perform the validation that scan types of the CSV columns are matching the expected database types.
// `dbColIndexMap` contains a mapping of ClickHouse column name to its index in table, exactly how it is in the database.
// `csvHeader` is the first line of the CSV file.
// `table` is taken from the Fivetran request.
func MakeCSVColumns(
	csvHeader []string,
	driverColumns *DriverColumns,
	fivetranColMap map[string]*pb.Column,
) (*CSVColumns, error) {
	if len(csvHeader) == 0 {
		return nil, fmt.Errorf("input file header is empty")
	}
	if len(fivetranColMap) == 0 {
		return nil, fmt.Errorf("table definition is empty")
	}
	if len(driverColumns.Columns) != len(csvHeader) {
		return nil, fmt.Errorf(
			"columns count in ClickHouse table (%d) does not match the input file (%d). Expected columns: %s, got: %s",
			len(driverColumns.Columns), len(csvHeader), joinDriverColumns(driverColumns.Columns), strings.Join(csvHeader, ", "),
		)
	}
	if len(fivetranColMap) != len(csvHeader) {
		return nil, fmt.Errorf(
			"columns count in the table definition (%d) does not match the input file (%d). Expected columns: %s, got: %s",
			len(fivetranColMap), len(csvHeader), joinDriverColumns(driverColumns.Columns), strings.Join(csvHeader, ", "),
		)
	}
	err := CheckScanTypes(fivetranColMap, driverColumns.Mapping)
	if err != nil {
		return nil, err
	}
	allCSVColumns := make([]*CSVColumn, len(csvHeader))
	primaryKeyCSVColumns := make([]*CSVColumn, 0)
	for i, csvColName := range csvHeader {
		driverColType, ok := driverColumns.Mapping[csvColName]
		if !ok {
			return nil, fmt.Errorf(
				"column %s was not found in the input file. ClickHouse columns: %s; input file columns: %s",
				csvColName, joinDriverColumns(driverColumns.Columns), strings.Join(csvHeader, ", "),
			)
		}
		fivetranCol, ok := fivetranColMap[csvColName]
		if !ok {
			return nil, fmt.Errorf(
				"column %s was not found in the table definition. Table columns: %s; input file columns: %s",
				csvColName, joinDriverColumns(driverColumns.Columns), strings.Join(csvHeader, ", "))
		}
		col := &CSVColumn{
			Index:        uint(i),
			TableIndex:   driverColType.Index,
			Name:         csvColName,
			Type:         fivetranCol.Type,
			IsPrimaryKey: fivetranCol.PrimaryKey,
		}
		allCSVColumns[i] = col
		if fivetranCol.PrimaryKey {
			primaryKeyCSVColumns = append(primaryKeyCSVColumns, col)
		}
	}
	if len(primaryKeyCSVColumns) == 0 {
		return nil, fmt.Errorf("no primary key columns found in the input file")
	}
	return &CSVColumns{
		All:         allCSVColumns,
		PrimaryKeys: primaryKeyCSVColumns,
	}, nil
}

// format columns as a string in the right order (using their database table definition index values).
func joinDriverColumns(driverColumns []*DriverColumn) string {
	var sb strings.Builder
	for i, col := range driverColumns {
		sb.WriteString(col.Name)
		if i < len(driverColumns)-1 {
			sb.WriteString(", ")
		}
	}
	return sb.String()
}
