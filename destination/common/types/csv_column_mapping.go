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
	driverColMap map[string]*DriverColumn,
	csvHeader []string,
	fivetranColMap map[string]*pb.Column,
) ([]*CSVColumn, error) {
	if len(csvHeader) == 0 {
		return nil, fmt.Errorf("input file header is empty")
	}
	if len(fivetranColMap) == 0 {
		return nil, fmt.Errorf("table definition is empty")
	}
	if len(driverColMap) != len(csvHeader) {
		return nil, fmt.Errorf(
			"columns count in ClickHouse table (%d) does not match the input file (%d). Expected columns: %s, got: %s",
			len(driverColMap), len(csvHeader), "", strings.Join(csvHeader, ", "), // TODO <- list all columns
		)
	}
	if len(fivetranColMap) != len(csvHeader) {
		return nil, fmt.Errorf(
			"columns count in the table definition (%d) does not match the input file (%d). Expected columns: %s, got: %s",
			len(fivetranColMap), len(csvHeader), "", strings.Join(csvHeader, ", "), // TODO <- list all columns
		)
	}
	csvMapping := make([]*CSVColumn, len(csvHeader))
	for i, csvColName := range csvHeader {
		driverColType, ok := driverColMap[csvColName]
		if !ok {
			return nil, fmt.Errorf(
				"column %s was not found in the input file. ClickHouse columns: %s; input file columns: %s",
				csvColName, "", strings.Join(csvHeader, ", "), // TODO <- list all columns
			)
		}
		fivetranCol, ok := fivetranColMap[csvColName]
		if !ok {
			return nil, fmt.Errorf(
				"column %s was not found in the table definition. Table columns: %s; input file columns: %s",
				csvColName, "", strings.Join(csvHeader, ", ")) // TODO <- list all columns
		}
		csvMapping[i] = &CSVColumn{
			Index:        uint(i),
			TableIndex:   driverColType.Index,
			Name:         csvColName,
			Type:         fivetranCol.Type,
			IsPrimaryKey: fivetranCol.PrimaryKey,
		}
	}
	return csvMapping, nil
}

// format columns as a string in the right order (using their database table definition index values).
func joinColTypes(colTypes []*DriverColumn) string {
	var sb strings.Builder
	for i, col := range colTypes {
		sb.WriteString(col.Name)
		if i < len(colTypes)-1 {
			sb.WriteString(", ")
		}
	}
	return sb.String()
}

func joinFivetranTableColumns(table *pb.Table) string {
	var sb strings.Builder
	for i, col := range table.Columns {
		sb.WriteString(col.Name)
		if i < len(table.Columns)-1 {
			sb.WriteString(", ")
		}
	}
	return sb.String()
}
