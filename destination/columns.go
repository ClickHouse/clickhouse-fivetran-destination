package main

import (
	pb "fivetran.com/fivetran_sdk/proto"
)

const MaxDecimalPrecision = 76

type ColumnDefinition struct {
	name   string
	dbType string
}

type TableDescription struct {
	mapping map[string]string // column name -> db type mapping (unordered)
	columns []string          // preserves the correct order of the columns
}

func MakeTableDescription(columnDefinitions []*ColumnDefinition) *TableDescription {
	mapping := make(map[string]string, len(columnDefinitions))
	var columns = make([]string, len(columnDefinitions))

	for i, col := range columnDefinitions {
		mapping[col.name] = col.dbType
		columns[i] = col.name
	}

	return &TableDescription{
		mapping: mapping,
		columns: columns,
	}
}

func ToFivetranColumns(description *TableDescription) []*pb.Column {
	columns := make([]*pb.Column, len(description.columns))
	i := 0
	for _, colName := range description.columns {
		columns[i] = &pb.Column{
			Name:       colName,
			Type:       GetFivetranDataType(description.mapping[colName]),
			PrimaryKey: false,
			Decimal:    nil,
		}
		i++
	}
	return columns
}

func ToClickHouseColumns(table *pb.Table) (*TableDescription, error) {
	result := make([]*ColumnDefinition, len(table.Columns))
	for i, column := range table.Columns {
		colType, err := GetClickHouseColumnType(column.Type, column.Decimal)
		if err != nil {
			return nil, err
		}
		result[i] = &ColumnDefinition{
			name:   column.Name,
			dbType: colType,
		}
	}
	return MakeTableDescription(result), nil
}

type AlterTableDiff struct {
	add    []ColumnDefinition
	modify []ColumnDefinition
	drop   []string
}

func GetAlterTableDiff(current *TableDescription, alter *TableDescription) *AlterTableDiff {
	var add []ColumnDefinition
	var modify []ColumnDefinition
	var drop []string

	// what columns are missing from the "current" or have a different data type? (add + modify)
	for _, colName := range alter.columns {
		alterColType := alter.mapping[colName]
		curColType, ok := current.mapping[colName]
		if !ok {
			add = append(add, ColumnDefinition{
				name:   colName,
				dbType: alter.mapping[colName],
			})
		}
		if curColType != alterColType {
			modify = append(modify, ColumnDefinition{
				name:   colName,
				dbType: alterColType,
			})
		}
	}

	// what columns are missing from the "alter"? (drop)
	for _, colName := range current.columns {
		_, ok := alter.mapping[colName]
		if !ok {
			drop = append(drop, colName)
		}
	}

	return &AlterTableDiff{
		add:    add,
		modify: modify,
		drop:   drop,
	}
}
