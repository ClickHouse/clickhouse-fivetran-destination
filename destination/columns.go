package main

import (
	pb "fivetran.com/fivetran_sdk/proto"
)

const MaxDecimalPrecision = 76

type ColumnDefinition struct {
	Name string
	Type string
}

type TableDescription struct {
	Mapping map[string]string // column name -> db type mapping (unordered)
	Columns []string          // preserves the correct order of the columns
}

func MakeTableDescription(columnDefinitions []*ColumnDefinition) *TableDescription {
	mapping := make(map[string]string, len(columnDefinitions))
	var columns = make([]string, len(columnDefinitions))

	for i, col := range columnDefinitions {
		mapping[col.Name] = col.Type
		columns[i] = col.Name
	}

	return &TableDescription{
		Mapping: mapping,
		Columns: columns,
	}
}

func ToFivetranColumns(description *TableDescription) []*pb.Column {
	columns := make([]*pb.Column, len(description.Columns))
	i := 0
	for _, colName := range description.Columns {
		columns[i] = &pb.Column{
			Name:       colName,
			Type:       GetFivetranDataType(description.Mapping[colName]),
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
			Name: column.Name,
			Type: colType,
		}
	}
	return MakeTableDescription(result), nil
}

type AlterTableOpType int

const (
	Add AlterTableOpType = iota
	Modify
	Drop
)

type AlterTableOp struct {
	Op     AlterTableOpType
	Column string
	Type   *string // not needed for Drop
}

func GetAlterTableOps(current *TableDescription, alter *TableDescription) []*AlterTableOp {
	var ops []*AlterTableOp

	// what columns are missing from the "current" or have a different Data type? (add + modify)
	for _, colName := range alter.Columns {
		alterColType := alter.Mapping[colName]
		curColType, ok := current.Mapping[colName]
		if !ok {
			dbType := alter.Mapping[colName]
			ops = append(ops, &AlterTableOp{
				Op:     Add,
				Column: colName,
				Type:   &dbType,
			})
		}
		if curColType != alterColType {
			ops = append(ops, &AlterTableOp{
				Op:     Modify,
				Column: colName,
				Type:   &alterColType,
			})
		}
	}

	// what columns are missing from the "alter"? (drop)
	for _, colName := range current.Columns {
		_, ok := alter.Mapping[colName]
		if !ok {
			ops = append(ops, &AlterTableOp{
				Op:     Drop,
				Column: colName,
			})
		}
	}

	return ops
}
