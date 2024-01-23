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

type AlterTableOpType int

const (
	Add AlterTableOpType = iota
	Modify
	Drop
)

type AlterTableOp struct {
	op     AlterTableOpType
	column string
	dbType *string // not needed for Drop
}

func GetAlterTableOps(current *TableDescription, alter *TableDescription) []*AlterTableOp {
	var ops []*AlterTableOp

	// what columns are missing from the "current" or have a different data type? (add + modify)
	for _, colName := range alter.columns {
		alterColType := alter.mapping[colName]
		curColType, ok := current.mapping[colName]
		if !ok {
			dbType := alter.mapping[colName]
			ops = append(ops, &AlterTableOp{
				op:     Add,
				column: colName,
				dbType: &dbType,
			})
		}
		if curColType != alterColType {
			ops = append(ops, &AlterTableOp{
				op:     Modify,
				column: colName,
				dbType: &alterColType,
			})
		}
	}

	// what columns are missing from the "alter"? (drop)
	for _, colName := range current.columns {
		_, ok := alter.mapping[colName]
		if !ok {
			ops = append(ops, &AlterTableOp{
				op:     Drop,
				column: colName,
			})
		}
	}

	return ops
}
