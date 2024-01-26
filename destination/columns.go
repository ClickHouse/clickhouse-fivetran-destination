package main

import (
	pb "fivetran.com/fivetran_sdk/proto"
)

const MaxDecimalPrecision = 76

type ColumnDefinition struct {
	Name         string
	Type         string
	IsPrimaryKey bool
}

type TableDescription struct {
	Mapping     map[string]string   // column name -> db type mapping (unordered)
	Columns     []*ColumnDefinition // all the information about the columns (ordered)
	PrimaryKeys []string
}

func MakeTableDescription(columnDefinitions []*ColumnDefinition) *TableDescription {
	mapping := make(map[string]string, len(columnDefinitions))
	var primaryKeys []string
	for _, col := range columnDefinitions {
		mapping[col.Name] = col.Type
		if col.IsPrimaryKey {
			primaryKeys = append(primaryKeys, col.Name)
		}
	}
	return &TableDescription{
		Mapping: mapping,
		Columns: columnDefinitions,
	}
}

func ToFivetranColumns(description *TableDescription) ([]*pb.Column, error) {
	columns := make([]*pb.Column, len(description.Columns))
	i := 0
	for _, col := range description.Columns {
		fivetranType, decimalParams, err := GetFivetranDataType(col.Type)
		if err != nil {
			return nil, err
		}
		columns[i] = &pb.Column{
			Name:       col.Name,
			Type:       fivetranType,
			PrimaryKey: col.IsPrimaryKey,
			Decimal:    decimalParams,
		}
		i++
	}
	return columns, nil
}

func ToClickHouseColumns(table *pb.Table) (*TableDescription, error) {
	result := make([]*ColumnDefinition, len(table.Columns))
	for i, column := range table.Columns {
		colType, err := GetClickHouseDataType(column)
		if err != nil {
			return nil, err
		}
		result[i] = &ColumnDefinition{
			Name:         column.Name,
			Type:         colType,
			IsPrimaryKey: column.PrimaryKey,
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
	for _, col := range alter.Columns {
		curColType, ok := current.Mapping[col.Name]
		if !ok {
			ops = append(ops, &AlterTableOp{
				Op:     Add,
				Column: col.Name,
				Type:   &col.Type,
			})
		}
		if curColType != col.Type {
			ops = append(ops, &AlterTableOp{
				Op:     Modify,
				Column: col.Name,
				Type:   &col.Type,
			})
		}
	}

	// what columns are missing from the "alter"? (drop)
	for _, col := range current.Columns {
		_, ok := alter.Mapping[col.Name]
		if !ok {
			ops = append(ops, &AlterTableOp{
				Op:     Drop,
				Column: col.Name,
			})
		}
	}

	return ops
}
