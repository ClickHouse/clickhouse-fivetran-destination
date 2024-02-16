package main

import (
	"fmt"

	pb "fivetran.com/fivetran_sdk/proto"
)

type DecimalParams struct {
	Precision uint64
	Scale     uint64
}

// ColumnDefinition as it is defined or should be defined in ClickHouse
type ColumnDefinition struct {
	Name          string
	Type          string
	Comment       string
	IsPrimaryKey  bool
	DecimalParams *DecimalParams // only for Decimal types, nil otherwise
}

// TableDescription
// Mapping is ColumnDefinition.Name -> ColumnDefinition (unordered)
// Columns are the same as in Mapping, but ordered, used to preserve column order for CREATE TABLE statement generation
// PrimaryKeys is a convenience list of ColumnDefinition.Name that are primary keys
type TableDescription struct {
	Mapping     map[string]*ColumnDefinition
	Columns     []*ColumnDefinition
	PrimaryKeys []string
}

func MakeTableDescription(columnDefinitions []*ColumnDefinition) *TableDescription {
	if len(columnDefinitions) == 0 {
		return &TableDescription{}
	}
	mapping := make(map[string]*ColumnDefinition, len(columnDefinitions))
	var primaryKeys []string
	for _, col := range columnDefinitions {
		mapping[col.Name] = col
		if col.IsPrimaryKey {
			primaryKeys = append(primaryKeys, col.Name)
		}
	}
	return &TableDescription{
		Mapping:     mapping,
		Columns:     columnDefinitions,
		PrimaryKeys: primaryKeys,
	}
}

func ToFivetranColumns(description *TableDescription) ([]*pb.Column, error) {
	if description == nil || len(description.Columns) == 0 {
		return []*pb.Column{}, nil
	}
	columns := make([]*pb.Column, len(description.Columns))
	i := 0
	for _, col := range description.Columns {
		fivetranType, decimalParams, err := GetFivetranDataType(col)
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
	if table == nil || len(table.Columns) == 0 {
		return nil, fmt.Errorf("no columns in Fivetran table definition")
	}
	result := make([]*ColumnDefinition, len(table.Columns))
	for i, column := range table.Columns {
		chType, err := GetClickHouseDataType(column)
		if err != nil {
			return nil, err
		}
		result[i] = &ColumnDefinition{
			Name:         column.Name,
			Type:         chType.Type,
			Comment:      chType.Comment,
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
	Op      AlterTableOpType
	Column  string
	Type    *string // not needed for Drop
	Comment *string // not needed for Drop
}

func GetAlterTableOps(current *TableDescription, alter *TableDescription) []*AlterTableOp {
	var ops = make([]*AlterTableOp, 0)

	// what columns are missing from the "current" or have a different Data type? (add + modify)
	for _, alterCol := range alter.Columns {
		curCol, ok := current.Mapping[alterCol.Name]
		if !ok {
			ops = append(ops, &AlterTableOp{
				Op:      Add,
				Column:  alterCol.Name,
				Type:    &alterCol.Type,
				Comment: &alterCol.Comment,
			})
		} else if curCol.Type != alterCol.Type || curCol.Comment != alterCol.Comment {
			ops = append(ops, &AlterTableOp{
				Op:      Modify,
				Column:  alterCol.Name,
				Type:    &alterCol.Type,
				Comment: &alterCol.Comment,
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
