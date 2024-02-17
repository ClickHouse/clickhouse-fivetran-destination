package service

import (
	"fmt"

	dt "fivetran.com/fivetran_sdk/destination/common/data_types"
	"fivetran.com/fivetran_sdk/destination/common/types"
	"fivetran.com/fivetran_sdk/destination/db"
	pb "fivetran.com/fivetran_sdk/proto"
)

func ToFivetran(description *types.TableDescription) ([]*pb.Column, error) {
	if description == nil || len(description.Columns) == 0 {
		return []*pb.Column{}, nil
	}
	columns := make([]*pb.Column, len(description.Columns))
	i := 0
	for _, col := range description.Columns {
		fivetranType, decimalParams, err := dt.ToFivetranDataType(col.Type, col.Comment, col.DecimalParams)
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

func ToClickHouse(table *pb.Table) (*types.TableDescription, error) {
	if table == nil || len(table.Columns) == 0 {
		return nil, fmt.Errorf("no columns in Fivetran table definition")
	}
	result := make([]*types.ColumnDefinition, len(table.Columns))
	for i, column := range table.Columns {
		chType, err := dt.ToClickHouseDataType(column)
		if err != nil {
			return nil, err
		}
		result[i] = &types.ColumnDefinition{
			Name:         column.Name,
			Type:         chType.Type,
			Comment:      chType.Comment,
			IsPrimaryKey: column.PrimaryKey,
		}
	}
	return db.MakeTableDescription(result), nil
}