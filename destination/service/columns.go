package service

import (
	"fmt"

	"fivetran.com/fivetran_sdk/destination/common/constants"
	dt "fivetran.com/fivetran_sdk/destination/common/data_types"
	"fivetran.com/fivetran_sdk/destination/common/types"
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
	return types.MakeTableDescription(result), nil
}

func GetPrimaryKeysAndMetadataColumns(table *pb.Table) (*types.PrimaryKeysAndMetadataColumns, error) {
	if table == nil || len(table.Columns) == 0 {
		return nil, fmt.Errorf("no columns in Fivetran table definition")
	}
	var pkCols []*types.PrimaryKeyColumn
	fivetranSyncedIdx := -1
	fivetranDeletedIdx := -1
	for i, col := range table.Columns {
		if col.PrimaryKey {
			pkCols = append(pkCols, &types.PrimaryKeyColumn{
				Name:  col.Name,
				Type:  col.Type,
				Index: uint(i),
			})
		}
		if col.Name == constants.FivetranSynced {
			fivetranSyncedIdx = i
		}
		if col.Name == constants.FivetranDeleted {
			fivetranDeletedIdx = i
		}
	}
	if len(pkCols) == 0 {
		return nil, fmt.Errorf("no primary keys found")
	}
	if fivetranSyncedIdx < 0 {
		return nil, fmt.Errorf("no %s column found", constants.FivetranSynced)
	}
	if fivetranDeletedIdx < 0 {
		return nil, fmt.Errorf("no %s column found", constants.FivetranDeleted)
	}
	return &types.PrimaryKeysAndMetadataColumns{
		PrimaryKeys:        pkCols,
		FivetranSyncedIdx:  uint(fivetranSyncedIdx),
		FivetranDeletedIdx: uint(fivetranDeletedIdx),
	}, nil
}
