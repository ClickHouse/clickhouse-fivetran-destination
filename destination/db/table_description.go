package db

import (
	"fivetran.com/fivetran_sdk/destination/common/types"
)

// MakeTableDescription ensures that a valid types.TableDescription is created from a list of types.ColumnDefinition.
func MakeTableDescription(columnDefinitions []*types.ColumnDefinition) *types.TableDescription {
	if len(columnDefinitions) == 0 {
		return &types.TableDescription{}
	}
	mapping := make(map[string]*types.ColumnDefinition, len(columnDefinitions))
	var primaryKeys []string
	for _, col := range columnDefinitions {
		mapping[col.Name] = col
		if col.IsPrimaryKey {
			primaryKeys = append(primaryKeys, col.Name)
		}
	}
	return &types.TableDescription{
		Mapping:     mapping,
		Columns:     columnDefinitions,
		PrimaryKeys: primaryKeys,
	}
}
