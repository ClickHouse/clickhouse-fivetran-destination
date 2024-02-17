package db

import (
	"fivetran.com/fivetran_sdk/destination/common/types"
)

// GetAlterTableOps returns a list of operations to alter the table from the current to the new definition.
// `from` is the table definition from ClickHouse
// `to` is the table definition from a Fivetran AlterTable request
func GetAlterTableOps(from *types.TableDescription, to *types.TableDescription) []*types.AlterTableOp {
	ops := make([]*types.AlterTableOp, 0)

	// what columns are missing from the current table definition or have a different Data type? (add + modify)
	for _, toCol := range to.Columns {
		fromCol, ok := from.Mapping[toCol.Name]
		if !ok {
			ops = append(ops, &types.AlterTableOp{
				Op:      types.AlterTableAdd,
				Column:  toCol.Name,
				Type:    &toCol.Type,
				Comment: &toCol.Comment,
			})
		} else if fromCol.Type != toCol.Type || fromCol.Comment != toCol.Comment {
			ops = append(ops, &types.AlterTableOp{
				Op:      types.AlterTableModify,
				Column:  toCol.Name,
				Type:    &toCol.Type,
				Comment: &toCol.Comment,
			})
		}
	}

	// what columns are missing from the new table definition? (drop)
	for _, fromCol := range from.Columns {
		_, ok := to.Mapping[fromCol.Name]
		if !ok {
			ops = append(ops, &types.AlterTableOp{
				Op:     types.AlterTableDrop,
				Column: fromCol.Name,
			})
		}
	}

	return ops
}
