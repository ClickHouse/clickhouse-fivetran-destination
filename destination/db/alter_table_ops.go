package db

import (
	"fivetran.com/fivetran_sdk/destination/common/types"
)

// GetAlterTableOps returns a list of operations to alter the table from the current to the new definition.
// `from` is the table definition from ClickHouse
// `to` is the table definition from a Fivetran AlterTable request
func GetAlterTableOps(
	from *types.TableDescription,
	to *types.TableDescription,
) (ops []*types.AlterTableOp, hasChangedPK bool, unchangedColNames []string, err error) {
	ops = make([]*types.AlterTableOp, 0)
	unchangedColNames = make([]string, 0)
	hasChangedPK = false

	// what columns are missing from the current table definition or have a different Data type? (add + modify)
	for _, toCol := range to.Columns {
		fromCol, ok := from.Mapping[toCol.Name]
		if !ok {
			if toCol.IsPrimaryKey {
				hasChangedPK = true
			}
			ops = append(ops, &types.AlterTableOp{
				Op:      types.AlterTableAdd,
				Column:  toCol.Name,
				Type:    &toCol.Type,
				Comment: &toCol.Comment,
			})
		} else {
			if fromCol.IsPrimaryKey != toCol.IsPrimaryKey {
				hasChangedPK = true
			}
			if fromCol.Type != toCol.Type || fromCol.Comment != toCol.Comment {
				if fromCol.IsPrimaryKey {
					hasChangedPK = true
				}
				ops = append(ops, &types.AlterTableOp{
					Op:      types.AlterTableModify,
					Column:  toCol.Name,
					Type:    &toCol.Type,
					Comment: &toCol.Comment,
				})
			}
		}
	}

	// what columns are missing from the new table definition? (drop)
	for _, fromCol := range from.Columns {
		_, ok := to.Mapping[fromCol.Name]
		if !ok {
			if fromCol.IsPrimaryKey {
				hasChangedPK = true
			}
			ops = append(ops, &types.AlterTableOp{
				Op:     types.AlterTableDrop,
				Column: fromCol.Name,
			})
		} else {
			unchangedColNames = append(unchangedColNames, fromCol.Name)
		}
	}

	return ops, hasChangedPK, unchangedColNames, nil
}
