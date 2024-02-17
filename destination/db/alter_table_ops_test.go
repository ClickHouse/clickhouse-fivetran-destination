package db

import (
	"testing"

	"fivetran.com/fivetran_sdk/destination/common/types"
	"github.com/stretchr/testify/assert"
)

func TestGetAlterTableOpsModify(t *testing.T) {
	int64Type := "Int64"
	emptyComment := ""
	curCol1 := &types.ColumnDefinition{Name: "qaz", Type: "Int32", IsPrimaryKey: true}
	curCol2 := &types.ColumnDefinition{Name: "qux", Type: "String", IsPrimaryKey: false}
	alterCol1 := &types.ColumnDefinition{Name: "qaz", Type: "Int32", IsPrimaryKey: true}
	alterCol2 := &types.ColumnDefinition{Name: "qux", Type: "Int64", IsPrimaryKey: false}
	ops := GetAlterTableOps(
		&types.TableDescription{
			Mapping: map[string]*types.ColumnDefinition{"qaz": curCol1, "qux": curCol2},
			Columns: []*types.ColumnDefinition{curCol1, curCol2},
		},
		&types.TableDescription{
			Mapping: map[string]*types.ColumnDefinition{"qaz": alterCol1, "qux": alterCol2},
			Columns: []*types.ColumnDefinition{alterCol1, alterCol2},
		})
	assert.Equal(t, ops, []*types.AlterTableOp{{Op: types.AlterTableModify, Column: "qux", Type: &int64Type, Comment: &emptyComment}})
}

func TestGetAlterTableOpsAdd(t *testing.T) {
	int64Type := "Int64"
	emptyComment := ""
	curCol1 := &types.ColumnDefinition{Name: "qaz", Type: "Int32", IsPrimaryKey: true}
	curCol2 := &types.ColumnDefinition{Name: "qux", Type: "String", IsPrimaryKey: false}
	alterCol1 := &types.ColumnDefinition{Name: "qaz", Type: "Int32", IsPrimaryKey: true}
	alterCol2 := &types.ColumnDefinition{Name: "qux", Type: "String", IsPrimaryKey: false}
	alterCol3 := &types.ColumnDefinition{Name: "zaq", Type: "Int64", IsPrimaryKey: false}
	ops := GetAlterTableOps(
		&types.TableDescription{
			Mapping: map[string]*types.ColumnDefinition{"qaz": curCol1, "qux": curCol2},
			Columns: []*types.ColumnDefinition{curCol1, curCol2},
		},
		&types.TableDescription{
			Mapping: map[string]*types.ColumnDefinition{"qaz": alterCol1, "qux": alterCol2, "zaq": alterCol3},
			Columns: []*types.ColumnDefinition{alterCol1, alterCol2, alterCol3},
		})
	assert.Equal(t, ops, []*types.AlterTableOp{{Op: types.AlterTableAdd, Column: "zaq", Type: &int64Type, Comment: &emptyComment}})

}

func TestGetAlterTableOpsDrop(t *testing.T) {
	curCol1 := &types.ColumnDefinition{Name: "qaz", Type: "Int32", IsPrimaryKey: true}
	curCol2 := &types.ColumnDefinition{Name: "qux", Type: "String", IsPrimaryKey: false}
	alterCol1 := &types.ColumnDefinition{Name: "qaz", Type: "Int32", IsPrimaryKey: true}
	ops := GetAlterTableOps(
		&types.TableDescription{
			Mapping: map[string]*types.ColumnDefinition{"qaz": curCol1, "qux": curCol2},
			Columns: []*types.ColumnDefinition{curCol1, curCol2},
		},
		&types.TableDescription{
			Mapping: map[string]*types.ColumnDefinition{"qaz": alterCol1},
			Columns: []*types.ColumnDefinition{alterCol1},
		})
	assert.Equal(t, ops, []*types.AlterTableOp{{Op: types.AlterTableDrop, Column: "qux", Type: nil, Comment: nil}})
}

func TestGetAlterTableOpsCombined(t *testing.T) {
	int64Type := "Int64"
	int16Type := "Int16"
	emptyComment := ""
	curCol1 := &types.ColumnDefinition{Name: "qaz", Type: "Int32", IsPrimaryKey: true}
	curCol2 := &types.ColumnDefinition{Name: "qux", Type: "String", IsPrimaryKey: false}
	alterCol1 := &types.ColumnDefinition{Name: "qux", Type: "Int64", IsPrimaryKey: false}
	alterCol2 := &types.ColumnDefinition{Name: "zaq", Type: "Int16", IsPrimaryKey: false}
	ops := GetAlterTableOps(
		&types.TableDescription{
			Mapping: map[string]*types.ColumnDefinition{"qaz": curCol1, "qux": curCol2},
			Columns: []*types.ColumnDefinition{curCol1, curCol2},
		},
		&types.TableDescription{
			Mapping: map[string]*types.ColumnDefinition{"qux": alterCol1, "zaq": alterCol2},
			Columns: []*types.ColumnDefinition{alterCol1, alterCol2},
		})
	assert.Equal(t, ops, []*types.AlterTableOp{
		{Op: types.AlterTableModify, Column: "qux", Type: &int64Type, Comment: &emptyComment},
		{Op: types.AlterTableAdd, Column: "zaq", Type: &int16Type, Comment: &emptyComment},
		{Op: types.AlterTableDrop, Column: "qaz", Type: nil, Comment: nil},
	})
}

func TestGetAlterTableOpsEqualTables(t *testing.T) {
	// Make sure they all are different pointers
	curCol1 := &types.ColumnDefinition{Name: "qaz", Type: "Int32", IsPrimaryKey: true}
	curCol2 := &types.ColumnDefinition{Name: "qux", Type: "String", IsPrimaryKey: false}
	alterCol1 := &types.ColumnDefinition{Name: "qaz", Type: "Int32", IsPrimaryKey: true}
	alterCol2 := &types.ColumnDefinition{Name: "qux", Type: "String", IsPrimaryKey: false}
	td1 := &types.TableDescription{
		Mapping: map[string]*types.ColumnDefinition{"qaz": curCol1, "qux": curCol2},
		Columns: []*types.ColumnDefinition{curCol1, curCol2},
	}
	td2 := &types.TableDescription{
		Mapping: map[string]*types.ColumnDefinition{"qaz": alterCol1, "qux": alterCol2},
		Columns: []*types.ColumnDefinition{alterCol1, alterCol2},
	}
	ops := GetAlterTableOps(td1, td2)
	assert.Equal(t, ops, []*types.AlterTableOp{})
}

func TestGetAlterTableOpsWithComments(t *testing.T) {
	strType := "String"
	emptyComment := ""
	xmlComment := "XML"
	binaryComment := "BINARY"
	curCol1 := &types.ColumnDefinition{Name: "s1", Type: strType, IsPrimaryKey: true, Comment: emptyComment}
	curCol2 := &types.ColumnDefinition{Name: "s2", Type: strType, IsPrimaryKey: true, Comment: emptyComment}
	curCol3 := &types.ColumnDefinition{Name: "s3", Type: strType, IsPrimaryKey: false, Comment: xmlComment}
	curCol4 := &types.ColumnDefinition{Name: "s4", Type: strType, IsPrimaryKey: false, Comment: binaryComment}
	curCol5 := &types.ColumnDefinition{Name: "s5", Type: strType, IsPrimaryKey: false, Comment: emptyComment}
	alterCol1 := &types.ColumnDefinition{Name: "s1", Type: strType, IsPrimaryKey: true, Comment: binaryComment}
	alterCol2 := &types.ColumnDefinition{Name: "s2", Type: strType, IsPrimaryKey: true, Comment: xmlComment}
	alterCol3 := &types.ColumnDefinition{Name: "s3", Type: strType, IsPrimaryKey: false, Comment: emptyComment}
	alterCol4 := &types.ColumnDefinition{Name: "s4", Type: strType, IsPrimaryKey: false, Comment: emptyComment}
	alterCol5 := &types.ColumnDefinition{Name: "s10", Type: strType, IsPrimaryKey: false, Comment: emptyComment}
	alterCol6 := &types.ColumnDefinition{Name: "s11", Type: strType, IsPrimaryKey: false, Comment: binaryComment}
	alterCol7 := &types.ColumnDefinition{Name: "s12", Type: strType, IsPrimaryKey: false, Comment: xmlComment}
	ops := GetAlterTableOps(
		&types.TableDescription{
			Mapping: map[string]*types.ColumnDefinition{
				"s1": curCol1, "s2": curCol2, "s3": curCol3, "s4": curCol4, "s5": curCol5,
			},
			Columns: []*types.ColumnDefinition{curCol1, curCol2, curCol3, curCol4, curCol5},
		},
		&types.TableDescription{
			Mapping: map[string]*types.ColumnDefinition{
				"s1": alterCol1, "s2": alterCol2, "s3": alterCol3, "s4": alterCol4,
				"s10": alterCol5, "s11": alterCol6, "s12": alterCol7,
			},
			Columns: []*types.ColumnDefinition{alterCol1, alterCol2, alterCol3, alterCol4, alterCol5, alterCol6, alterCol7},
		})
	assert.Equal(t, ops, []*types.AlterTableOp{
		{Op: types.AlterTableModify, Column: "s1", Type: &strType, Comment: &binaryComment},
		{Op: types.AlterTableModify, Column: "s2", Type: &strType, Comment: &xmlComment},
		{Op: types.AlterTableModify, Column: "s3", Type: &strType, Comment: &emptyComment},
		{Op: types.AlterTableModify, Column: "s4", Type: &strType, Comment: &emptyComment},
		{Op: types.AlterTableAdd, Column: "s10", Type: &strType, Comment: &emptyComment},
		{Op: types.AlterTableAdd, Column: "s11", Type: &strType, Comment: &binaryComment},
		{Op: types.AlterTableAdd, Column: "s12", Type: &strType, Comment: &xmlComment},
		{Op: types.AlterTableDrop, Column: "s5", Type: nil, Comment: nil},
	})
}
