package db

import (
	"testing"

	"fivetran.com/fivetran_sdk/destination/common/types"
	"github.com/stretchr/testify/assert"
)

func TestGetAlterTableOpsModify(t *testing.T) {
	int64Type := "Int64"
	emptyComment := ""
	ops, err := GetAlterTableOps(
		types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "qaz", Type: "Int32", IsPrimaryKey: true},
			{Name: "qux", Type: "String", IsPrimaryKey: false},
		}),
		types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "qaz", Type: "Int32", IsPrimaryKey: true},
			{Name: "qux", Type: "Int64", IsPrimaryKey: false},
		}))
	assert.NoError(t, err)
	assert.Equal(t, ops, []*types.AlterTableOp{{Op: types.AlterTableModify, Column: "qux", Type: &int64Type, Comment: &emptyComment}})
}

func TestGetAlterTableOpsAdd(t *testing.T) {
	int64Type := "Int64"
	emptyComment := ""
	ops, err := GetAlterTableOps(
		types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "qaz", Type: "Int32", IsPrimaryKey: true},
			{Name: "qux", Type: "String", IsPrimaryKey: false},
		}),
		types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "qaz", Type: "Int32", IsPrimaryKey: true},
			{Name: "qux", Type: "String", IsPrimaryKey: false},
			{Name: "zaq", Type: "Int64", IsPrimaryKey: false},
		}))
	assert.NoError(t, err)
	assert.Equal(t, ops, []*types.AlterTableOp{{Op: types.AlterTableAdd, Column: "zaq", Type: &int64Type, Comment: &emptyComment}})

}

func TestGetAlterTableOpsDrop(t *testing.T) {
	ops, err := GetAlterTableOps(
		types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "qaz", Type: "Int32", IsPrimaryKey: true},
			{Name: "qux", Type: "String", IsPrimaryKey: false},
		}),
		types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "qaz", Type: "Int32", IsPrimaryKey: true},
		}))
	assert.NoError(t, err)
	assert.Equal(t, ops, []*types.AlterTableOp{{Op: types.AlterTableDrop, Column: "qux", Type: nil, Comment: nil}})
}

func TestGetAlterTableOpsAllCombined(t *testing.T) {
	int64Type := "Int64"
	int16Type := "Int16"
	emptyComment := ""
	ops, err := GetAlterTableOps(
		types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "qaz", Type: "Int32", IsPrimaryKey: false},
			{Name: "qux", Type: "String", IsPrimaryKey: false},
		}),
		types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "qux", Type: "Int64", IsPrimaryKey: false},
			{Name: "zaq", Type: "Int16", IsPrimaryKey: false},
		}))
	assert.NoError(t, err)
	assert.Equal(t, ops, []*types.AlterTableOp{
		{Op: types.AlterTableModify, Column: "qux", Type: &int64Type, Comment: &emptyComment},
		{Op: types.AlterTableAdd, Column: "zaq", Type: &int16Type, Comment: &emptyComment},
		{Op: types.AlterTableDrop, Column: "qaz", Type: nil, Comment: nil},
	})
}

func TestGetAlterTableOpsEqualTables(t *testing.T) {
	// Make sure they all are different pointers
	ops, err := GetAlterTableOps(
		types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "qaz", Type: "Int32", IsPrimaryKey: true},
			{Name: "qux", Type: "String", IsPrimaryKey: false},
		}),
		types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "qaz", Type: "Int32", IsPrimaryKey: true},
			{Name: "qux", Type: "String", IsPrimaryKey: false},
		}))
	assert.NoError(t, err)
	assert.Equal(t, ops, []*types.AlterTableOp{})
}

func TestGetAlterTableOpsWithComments(t *testing.T) {
	strType := "String"
	emptyComment := ""
	xmlComment := "XML"
	binaryComment := "BINARY"
	ops, err := GetAlterTableOps(
		types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "s1", Type: strType, IsPrimaryKey: false, Comment: emptyComment},
			{Name: "s2", Type: strType, IsPrimaryKey: false, Comment: emptyComment},
			{Name: "s3", Type: strType, IsPrimaryKey: false, Comment: xmlComment},
			{Name: "s4", Type: strType, IsPrimaryKey: false, Comment: binaryComment},
			{Name: "s5", Type: strType, IsPrimaryKey: false, Comment: emptyComment},
		}),
		types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "s1", Type: strType, IsPrimaryKey: false, Comment: binaryComment},
			{Name: "s2", Type: strType, IsPrimaryKey: false, Comment: xmlComment},
			{Name: "s3", Type: strType, IsPrimaryKey: false, Comment: emptyComment},
			{Name: "s4", Type: strType, IsPrimaryKey: false, Comment: emptyComment},
			{Name: "s10", Type: strType, IsPrimaryKey: false, Comment: emptyComment},
			{Name: "s11", Type: strType, IsPrimaryKey: false, Comment: binaryComment},
			{Name: "s12", Type: strType, IsPrimaryKey: false, Comment: xmlComment},
		}))
	assert.NoError(t, err)
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

func TestGetAlterTableOpsChangePrimaryKeyType(t *testing.T) {
	ops, err := GetAlterTableOps(
		types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "qaz", Type: "Int32", IsPrimaryKey: true},
			{Name: "qux", Type: "String", IsPrimaryKey: false},
		}),
		types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "qaz", Type: "Int64", IsPrimaryKey: true},
			{Name: "qux", Type: "Int64", IsPrimaryKey: false},
		}))
	assert.ErrorContains(t, err, "primary key columns types cannot be changed")
	assert.Equal(t, ops, []*types.AlterTableOp(nil))
}

func TestGetAlterTableOpsChangeColumnToPrimaryKey(t *testing.T) {
	ops, err := GetAlterTableOps(
		types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "qaz", Type: "Int32", IsPrimaryKey: false},
			{Name: "qux", Type: "String", IsPrimaryKey: false},
		}),
		types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "qaz", Type: "Int32", IsPrimaryKey: false},
			{Name: "qux", Type: "String", IsPrimaryKey: true},
		}))
	assert.ErrorContains(t, err, "primary key columns cannot be modified")
	assert.Equal(t, ops, []*types.AlterTableOp(nil))
}

func TestGetAlterTableOpsChangeColumnFromPrimaryKey(t *testing.T) {
	ops, err := GetAlterTableOps(
		types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "qaz", Type: "Int32", IsPrimaryKey: false},
			{Name: "qux", Type: "String", IsPrimaryKey: true},
		}),
		types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "qaz", Type: "Int32", IsPrimaryKey: false},
			{Name: "qux", Type: "Int64", IsPrimaryKey: false},
		}))
	assert.ErrorContains(t, err, "primary key columns cannot be modified")
	assert.Equal(t, ops, []*types.AlterTableOp(nil))
}

func TestGetAlterTableOpsDropPrimaryKey(t *testing.T) {
	ops, err := GetAlterTableOps(
		types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "qaz", Type: "Int32", IsPrimaryKey: true},
			{Name: "qux", Type: "String", IsPrimaryKey: false},
		}),
		types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "qux", Type: "Int64", IsPrimaryKey: false},
		}))
	assert.ErrorContains(t, err, "primary key columns cannot be dropped")
	assert.Equal(t, ops, []*types.AlterTableOp(nil))
}

func TestGetAlterTableOpsAddPrimaryKey(t *testing.T) {
	ops, err := GetAlterTableOps(
		types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "qaz", Type: "Int32", IsPrimaryKey: true},
		}),
		types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "qaz", Type: "Int32", IsPrimaryKey: true},
			{Name: "qux", Type: "Int64", IsPrimaryKey: true},
		}))
	assert.ErrorContains(t, err, "primary key columns cannot be added")
	assert.Equal(t, ops, []*types.AlterTableOp(nil))
}
