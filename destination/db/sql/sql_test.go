package sql

import (
	"testing"
	"time"

	"fivetran.com/fivetran_sdk/destination/common/types"
	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/stretchr/testify/assert"
)

func TestGetQualifiedTableName(t *testing.T) {
	fullName, err := GetQualifiedTableName("foo", "bar")
	assert.NoError(t, err)
	assert.Equal(t, "`foo`.`bar`", fullName)

	fullName, err = GetQualifiedTableName("", "bar")
	assert.NoError(t, err)
	assert.Equal(t, "`bar`", fullName)

	_, err = GetQualifiedTableName("foo", "")
	assert.ErrorContains(t, err, "table name is empty")
}

func TestGetAlterTableStatement(t *testing.T) {
	intType := "Int32"
	strType := "String"
	comment := "foobar"
	emptyComment := ""
	statement, err := GetAlterTableStatement("foo", "bar", []*types.AlterTableOp{
		{Op: types.AlterTableAdd, Column: "qaz", Type: &intType},
	})
	assert.NoError(t, err)
	assert.Equal(t, "ALTER TABLE `foo`.`bar` ADD COLUMN `qaz` Int32", statement)

	statement, err = GetAlterTableStatement("foo", "bar", []*types.AlterTableOp{
		{Op: types.AlterTableAdd, Column: "qaz", Type: &intType, Comment: &emptyComment},
	})
	assert.NoError(t, err)
	assert.Equal(t, "ALTER TABLE `foo`.`bar` ADD COLUMN `qaz` Int32 COMMENT ''", statement)

	statement, err = GetAlterTableStatement("foo", "bar", []*types.AlterTableOp{
		{Op: types.AlterTableAdd, Column: "qaz", Type: &intType, Comment: &comment},
	})
	assert.NoError(t, err)
	assert.Equal(t, "ALTER TABLE `foo`.`bar` ADD COLUMN `qaz` Int32 COMMENT 'foobar'", statement)

	statement, err = GetAlterTableStatement("foo", "bar", []*types.AlterTableOp{
		{Op: types.AlterTableDrop, Column: "qaz"},
	})
	assert.NoError(t, err)
	assert.Equal(t, "ALTER TABLE `foo`.`bar` DROP COLUMN `qaz`", statement)

	// Type and Comment are ignored with AlterTableDrop
	statement, err = GetAlterTableStatement("foo", "bar", []*types.AlterTableOp{
		{Op: types.AlterTableDrop, Column: "qaz", Type: &strType, Comment: &comment},
	})
	assert.NoError(t, err)
	assert.Equal(t, "ALTER TABLE `foo`.`bar` DROP COLUMN `qaz`", statement)

	statement, err = GetAlterTableStatement("foo", "bar", []*types.AlterTableOp{
		{Op: types.AlterTableModify, Column: "qaz", Type: &strType},
	})
	assert.NoError(t, err)
	assert.Equal(t, "ALTER TABLE `foo`.`bar` MODIFY COLUMN `qaz` String", statement)

	statement, err = GetAlterTableStatement("foo", "bar", []*types.AlterTableOp{
		{Op: types.AlterTableModify, Column: "qaz", Type: &strType, Comment: &emptyComment},
	})
	assert.NoError(t, err)
	assert.Equal(t, "ALTER TABLE `foo`.`bar` MODIFY COLUMN `qaz` String COMMENT ''", statement)

	statement, err = GetAlterTableStatement("foo", "bar", []*types.AlterTableOp{
		{Op: types.AlterTableModify, Column: "qaz", Type: &strType, Comment: &comment},
	})
	assert.NoError(t, err)
	assert.Equal(t, "ALTER TABLE `foo`.`bar` MODIFY COLUMN `qaz` String COMMENT 'foobar'", statement)

	statement, err = GetAlterTableStatement("", "bar", []*types.AlterTableOp{
		{Op: types.AlterTableAdd, Column: "qaz", Type: &strType, Comment: &comment},
		{Op: types.AlterTableDrop, Column: "qux"},
		{Op: types.AlterTableModify, Column: "zaq", Type: &intType, Comment: &emptyComment},
		{Op: types.AlterTableModify, Column: "qwe", Type: &strType},
	})
	assert.NoError(t, err)
	assert.Equal(t,
		"ALTER TABLE `bar` ADD COLUMN `qaz` String COMMENT 'foobar', DROP COLUMN `qux`, MODIFY COLUMN `zaq` Int32 COMMENT '', MODIFY COLUMN `qwe` String",
		statement)

	_, err = GetAlterTableStatement("foo", "bar", []*types.AlterTableOp{})
	assert.ErrorContains(t, err, "no statements to execute for altering table `foo`.`bar`")

	_, err = GetAlterTableStatement("foo", "", []*types.AlterTableOp{{Op: types.AlterTableModify, Column: "qaz", Type: &strType, Comment: &comment}})
	assert.ErrorContains(t, err, "table name is empty")

	_, err = GetAlterTableStatement("foo", "bar", []*types.AlterTableOp{{Op: types.AlterTableAdd, Column: "qaz"}})
	assert.ErrorContains(t, err, "type for column qaz is not specified")

	_, err = GetAlterTableStatement("foo", "bar", []*types.AlterTableOp{{Op: types.AlterTableModify, Column: "qaz"}})
	assert.ErrorContains(t, err, "type for column qaz is not specified")
}

func TestGetCreateTableStatement(t *testing.T) {
	statement, err := GetCreateTableStatement("foo", "bar",
		types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "qaz", Type: "Int32"},
			{Name: "qux", Type: "String", IsPrimaryKey: true},
			{Name: "_fivetran_synced", Type: "DateTime64(9, 'UTC')"},
			{Name: "_fivetran_deleted", Type: "Boolean"},
		}))
	assert.NoError(t, err)
	assert.Equal(t, "CREATE TABLE `foo`.`bar` (`qaz` Int32, `qux` String, `_fivetran_synced` DateTime64(9, 'UTC'), `_fivetran_deleted` Boolean) ENGINE = ReplacingMergeTree(`_fivetran_synced`) ORDER BY (`qux`)", statement)

	statement, err = GetCreateTableStatement("", "bar",
		types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "qaz", Type: "Int32", IsPrimaryKey: true},
			{Name: "qux", Type: "String", IsPrimaryKey: true},
			{Name: "_fivetran_synced", Type: "DateTime64(9, 'UTC')"},
			{Name: "_fivetran_deleted", Type: "Boolean"},
		}))
	assert.NoError(t, err)
	assert.Equal(t, "CREATE TABLE `bar` (`qaz` Int32, `qux` String, `_fivetran_synced` DateTime64(9, 'UTC'), `_fivetran_deleted` Boolean) ENGINE = ReplacingMergeTree(`_fivetran_synced`) ORDER BY (`qaz`, `qux`)", statement)

	statement, err = GetCreateTableStatement("", "bar",
		types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "i", Type: "Int32", IsPrimaryKey: true},
			{Name: "x", Type: "String", IsPrimaryKey: false, Comment: "XML"},
			{Name: "bin", Type: "String", IsPrimaryKey: false, Comment: "BINARY"},
			{Name: "_fivetran_synced", Type: "DateTime64(9, 'UTC')"},
			{Name: "_fivetran_deleted", Type: "Boolean"},
		}))
	assert.NoError(t, err)
	assert.Equal(t, "CREATE TABLE `bar` (`i` Int32, `x` String COMMENT 'XML', `bin` String COMMENT 'BINARY', `_fivetran_synced` DateTime64(9, 'UTC'), `_fivetran_deleted` Boolean) ENGINE = ReplacingMergeTree(`_fivetran_synced`) ORDER BY (`i`)", statement)

	// works without _fivetran_deleted column
	statement, err = GetCreateTableStatement("", "bar",
		types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "i", Type: "Int32", IsPrimaryKey: true},
			{Name: "x", Type: "String", IsPrimaryKey: false},
			{Name: "_fivetran_synced", Type: "DateTime64(9, 'UTC')"},
		}))
	assert.NoError(t, err)
	assert.Equal(t, "CREATE TABLE `bar` (`i` Int32, `x` String, `_fivetran_synced` DateTime64(9, 'UTC')) ENGINE = ReplacingMergeTree(`_fivetran_synced`) ORDER BY (`i`)", statement)

	_, err = GetCreateTableStatement("foo", "", nil)
	assert.ErrorContains(t, err, "table name is empty")

	_, err = GetCreateTableStatement("foo", "bar", nil)
	assert.ErrorContains(t, err, "no columns to create table `foo`.`bar`")

	_, err = GetCreateTableStatement("foo", "bar", &types.TableDescription{})
	assert.ErrorContains(t, err, "no columns to create table `foo`.`bar`")

	_, err = GetCreateTableStatement("foo", "bar",
		types.MakeTableDescription([]*types.ColumnDefinition{{Name: "qaz", Type: "Int32"}}))
	assert.ErrorContains(t, err, "no primary keys for table `foo`.`bar`")

	_, err = GetCreateTableStatement("foo", "bar",
		types.MakeTableDescription([]*types.ColumnDefinition{{Name: "qaz", Type: "Int32", IsPrimaryKey: true}}))
	assert.ErrorContains(t, err, "no _fivetran_synced column")
}

func TestGetTruncateTableStatement(t *testing.T) {
	emptyStr := ""
	syncedColumn := "_fivetran_synced"
	softDeletedColumn := "_fivetran_deleted"
	truncateBefore := time.Unix(1646455512, 123456789)

	expectedHard := "ALTER TABLE `foo`.`bar` DELETE WHERE `_fivetran_synced` < '1646455512123456789'"
	expectedSoft := "ALTER TABLE `foo`.`bar` UPDATE `_fivetran_deleted` = 1 WHERE `_fivetran_synced` < '1646455512123456789'"

	statement, err := GetTruncateTableStatement("foo", "bar", syncedColumn, truncateBefore, nil)
	assert.NoError(t, err)
	assert.Equal(t, expectedHard, statement)

	statement, err = GetTruncateTableStatement("foo", "bar", syncedColumn, truncateBefore, &emptyStr)
	assert.NoError(t, err)
	assert.Equal(t, expectedHard, statement)

	statement, err = GetTruncateTableStatement("foo", "bar", syncedColumn, truncateBefore, &softDeletedColumn)
	assert.NoError(t, err)
	assert.Equal(t, expectedSoft, statement)

	_, err = GetTruncateTableStatement("foo", "", syncedColumn, truncateBefore, nil)
	assert.ErrorContains(t, err, "table name is empty")

	_, err = GetTruncateTableStatement("foo", "bar", "", truncateBefore, nil)
	assert.ErrorContains(t, err, "synced column name is empty")

	_, err = GetTruncateTableStatement("foo", "bar", syncedColumn, time.Unix(0, 0), nil)
	assert.ErrorContains(t, err, "truncate before time is zero")
}

func TestGetColumnTypesQuery(t *testing.T) {
	query, err := GetColumnTypesQuery("foo", "bar")
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM `foo`.`bar` WHERE false", query)

	_, err = GetColumnTypesQuery("foo", "")
	assert.ErrorContains(t, err, "table name is empty")
}

func TestGetDescribeTableQuery(t *testing.T) {
	query, err := GetDescribeTableQuery("foo", "bar")
	assert.NoError(t, err)
	assert.Equal(t, "SELECT name, type, comment, is_in_primary_key, numeric_precision, numeric_scale FROM system.columns WHERE database = 'foo' AND table = 'bar'", query)

	_, err = GetDescribeTableQuery("", "bar")
	assert.ErrorContains(t, err, "schema name is empty")

	_, err = GetDescribeTableQuery("foo", "")
	assert.ErrorContains(t, err, "table name is empty")
}

func TestGetSelectByPrimaryKeysQueryValidation(t *testing.T) {
	pkCols := []*types.PrimaryKeyColumn{{Index: 0, Name: "id", Type: pb.DataType_LONG}}
	_, err := GetSelectByPrimaryKeysQuery(nil, "", nil)
	assert.ErrorContains(t, err, "expected non-empty list of primary keys columns")
	_, err = GetSelectByPrimaryKeysQuery(nil, "", pkCols)
	assert.ErrorContains(t, err, "table name is empty")
	_, err = GetSelectByPrimaryKeysQuery([][]string{}, "test_table", pkCols)
	assert.ErrorContains(t, err, "expected non-empty CSV slice")
	_, err = GetSelectByPrimaryKeysQuery([][]string{{"foo"}}, "test_table",
		[]*types.PrimaryKeyColumn{{Index: 5, Name: "id", Type: pb.DataType_LONG}})
	assert.ErrorContains(t, err, "can't find matching value for primary key with index 5")
}

func TestGetSelectByPrimaryKeysQuery(t *testing.T) {
	fullTableName := "`foo`.`bar`"
	batch := [][]string{
		{"42", "foo", "2022-03-05T04:45:12.123456789Z", "false"},
		{"43", "bar", "2022-03-05T04:45:12.123456789Z", "false"},
	}
	pkCols := []*types.PrimaryKeyColumn{
		{Index: 0, Name: "id", Type: pb.DataType_LONG},
	}
	query, err := GetSelectByPrimaryKeysQuery(batch, fullTableName, pkCols)
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM `foo`.`bar` FINAL WHERE (`id`) IN ((42), (43)) ORDER BY (`id`) LIMIT 2", query)

	batch = [][]string{
		{"42", "foo", "2022-03-05T04:45:12.123456789Z", "false"},
		{"43", "bar", "2022-03-05T04:45:12.123456789Z", "false"},
		{"44", "qaz", "2022-03-05T04:45:12.123456789Z", "false"},
		{"45", "qux", "2022-03-05T04:45:12.123456789Z", "false"},
	}
	pkCols = []*types.PrimaryKeyColumn{
		{Index: 0, Name: "id", Type: pb.DataType_LONG},
		{Index: 1, Name: "name", Type: pb.DataType_STRING},
	}
	query, err = GetSelectByPrimaryKeysQuery(batch, fullTableName, pkCols)
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM `foo`.`bar` FINAL WHERE (`id`, `name`) IN ((42, 'foo'), (43, 'bar'), (44, 'qaz'), (45, 'qux')) ORDER BY (`id`, `name`) LIMIT 4", query)
}
