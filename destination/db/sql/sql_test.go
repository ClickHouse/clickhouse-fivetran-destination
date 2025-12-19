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
	assert.Equal(t, QualifiedTableName("`foo`.`bar`"), fullName)

	_, err = GetQualifiedTableName("", "bar")
	assert.ErrorContains(t, err, "schema name for table bar is empty")

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

	statement, err = GetAlterTableStatement("foo", "bar", []*types.AlterTableOp{
		{Op: types.AlterTableAdd, Column: "qaz", Type: &strType, Comment: &comment},
		{Op: types.AlterTableDrop, Column: "qux"},
		{Op: types.AlterTableModify, Column: "zaq", Type: &intType, Comment: &emptyComment},
		{Op: types.AlterTableModify, Column: "qwe", Type: &strType},
	})
	assert.NoError(t, err)
	assert.Equal(t,
		"ALTER TABLE `foo`.`bar` ADD COLUMN `qaz` String COMMENT 'foobar', DROP COLUMN `qux`, MODIFY COLUMN `zaq` Int32 COMMENT '', MODIFY COLUMN `qwe` String",
		statement)

	_, err = GetAlterTableStatement("foo", "bar", []*types.AlterTableOp{})
	assert.ErrorContains(t, err, "no statements to execute for altering table `foo`.`bar`")

	_, err = GetAlterTableStatement("foo", "", []*types.AlterTableOp{{Op: types.AlterTableModify, Column: "qaz", Type: &strType, Comment: &comment}})
	assert.ErrorContains(t, err, "table name is empty")

	_, err = GetAlterTableStatement("", "bar", []*types.AlterTableOp{{Op: types.AlterTableModify, Column: "qaz", Type: &strType, Comment: &comment}})
	assert.ErrorContains(t, err, "schema name for table bar is empty")

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

	statement, err = GetCreateTableStatement("foo", "bar",
		types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "qaz", Type: "Int32", IsPrimaryKey: true},
			{Name: "qux", Type: "String", IsPrimaryKey: true},
			{Name: "_fivetran_synced", Type: "DateTime64(9, 'UTC')"},
			{Name: "_fivetran_deleted", Type: "Boolean"},
		}))
	assert.NoError(t, err)
	assert.Equal(t, "CREATE TABLE `foo`.`bar` (`qaz` Int32, `qux` String, `_fivetran_synced` DateTime64(9, 'UTC'), `_fivetran_deleted` Boolean) ENGINE = ReplacingMergeTree(`_fivetran_synced`) ORDER BY (`qaz`, `qux`)", statement)

	statement, err = GetCreateTableStatement("foo", "bar",
		types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "i", Type: "Int32", IsPrimaryKey: true},
			{Name: "x", Type: "String", IsPrimaryKey: false, Comment: "XML"},
			{Name: "bin", Type: "String", IsPrimaryKey: false, Comment: "BINARY"},
			{Name: "_fivetran_synced", Type: "DateTime64(9, 'UTC')"},
			{Name: "_fivetran_deleted", Type: "Boolean"},
		}))
	assert.NoError(t, err)
	assert.Equal(t, "CREATE TABLE `foo`.`bar` (`i` Int32, `x` String COMMENT 'XML', `bin` String COMMENT 'BINARY', `_fivetran_synced` DateTime64(9, 'UTC'), `_fivetran_deleted` Boolean) ENGINE = ReplacingMergeTree(`_fivetran_synced`) ORDER BY (`i`)", statement)

	// works without _fivetran_deleted column
	statement, err = GetCreateTableStatement("foo", "bar",
		types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "i", Type: "Int32", IsPrimaryKey: true},
			{Name: "x", Type: "String", IsPrimaryKey: false},
			{Name: "_fivetran_synced", Type: "DateTime64(9, 'UTC')"},
		}))
	assert.NoError(t, err)
	assert.Equal(t, "CREATE TABLE `foo`.`bar` (`i` Int32, `x` String, `_fivetran_synced` DateTime64(9, 'UTC')) ENGINE = ReplacingMergeTree(`_fivetran_synced`) ORDER BY (`i`)", statement)

	_, err = GetCreateTableStatement("foo", "", nil)
	assert.ErrorContains(t, err, "table name is empty")

	_, err = GetCreateTableStatement("", "bar", nil)
	assert.ErrorContains(t, err, "schema name for table bar is empty")

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

	expectedHard := "ALTER TABLE `foo`.`bar` DELETE WHERE toUnixTimestamp64Milli(`_fivetran_synced`) <= '1646455512123'"
	expectedSoft := "ALTER TABLE `foo`.`bar` UPDATE `_fivetran_deleted` = 1 WHERE toUnixTimestamp64Milli(`_fivetran_synced`) <= '1646455512123'"

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

	_, err = GetTruncateTableStatement("", "bar", syncedColumn, truncateBefore, nil)
	assert.ErrorContains(t, err, "schema name for table bar is empty")

	_, err = GetTruncateTableStatement("foo", "bar", "", truncateBefore, nil)
	assert.ErrorContains(t, err, "synced column name is empty")

	_, err = GetTruncateTableStatement("foo", "bar", syncedColumn, time.Time{}, nil)
	assert.ErrorContains(t, err, "truncate before time is zero")

	truncateBeforeDate := time.Date(2000, 1, 15, 14, 35, 0, 0, time.UTC)
	expectedStmtBeforeDate := "ALTER TABLE `foo`.`bar` DELETE WHERE toUnixTimestamp64Milli(`_fivetran_synced`) <= '947946900000'"

	statement, err = GetTruncateTableStatement("foo", "bar", syncedColumn, truncateBeforeDate, nil)
	assert.NoError(t, err)
	assert.Equal(t, expectedStmtBeforeDate, statement)
}

func TestGetColumnTypesQuery(t *testing.T) {
	query, err := GetColumnTypesQuery("foo", "bar")
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM `foo`.`bar` WHERE false", query)

	_, err = GetColumnTypesQuery("foo", "")
	assert.ErrorContains(t, err, "table name is empty")

	_, err = GetColumnTypesQuery("", "bar")
	assert.ErrorContains(t, err, "schema name for table bar is empty")
}

func TestGetDescribeTableQuery(t *testing.T) {
	query, err := GetDescribeTableQuery("foo", "bar")
	assert.NoError(t, err)
	assert.Equal(t, "SELECT name, type, comment, is_in_primary_key, numeric_precision, numeric_scale FROM system.columns WHERE database = 'foo' AND table = 'bar'", query)

	_, err = GetDescribeTableQuery("", "bar")
	assert.ErrorContains(t, err, "schema name for table bar is empty")

	_, err = GetDescribeTableQuery("foo", "")
	assert.ErrorContains(t, err, "table name is empty")
}

func TestGetSelectByPrimaryKeysQueryValidation(t *testing.T) {
	fullTableName := QualifiedTableName("`foo`.`bar`")
	csvCols := &types.CSVColumns{
		All:         []*types.CSVColumn{{Index: 0, Name: "id", Type: pb.DataType_LONG}},
		PrimaryKeys: nil,
	}
	batch := [][]string{{"42", "foo", "2022-03-05T04:45:12.123456789Z"}}

	_, err := GetSelectByPrimaryKeysQuery(batch, csvCols, "", false)
	assert.ErrorContains(t, err, "table name is empty")

	_, err = GetSelectByPrimaryKeysQuery([][]string{}, csvCols, fullTableName, false)
	assert.ErrorContains(t, err, "expected non-empty CSV slice")
	_, err = GetSelectByPrimaryKeysQuery(nil, csvCols, fullTableName, false)
	assert.ErrorContains(t, err, "expected non-empty CSV slice")

	_, err = GetSelectByPrimaryKeysQuery(batch, nil, fullTableName, false)
	assert.ErrorContains(t, err, "expected non-empty primary keys")
	_, err = GetSelectByPrimaryKeysQuery(batch, csvCols, fullTableName, false)
	assert.ErrorContains(t, err, "expected non-empty primary keys")

	withInvalidCol := []*types.CSVColumn{{Index: 5, Name: "id", Type: pb.DataType_LONG, IsPrimaryKey: false}}
	invalidIndexCSVCols := &types.CSVColumns{
		All:         withInvalidCol,
		PrimaryKeys: withInvalidCol,
	}
	_, err = GetSelectByPrimaryKeysQuery([][]string{{"foo"}}, invalidIndexCSVCols, fullTableName, false)
	assert.ErrorContains(t, err, "can't find matching value for primary key with index 5")
}

func TestGetSelectByPrimaryKeysQuery(t *testing.T) {
	fullTableName := QualifiedTableName("`foo`.`bar`")
	batch := [][]string{
		{"42", "foo", "2022-03-05T04:45:12.123456789Z"},
		{"43", "bar", "2023-04-06T12:30:00.234567890Z"},
	}
	statement, err := GetSelectByPrimaryKeysQuery(batch, &types.CSVColumns{
		All: []*types.CSVColumn{
			{Index: 0, Name: "id", Type: pb.DataType_LONG, IsPrimaryKey: true},
			{Index: 1, Name: "name", Type: pb.DataType_STRING},
			{Index: 2, Name: "ts", Type: pb.DataType_UTC_DATETIME}},
		PrimaryKeys: []*types.CSVColumn{
			{Index: 0, Name: "id", Type: pb.DataType_LONG, IsPrimaryKey: true}},
	}, fullTableName, false)
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM `foo`.`bar` FINAL WHERE (`id`) IN ((42), (43)) ORDER BY (`id`) LIMIT 2", statement)

	statement, err = GetSelectByPrimaryKeysQuery(batch, &types.CSVColumns{
		All: []*types.CSVColumn{
			{Index: 0, Name: "id", Type: pb.DataType_LONG, IsPrimaryKey: true},
			{Index: 1, Name: "name", Type: pb.DataType_STRING, IsPrimaryKey: true},
			{Index: 2, Name: "ts", Type: pb.DataType_UTC_DATETIME}},
		PrimaryKeys: []*types.CSVColumn{
			{Index: 0, Name: "id", Type: pb.DataType_LONG, IsPrimaryKey: true},
			{Index: 1, Name: "name", Type: pb.DataType_STRING, IsPrimaryKey: true}},
	}, fullTableName, false)
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM `foo`.`bar` FINAL WHERE (`id`, `name`) IN ((42, 'foo'), (43, 'bar')) ORDER BY (`id`, `name`) LIMIT 2", statement)

	statement, err = GetSelectByPrimaryKeysQuery(batch, &types.CSVColumns{
		All: []*types.CSVColumn{
			{Index: 0, Name: "id", Type: pb.DataType_LONG},
			{Index: 1, Name: "name", Type: pb.DataType_STRING},
			{Index: 2, Name: "ts", Type: pb.DataType_UTC_DATETIME, IsPrimaryKey: true}},
		PrimaryKeys: []*types.CSVColumn{
			{Index: 2, Name: "ts", Type: pb.DataType_UTC_DATETIME, IsPrimaryKey: true}},
	}, fullTableName, false)
	assert.NoError(t, err)
	// DateTime64(9, 'UTC') is converted to nanoseconds.
	assert.Equal(t, "SELECT * FROM `foo`.`bar` FINAL WHERE (`ts`) IN (('1646455512123456789'), ('1680784200234567890')) ORDER BY (`ts`) LIMIT 2", statement)
}

func TestGetCheckDatabaseExistsStatement(t *testing.T) {
	statement, err := GetCheckDatabaseExistsStatement("foo")
	assert.NoError(t, err)
	assert.Equal(t, "SELECT COUNT(*) FROM system.databases WHERE `name` = 'foo'", statement)

	_, err = GetCheckDatabaseExistsStatement("")
	assert.ErrorContains(t, err, "schema name is empty")
}

func TestGetCreateDatabaseStatement(t *testing.T) {
	statement, err := GetCreateDatabaseStatement("foo")
	assert.NoError(t, err)
	assert.Equal(t, "CREATE DATABASE IF NOT EXISTS `foo`", statement)

	_, err = GetCreateDatabaseStatement("")
	assert.ErrorContains(t, err, "schema name is empty")
}

func TestGetSelectFromSystemGrantsQuery(t *testing.T) {
	query, err := GetSelectFromSystemGrantsQuery("foo")
	assert.NoError(t, err)
	assert.Equal(t, "SELECT `access_type`, `database`, `table`, `column` FROM system.grants WHERE `user_name` = 'foo'", query)

	_, err = GetSelectFromSystemGrantsQuery("")
	assert.ErrorContains(t, err, "username is empty")
}

func TestGetHardDeleteStatementValidation(t *testing.T) {
	fullTableName := QualifiedTableName("`foo`.`bar`")
	csvCols := &types.CSVColumns{
		All:         []*types.CSVColumn{{Index: 0, Name: "id", Type: pb.DataType_LONG}},
		PrimaryKeys: nil,
	}
	batch := [][]string{{"42", "foo", "2022-03-05T04:45:12.123456789Z"}}

	_, err := GetHardDeleteStatement(batch, csvCols, "")
	assert.ErrorContains(t, err, "table name is empty")

	_, err = GetHardDeleteStatement(batch, nil, fullTableName)
	assert.ErrorContains(t, err, "expected non-empty primary keys")
	_, err = GetHardDeleteStatement(batch, &types.CSVColumns{}, fullTableName)
	assert.ErrorContains(t, err, "expected non-empty primary keys")

	_, err = GetHardDeleteStatement(nil, csvCols, fullTableName)
	assert.ErrorContains(t, err, "expected non-empty CSV slice")
	_, err = GetHardDeleteStatement([][]string{}, csvCols, fullTableName)
	assert.ErrorContains(t, err, "expected non-empty CSV slice")

	withInvalidCol := []*types.CSVColumn{{Index: 5, Name: "id", Type: pb.DataType_LONG, IsPrimaryKey: true}}
	invalidIndexCSVCols := &types.CSVColumns{
		All:         withInvalidCol,
		PrimaryKeys: withInvalidCol,
	}
	_, err = GetHardDeleteStatement([][]string{{"foo"}}, invalidIndexCSVCols, fullTableName)
	assert.ErrorContains(t, err, "can't find matching value for primary key with index 5")
}

func TestGetHardDeleteStatement(t *testing.T) {
	fullTableName := QualifiedTableName("`foo`.`bar`")
	batch := [][]string{
		{"42", "foo", "2022-03-05T04:45:12.123456789Z"},
		{"43", "bar", "2023-04-06T12:30:00.234567890Z"},
	}
	statement, err := GetHardDeleteStatement(batch, &types.CSVColumns{
		All: []*types.CSVColumn{
			{Index: 0, Name: "id", Type: pb.DataType_LONG, IsPrimaryKey: true},
			{Index: 1, Name: "name", Type: pb.DataType_STRING},
			{Index: 2, Name: "ts", Type: pb.DataType_UTC_DATETIME}},
		PrimaryKeys: []*types.CSVColumn{
			{Index: 0, Name: "id", Type: pb.DataType_LONG, IsPrimaryKey: true}},
	}, fullTableName)
	assert.NoError(t, err)
	assert.Equal(t, "DELETE FROM `foo`.`bar` WHERE (`id`) IN ((42), (43))", statement)

	statement, err = GetHardDeleteStatement(batch, &types.CSVColumns{
		All: []*types.CSVColumn{
			{Index: 0, Name: "id", Type: pb.DataType_LONG, IsPrimaryKey: true},
			{Index: 1, Name: "name", Type: pb.DataType_STRING, IsPrimaryKey: true},
			{Index: 2, Name: "ts", Type: pb.DataType_UTC_DATETIME}},
		PrimaryKeys: []*types.CSVColumn{
			{Index: 0, Name: "id", Type: pb.DataType_LONG, IsPrimaryKey: true},
			{Index: 1, Name: "name", Type: pb.DataType_STRING, IsPrimaryKey: true}},
	}, fullTableName)
	assert.NoError(t, err)
	assert.Equal(t, "DELETE FROM `foo`.`bar` WHERE (`id`, `name`) IN ((42, 'foo'), (43, 'bar'))", statement)

	statement, err = GetHardDeleteStatement(batch, &types.CSVColumns{
		All: []*types.CSVColumn{
			{Index: 0, Name: "id", Type: pb.DataType_LONG},
			{Index: 1, Name: "name", Type: pb.DataType_STRING},
			{Index: 2, Name: "ts", Type: pb.DataType_UTC_DATETIME, IsPrimaryKey: true}},
		PrimaryKeys: []*types.CSVColumn{
			{Index: 2, Name: "ts", Type: pb.DataType_UTC_DATETIME, IsPrimaryKey: true}},
	}, fullTableName)
	assert.NoError(t, err)
	// DateTime64(9, 'UTC') is converted to nanoseconds.
	assert.Equal(t, "DELETE FROM `foo`.`bar` WHERE (`ts`) IN (('1646455512123456789'), ('1680784200234567890'))", statement)
}

func TestGetAllReplicasActiveQuery(t *testing.T) {
	query, err := GetAllReplicasActiveQuery("foo", "bar")
	assert.NoError(t, err)
	assert.Equal(t, "SELECT toBool(mapExists((k, v) -> (v = 0), replica_is_active) = 0) AS all_replicas_active FROM system.replicas WHERE database = 'foo' AND table = 'bar' AND is_readonly != 1 LIMIT 1", query)

	_, err = GetAllReplicasActiveQuery("", "bar")
	assert.ErrorContains(t, err, "schema name for table bar is empty")

	_, err = GetAllReplicasActiveQuery("foo", "")
	assert.ErrorContains(t, err, "table name is empty")
}

func TestGetAllMutationsCompletedQuery(t *testing.T) {
	query, err := GetAllMutationsCompletedQuery("foo", "bar")
	assert.NoError(t, err)
	assert.Equal(t, "SELECT toBool(count(*) = 0) FROM clusterAllReplicas(default, system.mutations) WHERE database = 'foo' AND table = 'bar' AND is_done = 0", query)

	_, err = GetAllMutationsCompletedQuery("", "bar")
	assert.ErrorContains(t, err, "schema name for table bar is empty")

	_, err = GetAllMutationsCompletedQuery("foo", "")
	assert.ErrorContains(t, err, "table name is empty")
}

func TestGetInsertFromSelectStatement(t *testing.T) {
	query, err := GetInsertFromSelectStatement("foo", "bar", "qaz", []string{"a", "b"})
	assert.NoError(t, err)
	assert.Equal(t, "INSERT INTO `foo`.`qaz` (`a`, `b`) SELECT `a`, `b` FROM `foo`.`bar`", query)
}

func TestGetInsertFromSelectStatementSingleColumn(t *testing.T) {
	query, err := GetInsertFromSelectStatement("foo", "bar", "qaz", []string{"a"})
	assert.NoError(t, err)
	assert.Equal(t, "INSERT INTO `foo`.`qaz` (`a`) SELECT `a` FROM `foo`.`bar`", query)
}

func TestGetInsertFromSelectStatementErrors(t *testing.T) {
	_, err := GetInsertFromSelectStatement("foo", "bar", "qaz", []string{})
	assert.ErrorContains(t, err, "column names list is empty")

	_, err = GetInsertFromSelectStatement("foo", "bar", "", []string{"a", "b"})
	assert.ErrorContains(t, err, "new table name is empty")

	_, err = GetInsertFromSelectStatement("foo", "", "qaz", []string{"a", "b"})
	assert.ErrorContains(t, err, "current table name is empty")

	_, err = GetInsertFromSelectStatement("", "bar", "qaz", []string{"a", "b"})
	assert.ErrorContains(t, err, "schema name for tables bar/qaz is empty")
}

func TestGetRenameTablesStatement(t *testing.T) {
	query, err := GetRenameTableStatement("s", "table", "new")
	assert.NoError(t, err)
	assert.Equal(t, "RENAME TABLE `s`.`table` TO `s`.`new`", query)
}

func TestGetRenameTablesStatementErrors(t *testing.T) {
	_, err := GetRenameTableStatement("s", "table", "")
	assert.ErrorContains(t, err, "to table name is empty")

	_, err = GetRenameTableStatement("s", "", "new")
	assert.ErrorContains(t, err, "from table name is empty")

	_, err = GetRenameTableStatement("", "table", "new")
	assert.ErrorContains(t, err, "schema name for tables table/new is empty")
}
