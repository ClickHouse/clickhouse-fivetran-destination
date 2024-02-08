package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetFullTableName(t *testing.T) {
	fullName, err := GetFullTableName("foo", "bar")
	assert.NoError(t, err)
	assert.Equal(t, "`foo`.`bar`", fullName)

	fullName, err = GetFullTableName("", "bar")
	assert.NoError(t, err)
	assert.Equal(t, "`bar`", fullName)

	_, err = GetFullTableName("foo", "")
	assert.ErrorContains(t, err, "table name is empty")
}

func TestGetAlterTableStatement(t *testing.T) {
	intType := "Int32"
	strType := "String"
	statement, err := GetAlterTableStatement("foo", "bar", []*AlterTableOp{
		{Add, "qaz", &intType},
	})
	assert.NoError(t, err)
	assert.Equal(t, "ALTER TABLE `foo`.`bar` ADD COLUMN qaz Int32", statement)

	statement, err = GetAlterTableStatement("foo", "bar", []*AlterTableOp{
		{Drop, "qaz", nil},
	})
	assert.NoError(t, err)
	assert.Equal(t, "ALTER TABLE `foo`.`bar` DROP COLUMN qaz", statement)

	statement, err = GetAlterTableStatement("foo", "bar", []*AlterTableOp{
		{Modify, "qaz", &strType},
	})
	assert.NoError(t, err)
	assert.Equal(t, "ALTER TABLE `foo`.`bar` MODIFY COLUMN qaz String", statement)

	statement, err = GetAlterTableStatement("", "bar", []*AlterTableOp{
		{Add, "qaz", &strType},
		{Drop, "qux", nil},
		{Modify, "zaq", &intType},
	})
	assert.NoError(t, err)
	assert.Equal(t, "ALTER TABLE `bar` ADD COLUMN qaz String, DROP COLUMN qux, MODIFY COLUMN zaq Int32", statement)

	_, err = GetAlterTableStatement("foo", "bar", []*AlterTableOp{})
	assert.ErrorContains(t, err, "no statements to execute for altering table `foo`.`bar`")

	_, err = GetAlterTableStatement("foo", "", []*AlterTableOp{{Modify, "qaz", &strType}})
	assert.ErrorContains(t, err, "table name is empty")
}

func TestGetCreateTableStatement(t *testing.T) {
	statement, err := GetCreateTableStatement("foo", "bar", &TableDescription{
		Columns: []*ColumnDefinition{
			{Name: "qaz", Type: "Int32"},
			{Name: "qux", Type: "String", IsPrimaryKey: true},
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, "CREATE TABLE `foo`.`bar` (qaz Int32, qux String) ENGINE = ReplacingMergeTree(_fivetran_synced) ORDER BY (qux)", statement)

	statement, err = GetCreateTableStatement("", "bar", &TableDescription{
		Columns: []*ColumnDefinition{
			{Name: "qaz", Type: "Int32", IsPrimaryKey: true},
			{Name: "qux", Type: "String", IsPrimaryKey: true},
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, "CREATE TABLE `bar` (qaz Int32, qux String) ENGINE = ReplacingMergeTree(_fivetran_synced) ORDER BY (qaz, qux)", statement)

	_, err = GetCreateTableStatement("foo", "", nil)
	assert.ErrorContains(t, err, "table name is empty")

	_, err = GetCreateTableStatement("foo", "bar", nil)
	assert.ErrorContains(t, err, "no columns to create table `foo`.`bar`")

	_, err = GetCreateTableStatement("foo", "bar", &TableDescription{})
	assert.ErrorContains(t, err, "no columns to create table `foo`.`bar`")
}

func TestGetTruncateTableStatement(t *testing.T) {
	statement, err := GetTruncateTableStatement("foo", "bar")
	assert.NoError(t, err)
	assert.Equal(t, "TRUNCATE TABLE `foo`.`bar`", statement)

	statement, err = GetTruncateTableStatement("", "bar")
	assert.NoError(t, err)
	assert.Equal(t, "TRUNCATE TABLE `bar`", statement)

	_, err = GetTruncateTableStatement("foo", "")
	assert.ErrorContains(t, err, "table name is empty")
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
	assert.Equal(t, "SELECT name, type, is_in_primary_key FROM system.columns WHERE database = 'foo' AND table = 'bar'", query)

	_, err = GetDescribeTableQuery("", "bar")
	assert.ErrorContains(t, err, "schema name is empty")

	_, err = GetDescribeTableQuery("foo", "")
	assert.ErrorContains(t, err, "table name is empty")
}

func TestGetWithDefault(t *testing.T) {
	configuration := map[string]string{
		"key": "value",
	}
	assert.Equal(t, "value", GetWithDefault(configuration, "key", "default"))
	assert.Equal(t, "default", GetWithDefault(configuration, "missing", "default"))
	assert.Equal(t, "", GetWithDefault(configuration, "missing", ""))
}
