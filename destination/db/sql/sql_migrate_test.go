package sql

import (
	"testing"

	constants "fivetran.com/fivetran_sdk/destination/common/constants"
	"fivetran.com/fivetran_sdk/destination/common/types"
	"github.com/stretchr/testify/assert"
)

func TestGetRenameColumnStatement(t *testing.T) {
	stmt, err := GetRenameColumnStatement("s", "t", "old", "new")
	assert.NoError(t, err)
	assert.Equal(t, "ALTER TABLE `s`.`t` RENAME COLUMN `old` TO `new`", stmt)

	_, err = GetRenameColumnStatement("", "t", "old", "new")
	assert.ErrorContains(t, err, "schema name for table t is empty")
}

func TestGetUpdateColumnValueStatement(t *testing.T) {
	stmt, err := GetUpdateColumnValueStatement("s", "t", "col", "42", false)
	assert.NoError(t, err)
	assert.Equal(t, "ALTER TABLE `s`.`t` UPDATE `col` = '42' WHERE true", stmt)

	stmt, err = GetUpdateColumnValueStatement("s", "t", "col", "", true)
	assert.NoError(t, err)
	assert.Equal(t, "ALTER TABLE `s`.`t` UPDATE `col` = NULL WHERE true", stmt)
}

func TestGetUpdateRowsAtOperationTimestampStatement(t *testing.T) {
	stmt, err := GetUpdateRowsAtOperationTimestampStatement("s", "t", "col", "42", false, "1117314420000000000")
	assert.NoError(t, err)
	assert.Equal(t,
		"ALTER TABLE `s`.`t` UPDATE `col` = '42' WHERE `_fivetran_start` = '1117314420000000000'",
		stmt)

	stmt, err = GetUpdateRowsAtOperationTimestampStatement("s", "t", "col", "", true, "1117314420000000000")
	assert.NoError(t, err)
	assert.Equal(t,
		"ALTER TABLE `s`.`t` UPDATE `col` = NULL WHERE `_fivetran_start` = '1117314420000000000'",
		stmt)
}

func TestGetCopyColumnUpdateStatement(t *testing.T) {
	stmt, err := GetCopyColumnUpdateStatement("s", "t", "new_col", "old_col")
	assert.NoError(t, err)
	assert.Equal(t, "ALTER TABLE `s`.`t` UPDATE `new_col` = `old_col` WHERE true", stmt)
}

func TestGetCreateTableAsStatement(t *testing.T) {
	stmt, err := GetCreateTableAsStatement("s", "from_t", "to_t")
	assert.NoError(t, err)
	assert.Equal(t, "CREATE TABLE `s`.`to_t` AS `s`.`from_t`", stmt)
}

func TestGetCloseActiveRowsStatement(t *testing.T) {
	// 1117314420000000000 - 1000000 = 1117314419999000000 (minus 1ms)
	unfiltered := "ALTER TABLE `s`.`t` UPDATE `_fivetran_active` = FALSE, `_fivetran_end` = '1117314419999000000' WHERE `_fivetran_active` = true AND `_fivetran_start` < '1117314420000000000'"

	stmt, err := GetCloseActiveRowsStatement("s", "t", "1117314420000000000", "")
	assert.NoError(t, err)
	assert.Equal(t, unfiltered, stmt)

	stmt, err = GetCloseActiveRowsStatement("s", "t", "1117314420000000000", "desc")
	assert.NoError(t, err)
	assert.Equal(t,
		unfiltered+" AND `desc` IS NOT NULL",
		stmt)
}

func TestGetInsertNewActiveVersionsStatement(t *testing.T) {
	defaultVal := "Ordered article"
	dataCols := []*types.ColumnDefinition{
		{Name: "id"},
		{Name: "amount"},
		{Name: "article"},
	}
	// add_column_in_history_mode: override column with default value
	stmt, err := GetInsertNewActiveVersionsStatement("s", "t",
		dataCols,
		"article", &defaultVal,
		"1117314420000000000",
	)
	assert.NoError(t, err)
	assert.Equal(t,
		"INSERT INTO `s`.`t` (`id`,`amount`,`article`,`_fivetran_synced`,`_fivetran_start`,`_fivetran_end`,`_fivetran_active`) SELECT `id`,`amount`,'Ordered article',`_fivetran_synced`,'1117314420000000000','9223372036000000000',true FROM `s`.`t` FINAL WHERE `_fivetran_active` AND `_fivetran_start` < '1117314420000000000'",
		stmt)

	// full describe order including history columns: same INSERT as data-only list
	withHistory := append([]*types.ColumnDefinition{}, dataCols...)
	withHistory = append(withHistory,
		&types.ColumnDefinition{Name: constants.FivetranSynced},
		&types.ColumnDefinition{Name: constants.FivetranStart},
		&types.ColumnDefinition{Name: constants.FivetranEnd},
		&types.ColumnDefinition{Name: constants.FivetranActive},
	)
	stmt, err = GetInsertNewActiveVersionsStatement("s", "t",
		withHistory,
		"article", &defaultVal,
		"1117314420000000000",
	)
	assert.NoError(t, err)
	assert.Equal(t,
		"INSERT INTO `s`.`t` (`id`,`amount`,`article`,`_fivetran_synced`,`_fivetran_start`,`_fivetran_end`,`_fivetran_active`) SELECT `id`,`amount`,'Ordered article',`_fivetran_synced`,'1117314420000000000','9223372036000000000',true FROM `s`.`t` FINAL WHERE `_fivetran_active` AND `_fivetran_start` < '1117314420000000000'",
		stmt)

	// drop_column_in_history_mode: override column with NULL, adds IS NOT NULL filter
	stmt, err = GetInsertNewActiveVersionsStatement("s", "t",
		[]*types.ColumnDefinition{
			{Name: "id"},
			{Name: "amount"},
			{Name: "desc"},
		},
		"desc", nil,
		"1117314420000000000",
	)
	assert.NoError(t, err)
	assert.Equal(t,
		"INSERT INTO `s`.`t` (`id`,`amount`,`desc`,`_fivetran_synced`,`_fivetran_start`,`_fivetran_end`,`_fivetran_active`) SELECT `id`,`amount`,NULL,`_fivetran_synced`,'1117314420000000000','9223372036000000000',true FROM `s`.`t` FINAL WHERE `_fivetran_active` AND `_fivetran_start` < '1117314420000000000' AND `desc` IS NOT NULL",
		stmt)

	_, err = GetInsertNewActiveVersionsStatement("s", "t", []*types.ColumnDefinition{}, "article", nil, "1117314420000000000")
	assert.ErrorContains(t, err, "column names list is empty")
}

func TestGetInsertFromSelectWithHistoryColumnsStatement(t *testing.T) {
	stmt, err := GetInsertFromSelectWithHistoryColumnsStatement("s", "from_t", "to_t",
		[]string{"id", "amount"}, "_fivetran_deleted")
	assert.NoError(t, err)
	assert.Equal(t,
		"INSERT INTO `s`.`to_t` (`id`,`amount`,`_fivetran_synced`,`_fivetran_start`,`_fivetran_end`,`_fivetran_active`) SELECT `id`,`amount`,`_fivetran_synced`,`_fivetran_synced`,'9223372036000000000',if(`_fivetran_deleted` = 0, true, false) FROM `s`.`from_t` FINAL",
		stmt)

	stmt, err = GetInsertFromSelectWithHistoryColumnsStatement("s", "from_t", "to_t",
		[]string{"id"}, "")
	assert.NoError(t, err)
	assert.Contains(t, stmt, "true FROM `s`.`from_t` FINAL") // no _fivetran_deleted reference
}

func TestGetInsertFromSelectHistoryToSoftDeleteStatement(t *testing.T) {
	stmt, err := GetInsertFromSelectHistoryToSoftDeleteStatement("s", "from_t", "to_t",
		[]string{"id", "amount"}, []string{"id"}, "_fivetran_deleted", false)
	assert.NoError(t, err)
	assert.Equal(t,
		"INSERT INTO `s`.`to_t` (`id`,`amount`,`_fivetran_synced`,`_fivetran_deleted`) SELECT `id`,`amount`,`_fivetran_synced`,if(`_fivetran_active` = true, false, true) FROM (SELECT `id`,`amount`,`_fivetran_synced`,`_fivetran_active` FROM `s`.`from_t` FINAL ORDER BY `id`,`_fivetran_start` DESC LIMIT 1 BY `id`) WHERE `_fivetran_active` = true",
		stmt)

	stmt, err = GetInsertFromSelectHistoryToSoftDeleteStatement("s", "from_t", "to_t",
		[]string{"id"}, []string{"id"}, "_fivetran_deleted", true)
	assert.NoError(t, err)
	assert.Equal(t,
		"INSERT INTO `s`.`to_t` (`id`,`_fivetran_synced`,`_fivetran_deleted`) SELECT `id`,`_fivetran_synced`,if(`_fivetran_active` = true, false, true) FROM (SELECT `id`,`_fivetran_synced`,`_fivetran_active` FROM `s`.`from_t` FINAL ORDER BY `id`,`_fivetran_start` DESC LIMIT 1 BY `id`)",
		stmt)

	// composite primary keys: ORDER BY and LIMIT 1 BY must list every PK column in order
	stmt, err = GetInsertFromSelectHistoryToSoftDeleteStatement("s", "from_t", "to_t",
		[]string{"tenant_id", "id", "amount"},
		[]string{"tenant_id", "id"},
		"_fivetran_deleted", false)
	assert.NoError(t, err)
	assert.Equal(t,
		"INSERT INTO `s`.`to_t` (`tenant_id`,`id`,`amount`,`_fivetran_synced`,`_fivetran_deleted`) SELECT `tenant_id`,`id`,`amount`,`_fivetran_synced`,if(`_fivetran_active` = true, false, true) FROM (SELECT `tenant_id`,`id`,`amount`,`_fivetran_synced`,`_fivetran_active` FROM `s`.`from_t` FINAL ORDER BY `tenant_id`,`id`,`_fivetran_start` DESC LIMIT 1 BY `tenant_id`,`id`) WHERE `_fivetran_active` = true",
		stmt)
}

func TestSubtractOneMillisecond(t *testing.T) {
	// typical nanosecond epoch timestamp
	out, err := subtractOneMillisecond("1117314420000000000")
	assert.NoError(t, err)
	assert.Equal(t, "1117314419999000000", out)

	// exact boundary: input equal to 1ms in nanos yields 0
	out, err = subtractOneMillisecond("1000000")
	assert.NoError(t, err)
	assert.Equal(t, "0", out)

	// underflow: input smaller than 1ms in nanos yields a negative value
	// (not expected for real timestamps, but pins down current behavior)
	out, err = subtractOneMillisecond("500000")
	assert.NoError(t, err)
	assert.Equal(t, "-500000", out)

	// non-numeric input surfaces the parse error
	_, err = subtractOneMillisecond("not-a-number")
	assert.Error(t, err)

	// empty string also surfaces a parse error
	_, err = subtractOneMillisecond("")
	assert.Error(t, err)
}

func TestGetUpdateColumnValueStatementSpecialChars(t *testing.T) {
	// Single quote in value should be escaped
	stmt, err := GetUpdateColumnValueStatement("s", "t", "col", "O'Brien", false)
	assert.NoError(t, err)
	assert.Equal(t, "ALTER TABLE `s`.`t` UPDATE `col` = 'O''Brien' WHERE true", stmt)

	// Backslash in value
	stmt, err = GetUpdateColumnValueStatement("s", "t", "col", "path\\to\\file", false)
	assert.NoError(t, err)
	assert.Equal(t, "ALTER TABLE `s`.`t` UPDATE `col` = 'path\\to\\file' WHERE true", stmt)

	// Empty string value (not null)
	stmt, err = GetUpdateColumnValueStatement("s", "t", "col", "", false)
	assert.NoError(t, err)
	assert.Equal(t, "ALTER TABLE `s`.`t` UPDATE `col` = '' WHERE true", stmt)
}

func TestGetTableRowCountQuery(t *testing.T) {
	query, err := GetTableRowCountQuery("foo", "bar")
	assert.NoError(t, err)
	assert.Equal(t, "SELECT count() FROM `foo`.`bar` LIMIT 1", query)
}

func TestGetMaxFivetranStartQuery(t *testing.T) {
	query, err := GetMaxFivetranStartQuery("foo", "bar")
	assert.NoError(t, err)
	assert.Equal(t, "SELECT max(`_fivetran_start`) FROM `foo`.`bar` WHERE `_fivetran_active` = true", query)
}

func TestGetCloseActiveRowsStatement_InvalidTimestamp(t *testing.T) {
	// A non-numeric operation timestamp is a real parse error — not a boundary-validated field.
	_, err := GetCloseActiveRowsStatement("s", "t", "not-a-number", "")
	assert.ErrorContains(t, err, "invalid operation timestamp")
}

func TestGetInsertFromSelectWithHistoryColumnsStatement_EmptyColumns(t *testing.T) {
	// Internal invariant: colNames is derived from DescribeTable and must be non-empty.
	_, err := GetInsertFromSelectWithHistoryColumnsStatement("s", "from", "to", []string{}, "")
	assert.ErrorContains(t, err, "column names list is empty")
}

func TestGetInsertFromSelectHistoryToSoftDeleteStatement_InternalInvariants(t *testing.T) {
	// colNames and pkColNames are derived internally from DescribeTable; these invariants
	// are still enforced by the builder.
	_, err := GetInsertFromSelectHistoryToSoftDeleteStatement("s", "from", "to", []string{}, []string{"id"}, "_fivetran_deleted", false)
	assert.ErrorContains(t, err, "column names list is empty")

	_, err = GetInsertFromSelectHistoryToSoftDeleteStatement("s", "from", "to", []string{"id"}, []string{}, "_fivetran_deleted", false)
	assert.ErrorContains(t, err, "primary key column names list is empty")
}
