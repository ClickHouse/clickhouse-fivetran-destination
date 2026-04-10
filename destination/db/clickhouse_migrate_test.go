package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// These tests cover error-propagation paths in migration functions where the SQL builder
// fails before any DB call is made, so a nil ClickHouseConnection is safe.
// One call per method is sufficient to exercise the `if err != nil { return err }` branch;
// the SQL builder's own validation logic is tested in destination/db/sql/sql_test.go.
var nilConn = &ClickHouseConnection{}

func TestRenameColumn_SQLBuilderError(t *testing.T) {
	err := nilConn.RenameColumn(context.Background(), "", "table", "from", "to")
	assert.Error(t, err)
}

func TestUpdateColumnValue_SQLBuilderError(t *testing.T) {
	err := nilConn.UpdateColumnValue(context.Background(), "", "table", "col", "val", false)
	assert.Error(t, err)
}

func TestCopyColumnData_SQLBuilderError(t *testing.T) {
	err := nilConn.CopyColumnData(context.Background(), "", "table", "from", "to")
	assert.Error(t, err)
}

func TestMigrateCopyTable_SQLBuilderError(t *testing.T) {
	err := nilConn.MigrateCopyTable(context.Background(), "", "from", "to")
	assert.Error(t, err)
}

func TestMigrateAddColumnWithDefault_SQLBuilderError(t *testing.T) {
	err := nilConn.MigrateAddColumnWithDefault(context.Background(), "", "table", "col", "String", "", "val")
	assert.Error(t, err)
}

func TestValidateHistoryModeTable_SQLBuilderError(t *testing.T) {
	_, err := nilConn.validateHistoryModeTable(context.Background(), "", "table", "123")
	assert.Error(t, err)
}
