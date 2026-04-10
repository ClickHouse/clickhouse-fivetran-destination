package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// These tests cover error-propagation paths in migration functions where the SQL builder
// fails before any DB call is made, so a nil ClickHouseConnection is safe.
var nilConn = &ClickHouseConnection{}

func TestRenameColumn_SQLBuilderErrors(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name       string
		schema     string
		table      string
		fromColumn string
		toColumn   string
		errMsg     string
	}{
		{"empty schema", "", "table", "from", "to", "schema name for table table is empty"},
		{"empty table", "schema", "", "from", "to", "table name is empty"},
		{"empty fromColumn", "schema", "table", "", "to", "from column name is empty"},
		{"empty toColumn", "schema", "table", "from", "", "to column name is empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := nilConn.RenameColumn(ctx, tt.schema, tt.table, tt.fromColumn, tt.toColumn)
			assert.ErrorContains(t, err, tt.errMsg)
		})
	}
}

func TestUpdateColumnValue_SQLBuilderErrors(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name   string
		schema string
		table  string
		column string
		errMsg string
	}{
		{"empty schema", "", "table", "col", "schema name for table table is empty"},
		{"empty table", "schema", "", "col", "table name is empty"},
		{"empty column", "schema", "table", "", "column name is empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := nilConn.UpdateColumnValue(ctx, tt.schema, tt.table, tt.column, "val", false)
			assert.ErrorContains(t, err, tt.errMsg)
		})
	}
}

func TestCopyColumnData_SQLBuilderErrors(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name       string
		schema     string
		table      string
		fromColumn string
		toColumn   string
		errMsg     string
	}{
		{"empty schema", "", "table", "from", "to", "schema name for table table is empty"},
		{"empty table", "schema", "", "from", "to", "table name is empty"},
		{"empty fromColumn", "schema", "table", "", "to", "from column name is empty"},
		{"empty toColumn", "schema", "table", "from", "", "to column name is empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := nilConn.CopyColumnData(ctx, tt.schema, tt.table, tt.fromColumn, tt.toColumn)
			assert.ErrorContains(t, err, tt.errMsg)
		})
	}
}

func TestMigrateCopyTable_SQLBuilderErrors(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		schema    string
		fromTable string
		toTable   string
		errMsg    string
	}{
		{"empty schema", "", "from", "to", "schema name is empty"},
		{"empty fromTable", "schema", "", "to", "from table name is empty"},
		{"empty toTable", "schema", "from", "", "to table name is empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := nilConn.MigrateCopyTable(ctx, tt.schema, tt.fromTable, tt.toTable)
			assert.ErrorContains(t, err, tt.errMsg)
		})
	}
}

func TestMigrateAddColumnWithDefault_SQLBuilderErrors(t *testing.T) {
	ctx := context.Background()
	err := nilConn.MigrateAddColumnWithDefault(ctx, "", "table", "col", "String", "", "val")
	assert.ErrorContains(t, err, "schema name for table table is empty")

	err = nilConn.MigrateAddColumnWithDefault(ctx, "schema", "", "col", "String", "", "val")
	assert.ErrorContains(t, err, "table name is empty")
}

func TestValidateHistoryModeTable_SQLBuilderErrors(t *testing.T) {
	ctx := context.Background()
	_, err := nilConn.validateHistoryModeTable(ctx, "", "table", "123")
	assert.ErrorContains(t, err, "schema name for table table is empty")

	_, err = nilConn.validateHistoryModeTable(ctx, "schema", "", "123")
	assert.ErrorContains(t, err, "table name is empty")
}
