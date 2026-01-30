package db

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"fivetran.com/fivetran_sdk/destination/common/types"
	"fivetran.com/fivetran_sdk/destination/db/sql"
	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestFindOptimalMutationBatchSize tests various batch sizes to find the maximum
// that works with ClickHouse default settings:
//   - max_ast_elements = 50000
//   - max_query_size = 262144 (256 KB)
//   - max_parser_backtracks = 1000000
//
// This helps determine safe defaults for MutationBatchSize and HardDeleteBatchSize
// that won't cause OOM or parsing errors in production.
func TestFindOptimalMutationBatchSize(t *testing.T) {
	ctx := context.Background()
	conn, err := GetClickHouseConnection(ctx, map[string]string{
		"host":     "localhost",
		"port":     "9000",
		"username": "default",
		"local":    "true",
	})
	require.NoError(t, err)
	defer conn.Close()

	// Create a test table for mutations
	schemaName := "mutation_batch_test"
	tableName := fmt.Sprintf("test_%s", strings.ReplaceAll(uuid.New().String(), "-", ""))

	// Create the test database and table
	err = conn.CreateDatabase(ctx, schemaName)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	tableDesc := types.MakeTableDescription([]*types.ColumnDefinition{
		{Name: "id", Type: "Int64", IsPrimaryKey: true},
		{Name: "name", Type: "Nullable(String)"},
		{Name: "_fivetran_synced", Type: "DateTime64(9, 'UTC')"},
		{Name: "_fivetran_deleted", Type: "Bool"},
		{Name: "_fivetran_active", Type: "Bool"},
		{Name: "_fivetran_start", Type: "DateTime64(9, 'UTC')", IsPrimaryKey: true},
		{Name: "_fivetran_end", Type: "Nullable(DateTime64(9, 'UTC'))"},
	})

	err = conn.CreateTable(ctx, schemaName, tableName, tableDesc)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	qualifiedTableName, _ := sql.GetQualifiedTableName(schemaName, tableName)

	// Test batch sizes: start small, increase until failure
	batchSizes := []uint{100, 250, 500, 750, 1000, 1500, 2000, 2500, 3000, 4000, 5000, 7500, 10000}

	t.Log("=== Testing UPDATE Mutation Batch Sizes (GetUpdateHistoryActiveStatement) ===")
	t.Log("ClickHouse defaults: max_ast_elements=50000, max_query_size=256KB")
	t.Log("")

	maxWorkingUpdateBatch := uint(0)
	for _, batchSize := range batchSizes {
		csv := generateTestCSV(batchSize)
		csvColumns := makeTestCSVColumns()

		// Generate the UPDATE statement (the problematic one with CASE/WHEN)
		statement, err := sql.GetUpdateHistoryActiveStatement(
			csv,
			csvColumns,
			qualifiedTableName,
			5, // _fivetran_start index
			pb.DataType_UTC_DATETIME,
		)
		if err != nil {
			t.Logf("  [%5d rows] ❌ Failed to generate statement: %v", batchSize, err)
			continue
		}

		querySize := len(statement)
		t.Logf("  [%5d rows] Query size: %d bytes (%.2f KB)", batchSize, querySize, float64(querySize)/1024)

		// Try to execute the statement
		err = conn.ExecStatement(ctx, statement, "TestUpdateMutation", false)
		if err != nil {
			t.Logf("  [%5d rows] ❌ FAILED: %v", batchSize, err)
			break
		} else {
			t.Logf("  [%5d rows] ✅ SUCCESS", batchSize)
			maxWorkingUpdateBatch = batchSize
		}
	}

	t.Log("")
	t.Log("=== Testing DELETE Mutation Batch Sizes (GetHardDeleteStatement) ===")
	t.Log("")

	maxWorkingDeleteBatch := uint(0)
	for _, batchSize := range batchSizes {
		csv := generateTestCSV(batchSize)
		csvColumns := makeTestCSVColumns()

		// Generate the DELETE statement
		statement, err := sql.GetHardDeleteStatement(
			csv,
			csvColumns,
			qualifiedTableName,
		)
		if err != nil {
			t.Logf("  [%5d rows] ❌ Failed to generate statement: %v", batchSize, err)
			continue
		}

		querySize := len(statement)
		t.Logf("  [%5d rows] Query size: %d bytes (%.2f KB)", batchSize, querySize, float64(querySize)/1024)

		// Try to execute the statement
		err = conn.ExecStatement(ctx, statement, "TestDeleteMutation", false)
		if err != nil {
			t.Logf("  [%5d rows] ❌ FAILED: %v", batchSize, err)
			break
		} else {
			t.Logf("  [%5d rows] ✅ SUCCESS", batchSize)
			maxWorkingDeleteBatch = batchSize
		}
	}

	t.Log("")
	t.Log("=== Testing DELETE with Timestamp Batch Sizes (GetHardDeleteWithTimestampStatement) ===")
	t.Log("")

	maxWorkingDeleteWithTsBatch := uint(0)
	for _, batchSize := range batchSizes {
		csv := generateTestCSV(batchSize)
		csvColumns := makeTestCSVColumns()

		// Generate the DELETE with timestamp statement (uses OR chains)
		statement, err := sql.GetHardDeleteWithTimestampStatement(
			csv,
			csvColumns,
			qualifiedTableName,
			"_fivetran_start",
			5, // _fivetran_start index
			pb.DataType_UTC_DATETIME,
		)
		if err != nil {
			t.Logf("  [%5d rows] ❌ Failed to generate statement: %v", batchSize, err)
			continue
		}

		querySize := len(statement)
		t.Logf("  [%5d rows] Query size: %d bytes (%.2f KB)", batchSize, querySize, float64(querySize)/1024)

		// Try to execute the statement
		err = conn.ExecStatement(ctx, statement, "TestDeleteWithTsMutation", false)
		if err != nil {
			t.Logf("  [%5d rows] ❌ FAILED: %v", batchSize, err)
			break
		} else {
			t.Logf("  [%5d rows] ✅ SUCCESS", batchSize)
			maxWorkingDeleteWithTsBatch = batchSize
		}
	}

	t.Log("")
	t.Log("╔════════════════════════════════════════════════════════════════╗")
	t.Log("║                        RECOMMENDATIONS                         ║")
	t.Log("╠════════════════════════════════════════════════════════════════╣")
	t.Logf("║  Max working UPDATE batch size:              %5d rows        ║", maxWorkingUpdateBatch)
	t.Logf("║  Max working DELETE batch size:              %5d rows        ║", maxWorkingDeleteBatch)
	t.Logf("║  Max working DELETE+Timestamp batch size:    %5d rows        ║", maxWorkingDeleteWithTsBatch)
	t.Log("╠════════════════════════════════════════════════════════════════╣")

	// Recommend 80% of max as safe default
	recommendedUpdate := uint(float64(maxWorkingUpdateBatch) * 0.8)
	recommendedDelete := uint(float64(maxWorkingDeleteBatch) * 0.8)
	recommendedDeleteWithTs := uint(float64(maxWorkingDeleteWithTsBatch) * 0.8)

	t.Logf("║  Recommended MutationBatchSize (80%%):        %5d rows        ║", recommendedUpdate)
	t.Logf("║  Recommended HardDeleteBatchSize (80%%):      %5d rows        ║", min(recommendedDelete, recommendedDeleteWithTs))
	t.Log("╚════════════════════════════════════════════════════════════════╝")

	// Clean up
	dropStmt, _ := sql.GetDropTableStatement(qualifiedTableName)
	conn.ExecStatement(ctx, dropStmt, "DropTestTable", false)
}

// generateTestCSV creates test CSV data with the specified number of rows
func generateTestCSV(rowCount uint) [][]string {
	csv := make([][]string, rowCount)
	for i := uint(0); i < rowCount; i++ {
		csv[i] = []string{
			fmt.Sprintf("%d", i),                    // id
			fmt.Sprintf("name_%d", i),               // name
			"2024-01-15T10:30:00.123456789Z",        // _fivetran_synced
			"false",                                 // _fivetran_deleted
			"true",                                  // _fivetran_active
			"2024-01-15T10:30:00.123456789Z",        // _fivetran_start
			"",                                      // _fivetran_end (null)
		}
	}
	return csv
}

// makeTestCSVColumns creates CSV column metadata for testing
func makeTestCSVColumns() *types.CSVColumns {
	allCols := []*types.CSVColumn{
		{Index: 0, Name: "id", Type: pb.DataType_LONG, IsPrimaryKey: true, TableIndex: 0},
		{Index: 1, Name: "name", Type: pb.DataType_STRING, TableIndex: 1},
		{Index: 2, Name: "_fivetran_synced", Type: pb.DataType_UTC_DATETIME, TableIndex: 2},
		{Index: 3, Name: "_fivetran_deleted", Type: pb.DataType_BOOLEAN, TableIndex: 3},
		{Index: 4, Name: "_fivetran_active", Type: pb.DataType_BOOLEAN, TableIndex: 4},
		{Index: 5, Name: "_fivetran_start", Type: pb.DataType_UTC_DATETIME, IsPrimaryKey: true, TableIndex: 5},
		{Index: 6, Name: "_fivetran_end", Type: pb.DataType_UTC_DATETIME, TableIndex: 6},
	}

	pkCols := []*types.CSVColumn{
		{Index: 0, Name: "id", Type: pb.DataType_LONG, IsPrimaryKey: true, TableIndex: 0},
		{Index: 5, Name: "_fivetran_start", Type: pb.DataType_UTC_DATETIME, IsPrimaryKey: true, TableIndex: 5},
	}

	return &types.CSVColumns{
		All:         allCols,
		PrimaryKeys: pkCols,
	}
}

func min(a, b uint) uint {
	if a < b {
		return a
	}
	return b
}

// TestFindOptimalMutationBatchSizeRealisticSchema tests with a schema matching
// real Fivetran data like ad_group_criterion_label_history which has:
// - 3 PK columns (ad_group_id, criterion_id, label_id) - all large integers
// - This generates larger SQL per row than simple schemas
//
// Uses ClickHouse default settings (no modifications):
// - max_query_size = 262144 (256 KB)
// - max_ast_elements = 50000
func TestFindOptimalMutationBatchSizeRealisticSchema(t *testing.T) {
	ctx := context.Background()
	conn, err := GetClickHouseConnection(ctx, map[string]string{
		"host":     "localhost",
		"port":     "9000",
		"username": "default",
		"local":    "true",
	})
	require.NoError(t, err)
	defer conn.Close()

	// Query and display the actual ClickHouse settings
	t.Log("")
	t.Log("=== ClickHouse Instance Settings (unmodified defaults) ===")
	rows, err := conn.Query(ctx, "SELECT name, value FROM system.settings WHERE name IN ('max_query_size', 'max_ast_elements', 'max_parser_backtracks') ORDER BY name")
	if err != nil {
		t.Logf("Could not query settings: %v", err)
	} else {
		defer rows.Close()
		for rows.Next() {
			var name, value string
			if err := rows.Scan(&name, &value); err == nil {
				t.Logf("  %s = %s", name, value)
			}
		}
	}

	// Create a test table matching ad_group_criterion_label_history schema
	schemaName := "mutation_batch_test"
	tableName := fmt.Sprintf("realistic_%s", strings.ReplaceAll(uuid.New().String(), "-", ""))

	err = conn.CreateDatabase(ctx, schemaName)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Schema matching ad_group_criterion_label_history: 3 PK columns + fivetran columns
	tableDesc := types.MakeTableDescription([]*types.ColumnDefinition{
		{Name: "ad_group_id", Type: "Int64", IsPrimaryKey: true},
		{Name: "criterion_id", Type: "Int64", IsPrimaryKey: true},
		{Name: "label_id", Type: "Int64", IsPrimaryKey: true},
		{Name: "_fivetran_synced", Type: "DateTime64(9, 'UTC')"},
		{Name: "_fivetran_deleted", Type: "Bool"},
		{Name: "_fivetran_active", Type: "Bool"},
		{Name: "_fivetran_start", Type: "DateTime64(9, 'UTC')", IsPrimaryKey: true},
		{Name: "_fivetran_end", Type: "Nullable(DateTime64(9, 'UTC'))"},
	})

	err = conn.CreateTable(ctx, schemaName, tableName, tableDesc)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	qualifiedTableName, _ := sql.GetQualifiedTableName(schemaName, tableName)

	// Test batch sizes incrementally to find exact failure point
	batchSizes := []uint{100, 250, 500, 750, 1000, 1250, 1500, 1750, 2000, 2500, 3000}

	t.Log("")
	t.Log("╔════════════════════════════════════════════════════════════════════════════╗")
	t.Log("║  REALISTIC SCHEMA TEST (3-column PK like ad_group_criterion_label_history) ║")
	t.Log("║  PK: ad_group_id, criterion_id, label_id + _fivetran_start                 ║")
	t.Log("║  Testing against ClickHouse default settings (no modifications)            ║")
	t.Log("╚════════════════════════════════════════════════════════════════════════════╝")
	t.Log("")
	t.Log("=== Testing UPDATE Mutation Batch Sizes ===")

	maxWorkingUpdateBatch := uint(0)
	for _, batchSize := range batchSizes {
		csv := generateRealisticCSV(batchSize)
		csvColumns := makeRealisticCSVColumns()

		statement, err := sql.GetUpdateHistoryActiveStatement(
			csv,
			csvColumns,
			qualifiedTableName,
			6, // _fivetran_start index
			pb.DataType_UTC_DATETIME,
		)
		if err != nil {
			t.Logf("  [%5d rows] ❌ Failed to generate statement: %v", batchSize, err)
			continue
		}

		querySize := len(statement)
		t.Logf("  [%5d rows] Query size: %7d bytes (%6.2f KB) | %d bytes/row",
			batchSize, querySize, float64(querySize)/1024, querySize/int(batchSize))

		err = conn.ExecStatement(ctx, statement, "TestUpdateMutation", false)
		if err != nil {
			t.Logf("  [%5d rows] ❌ FAILED: %v", batchSize, err)
			break
		} else {
			t.Logf("  [%5d rows] ✅ SUCCESS", batchSize)
			maxWorkingUpdateBatch = batchSize
		}
	}

	t.Log("")
	t.Log("=== Testing DELETE+Timestamp Mutation Batch Sizes ===")

	maxWorkingDeleteWithTsBatch := uint(0)
	for _, batchSize := range batchSizes {
		csv := generateRealisticCSV(batchSize)
		csvColumns := makeRealisticCSVColumns()

		statement, err := sql.GetHardDeleteWithTimestampStatement(
			csv,
			csvColumns,
			qualifiedTableName,
			"_fivetran_start",
			6, // _fivetran_start index
			pb.DataType_UTC_DATETIME,
		)
		if err != nil {
			t.Logf("  [%5d rows] ❌ Failed to generate statement: %v", batchSize, err)
			continue
		}

		querySize := len(statement)
		t.Logf("  [%5d rows] Query size: %7d bytes (%6.2f KB) | %d bytes/row",
			batchSize, querySize, float64(querySize)/1024, querySize/int(batchSize))

		err = conn.ExecStatement(ctx, statement, "TestDeleteWithTsMutation", false)
		if err != nil {
			t.Logf("  [%5d rows] ❌ FAILED: %v", batchSize, err)
			break
		} else {
			t.Logf("  [%5d rows] ✅ SUCCESS", batchSize)
			maxWorkingDeleteWithTsBatch = batchSize
		}
	}

	t.Log("")
	t.Log("╔════════════════════════════════════════════════════════════════════════════╗")
	t.Log("║                    REALISTIC SCHEMA RECOMMENDATIONS                        ║")
	t.Log("╠════════════════════════════════════════════════════════════════════════════╣")
	t.Logf("║  Max working UPDATE batch size (3-col PK):        %5d rows              ║", maxWorkingUpdateBatch)
	t.Logf("║  Max working DELETE+Timestamp batch size:         %5d rows              ║", maxWorkingDeleteWithTsBatch)
	t.Log("╠════════════════════════════════════════════════════════════════════════════╣")

	recommendedUpdate := uint(float64(maxWorkingUpdateBatch) * 0.8)
	recommendedDelete := uint(float64(maxWorkingDeleteWithTsBatch) * 0.8)

	t.Logf("║  Recommended MutationBatchSize (80%%):             %5d rows              ║", recommendedUpdate)
	t.Logf("║  Recommended HardDeleteBatchSize (80%%):           %5d rows              ║", recommendedDelete)
	t.Log("╚════════════════════════════════════════════════════════════════════════════╝")

	// Clean up
	dropStmt, _ := sql.GetDropTableStatement(qualifiedTableName)
	conn.ExecStatement(ctx, dropStmt, "DropTestTable", false)
}

// generateRealisticCSV creates test CSV data matching ad_group_criterion_label_history
// Uses large integers similar to real Google Ads IDs (12-digit numbers)
func generateRealisticCSV(rowCount uint) [][]string {
	csv := make([][]string, rowCount)
	for i := uint(0); i < rowCount; i++ {
		csv[i] = []string{
			fmt.Sprintf("%d", 160112174822+i),       // ad_group_id (12-digit)
			fmt.Sprintf("%d", 361397856850+i),       // criterion_id (12-digit)
			fmt.Sprintf("%d", 21956092608+i),        // label_id (11-digit)
			"2024-01-15T10:30:00.123456789Z",        // _fivetran_synced
			"false",                                 // _fivetran_deleted
			"true",                                  // _fivetran_active
			"2024-01-15T10:30:00.123456789Z",        // _fivetran_start
			"",                                      // _fivetran_end (null)
		}
	}
	return csv
}

// makeRealisticCSVColumns creates CSV column metadata matching ad_group_criterion_label_history
func makeRealisticCSVColumns() *types.CSVColumns {
	allCols := []*types.CSVColumn{
		{Index: 0, Name: "ad_group_id", Type: pb.DataType_LONG, IsPrimaryKey: true, TableIndex: 0},
		{Index: 1, Name: "criterion_id", Type: pb.DataType_LONG, IsPrimaryKey: true, TableIndex: 1},
		{Index: 2, Name: "label_id", Type: pb.DataType_LONG, IsPrimaryKey: true, TableIndex: 2},
		{Index: 3, Name: "_fivetran_synced", Type: pb.DataType_UTC_DATETIME, TableIndex: 3},
		{Index: 4, Name: "_fivetran_deleted", Type: pb.DataType_BOOLEAN, TableIndex: 4},
		{Index: 5, Name: "_fivetran_active", Type: pb.DataType_BOOLEAN, TableIndex: 5},
		{Index: 6, Name: "_fivetran_start", Type: pb.DataType_UTC_DATETIME, IsPrimaryKey: true, TableIndex: 6},
		{Index: 7, Name: "_fivetran_end", Type: pb.DataType_UTC_DATETIME, TableIndex: 7},
	}

	// 4 PK columns total: ad_group_id, criterion_id, label_id, _fivetran_start
	pkCols := []*types.CSVColumn{
		{Index: 0, Name: "ad_group_id", Type: pb.DataType_LONG, IsPrimaryKey: true, TableIndex: 0},
		{Index: 1, Name: "criterion_id", Type: pb.DataType_LONG, IsPrimaryKey: true, TableIndex: 1},
		{Index: 2, Name: "label_id", Type: pb.DataType_LONG, IsPrimaryKey: true, TableIndex: 2},
		{Index: 6, Name: "_fivetran_start", Type: pb.DataType_UTC_DATETIME, IsPrimaryKey: true, TableIndex: 6},
	}

	return &types.CSVColumns{
		All:         allCols,
		PrimaryKeys: pkCols,
	}
}
