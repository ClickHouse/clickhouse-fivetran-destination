package mutation_batch_size

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"fivetran.com/fivetran_sdk/destination/common/types"
	"fivetran.com/fivetran_sdk/destination/db"
	"fivetran.com/fivetran_sdk/destination/db/sql"
	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type batchSizeTestConfig struct {
	tableDesc          *types.TableDescription
	batchSizes         []uint
	generateCSV        func(uint) [][]string
	makeCSVColumns     func() *types.CSVColumns
	fivetranStartIndex uint
}

// TestFindOptimalMutationBatchSize tests batch sizes with a simple schema (2 PKs, 7 columns)
// against ClickHouse default settings (max_ast_elements=50000, max_query_size=256KB).
func TestFindOptimalMutationBatchSize(t *testing.T) {
	runBatchSizeTest(t, batchSizeTestConfig{
		batchSizes:     []uint{100, 250, 500, 750, 1000, 1500, 2000, 2500, 3000, 4000, 5000, 7500, 10000},
		generateCSV:    generateSimpleCSV,
		makeCSVColumns: makeSimpleCSVColumns,
		tableDesc: types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "id", Type: "Int64", IsPrimaryKey: true},
			{Name: "name", Type: "Nullable(String)"},
			{Name: "_fivetran_synced", Type: "DateTime64(9, 'UTC')"},
			{Name: "_fivetran_deleted", Type: "Bool"},
			{Name: "_fivetran_active", Type: "Bool"},
			{Name: "_fivetran_start", Type: "DateTime64(9, 'UTC')", IsPrimaryKey: true},
			{Name: "_fivetran_end", Type: "Nullable(DateTime64(9, 'UTC'))"},
		}),
		fivetranStartIndex: 5,
	})
}

// TestFindRealisticMutationBatchSize tests batch sizes with a realistic schema
// matching ad_group_criterion_label_history (4 PKs with large 12-digit integers).
func TestFindRealisticMutationBatchSize(t *testing.T) {
	runBatchSizeTest(t, batchSizeTestConfig{
		batchSizes:     []uint{100, 250, 500, 750, 1000, 1250, 1500, 1750, 2000, 2500, 3000},
		generateCSV:    generateRealisticCSV,
		makeCSVColumns: makeRealisticCSVColumns,
		tableDesc: types.MakeTableDescription([]*types.ColumnDefinition{
			{Name: "ad_group_id", Type: "Int64", IsPrimaryKey: true},
			{Name: "criterion_id", Type: "Int64", IsPrimaryKey: true},
			{Name: "label_id", Type: "Int64", IsPrimaryKey: true},
			{Name: "_fivetran_synced", Type: "DateTime64(9, 'UTC')"},
			{Name: "_fivetran_deleted", Type: "Bool"},
			{Name: "_fivetran_active", Type: "Bool"},
			{Name: "_fivetran_start", Type: "DateTime64(9, 'UTC')", IsPrimaryKey: true},
			{Name: "_fivetran_end", Type: "Nullable(DateTime64(9, 'UTC'))"},
		}),
		fivetranStartIndex: 6,
	})
}

type mutationTestCase struct {
	name     string
	generate func(csv [][]string, cols *types.CSVColumns, table sql.QualifiedTableName) (string, error)
}

func runBatchSizeTest(t *testing.T, cfg batchSizeTestConfig) {
	t.Helper()
	ctx := context.Background()
	conn, err := db.GetClickHouseConnection(ctx, map[string]string{
		"host":     "localhost",
		"port":     "9000",
		"username": "default",
		"local":    "true",
	})
	require.NoError(t, err)
	defer conn.Close()

	schemaName := "mutation_batch_test"
	tableName := fmt.Sprintf("test_%s", strings.ReplaceAll(uuid.New().String(), "-", ""))

	err = conn.CreateDatabase(ctx, schemaName)
	require.NoError(t, err, "Failed to create database")

	err = conn.CreateTable(ctx, schemaName, tableName, cfg.tableDesc)
	require.NoError(t, err, "Failed to create table")

	qualifiedTableName, _ := sql.GetQualifiedTableName(schemaName, tableName)
	defer func() {
		dropStmt, _ := sql.GetDropTableStatement(qualifiedTableName)
		conn.Exec(ctx, dropStmt)
	}()

	mutations := []mutationTestCase{
		{
			name: "UPDATE",
			generate: func(csv [][]string, cols *types.CSVColumns, table sql.QualifiedTableName) (string, error) {
				return sql.GetUpdateHistoryActiveStatement(csv, cols, table, cfg.fivetranStartIndex, pb.DataType_UTC_DATETIME)
			},
		},
		{
			name: "DELETE",
			generate: func(csv [][]string, cols *types.CSVColumns, table sql.QualifiedTableName) (string, error) {
				return sql.GetHardDeleteStatement(csv, cols, table)
			},
		},
		{
			name: "DELETE+Timestamp",
			generate: func(csv [][]string, cols *types.CSVColumns, table sql.QualifiedTableName) (string, error) {
				return sql.GetHardDeleteWithTimestampStatement(csv, cols, table, "_fivetran_start", cfg.fivetranStartIndex, pb.DataType_UTC_DATETIME)
			},
		},
	}

	results := make(map[string]uint, len(mutations))
	for _, mutation := range mutations {
		results[mutation.name] = runMutationSweep(t, ctx, conn, cfg.batchSizes, cfg.generateCSV, cfg.makeCSVColumns, mutation, qualifiedTableName)
	}

	minDeleteMax := uint(0)
	for _, mutation := range mutations {
		if mutation.name == "UPDATE" {
			continue
		}
		if minDeleteMax == 0 || results[mutation.name] < minDeleteMax {
			minDeleteMax = results[mutation.name]
		}
	}
	recommendedUpdate := uint(float64(results["UPDATE"]) * 0.8)
	recommendedDelete := uint(float64(minDeleteMax) * 0.8)

	t.Log("")
	t.Log("╔════════════════════════════════════════════════════════════════╗")
	t.Log("║                        RECOMMENDATIONS                         ║")
	t.Log("╠════════════════════════════════════════════════════════════════╣")
	for _, mutation := range mutations {
		t.Logf("║  Max working %-29s  %5d rows         ║", mutation.name+" batch size:", results[mutation.name])
	}
	t.Log("╠════════════════════════════════════════════════════════════════╣")
	t.Logf("║  Recommended MutationBatchSize (80%%):    %8d rows         ║", recommendedUpdate)
	t.Logf("║  Recommended HardDeleteBatchSize (80%%):  %8d rows         ║", recommendedDelete)
	t.Log("╚════════════════════════════════════════════════════════════════╝")
}

func runMutationSweep(
	t *testing.T,
	ctx context.Context,
	conn *db.ClickHouseConnection,
	batchSizes []uint,
	generateCSV func(uint) [][]string,
	makeCSVColumns func() *types.CSVColumns,
	mutation mutationTestCase,
	qualifiedTableName sql.QualifiedTableName,
) uint {
	t.Helper()
	t.Logf("\n=== %s ===", mutation.name)

	maxWorking := uint(0)
	for _, batchSize := range batchSizes {
		csv := generateCSV(batchSize)
		cols := makeCSVColumns()

		statement, err := mutation.generate(csv, cols, qualifiedTableName)
		if err != nil {
			t.Logf("  [%5d rows] SKIP: %v", batchSize, err)
			continue
		}

		querySize := len(statement)
		t.Logf("  [%5d rows] %d bytes (%.2f KB)", batchSize, querySize, float64(querySize)/1024)

		if err = conn.Exec(ctx, statement); err != nil {
			t.Logf("  [%5d rows] FAILED: %v", batchSize, err)
			break
		}
		t.Logf("  [%5d rows] OK", batchSize)
		maxWorking = batchSize
	}
	return maxWorking
}

func generateSimpleCSV(rowCount uint) [][]string {
	csv := make([][]string, rowCount)
	for i := uint(0); i < rowCount; i++ {
		csv[i] = []string{
			fmt.Sprintf("%d", i),
			fmt.Sprintf("name_%d", i),
			"2024-01-15T10:30:00.123456789Z",
			"false",
			"true",
			"2024-01-15T10:30:00.123456789Z",
			"",
		}
	}
	return csv
}

func makeSimpleCSVColumns() *types.CSVColumns {
	return &types.CSVColumns{
		All: []*types.CSVColumn{
			{Index: 0, Name: "id", Type: pb.DataType_LONG, IsPrimaryKey: true, TableIndex: 0},
			{Index: 1, Name: "name", Type: pb.DataType_STRING, TableIndex: 1},
			{Index: 2, Name: "_fivetran_synced", Type: pb.DataType_UTC_DATETIME, TableIndex: 2},
			{Index: 3, Name: "_fivetran_deleted", Type: pb.DataType_BOOLEAN, TableIndex: 3},
			{Index: 4, Name: "_fivetran_active", Type: pb.DataType_BOOLEAN, TableIndex: 4},
			{Index: 5, Name: "_fivetran_start", Type: pb.DataType_UTC_DATETIME, IsPrimaryKey: true, TableIndex: 5},
			{Index: 6, Name: "_fivetran_end", Type: pb.DataType_UTC_DATETIME, TableIndex: 6},
		},
		PrimaryKeys: []*types.CSVColumn{
			{Index: 0, Name: "id", Type: pb.DataType_LONG, IsPrimaryKey: true, TableIndex: 0},
			{Index: 5, Name: "_fivetran_start", Type: pb.DataType_UTC_DATETIME, IsPrimaryKey: true, TableIndex: 5},
		},
	}
}

func generateRealisticCSV(rowCount uint) [][]string {
	csv := make([][]string, rowCount)
	for i := uint(0); i < rowCount; i++ {
		csv[i] = []string{
			fmt.Sprintf("%d", 160112174822+i),
			fmt.Sprintf("%d", 361397856850+i),
			fmt.Sprintf("%d", 21956092608+i),
			"2024-01-15T10:30:00.123456789Z",
			"false",
			"true",
			"2024-01-15T10:30:00.123456789Z",
			"",
		}
	}
	return csv
}

func makeRealisticCSVColumns() *types.CSVColumns {
	return &types.CSVColumns{
		All: []*types.CSVColumn{
			{Index: 0, Name: "ad_group_id", Type: pb.DataType_LONG, IsPrimaryKey: true, TableIndex: 0},
			{Index: 1, Name: "criterion_id", Type: pb.DataType_LONG, IsPrimaryKey: true, TableIndex: 1},
			{Index: 2, Name: "label_id", Type: pb.DataType_LONG, IsPrimaryKey: true, TableIndex: 2},
			{Index: 3, Name: "_fivetran_synced", Type: pb.DataType_UTC_DATETIME, TableIndex: 3},
			{Index: 4, Name: "_fivetran_deleted", Type: pb.DataType_BOOLEAN, TableIndex: 4},
			{Index: 5, Name: "_fivetran_active", Type: pb.DataType_BOOLEAN, TableIndex: 5},
			{Index: 6, Name: "_fivetran_start", Type: pb.DataType_UTC_DATETIME, IsPrimaryKey: true, TableIndex: 6},
			{Index: 7, Name: "_fivetran_end", Type: pb.DataType_UTC_DATETIME, TableIndex: 7},
		},
		PrimaryKeys: []*types.CSVColumn{
			{Index: 0, Name: "ad_group_id", Type: pb.DataType_LONG, IsPrimaryKey: true, TableIndex: 0},
			{Index: 1, Name: "criterion_id", Type: pb.DataType_LONG, IsPrimaryKey: true, TableIndex: 1},
			{Index: 2, Name: "label_id", Type: pb.DataType_LONG, IsPrimaryKey: true, TableIndex: 2},
			{Index: 6, Name: "_fivetran_start", Type: pb.DataType_UTC_DATETIME, IsPrimaryKey: true, TableIndex: 6},
		},
	}
}
