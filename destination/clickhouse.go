package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

type WriteBatchOpType string

const (
	Replace    WriteBatchOpType = "Replace"
	Update     WriteBatchOpType = "Update"
	SoftDelete WriteBatchOpType = "SoftDelete"
)

// ClickHouseConnection
// TODO: on premise cluster setup for DDL
type ClickHouseConnection struct {
	Database string
	ctx      context.Context
	driver.Conn
}

func GetClickHouseConnection(ctx context.Context, configuration map[string]string) (*ClickHouseConnection, error) {
	return RetryNetErrorWithData(func() (*ClickHouseConnection, error) {
		hostname := fmt.Sprintf("%s:%s",
			GetWithDefault(configuration, "hostname", "localhost"),
			GetWithDefault(configuration, "port", "9000"))
		username := GetWithDefault(configuration, "username", "default")
		password := GetWithDefault(configuration, "password", "")
		database := GetWithDefault(configuration, "database", "default")
		options := &clickhouse.Options{
			Addr: []string{hostname},
			Auth: clickhouse.Auth{
				Username: username,
				Password: password,
				Database: database,
			},
			Protocol: clickhouse.Native,
			Settings: clickhouse.Settings{
				"allow_experimental_object_type": 1,
				"date_time_input_format":         "best_effort",
			},
			MaxOpenConns: int(*maxOpenConnections),
			MaxIdleConns: int(*maxIdleConnections),
			ClientInfo: clickhouse.ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{Name: "fivetran-destination", Version: Version},
				},
			},
		}
		ssl := GetWithDefault(configuration, "ssl", "false")
		if ssl == "true" {
			options.TLS = &tls.Config{InsecureSkipVerify: true}
		}
		conn, err := clickhouse.Open(options)
		if err != nil {
			LogError(fmt.Errorf("error while opening a connection to ClickHouse: %w", err))
			return nil, err
		}
		return &ClickHouseConnection{database, ctx, conn}, nil
	}, ctx, "GetClickHouseConnection")
}

func (conn *ClickHouseConnection) DescribeTable(schemaName string, tableName string) (*TableDescription, error) {
	return RetryNetErrorWithData(func() (*TableDescription, error) {
		query, err := GetDescribeTableQuery(schemaName, tableName)
		if err != nil {
			return nil, err
		}
		rows, err := conn.Query(conn.ctx, query)
		if err != nil {
			LogError(fmt.Errorf("error while executing %s: %w", query, err))
			return nil, err
		}
		var (
			colName      string
			colType      string
			colComment   string
			isPrimaryKey uint8
			precision    *uint64
			scale        *uint64
		)
		var columns []*ColumnDefinition
		for rows.Next() {
			if err = rows.Scan(&colName, &colType, &colComment, &isPrimaryKey, &precision, &scale); err != nil {
				return nil, err
			}
			var decimalParams *DecimalParams = nil
			if precision != nil && scale != nil {
				decimalParams = &DecimalParams{Precision: *precision, Scale: *scale}
			}
			columns = append(columns, &ColumnDefinition{
				Name:          colName,
				Type:          colType,
				Comment:       colComment,
				IsPrimaryKey:  isPrimaryKey == 1,
				DecimalParams: decimalParams,
			})
		}
		return MakeTableDescription(columns), nil
	}, conn.ctx, "DescribeTable")
}

func (conn *ClickHouseConnection) CreateTable(schemaName string, tableName string, tableDescription *TableDescription) error {
	return RetryNetError(func() error {
		statement, err := GetCreateTableStatement(schemaName, tableName, tableDescription)
		if err != nil {
			return err
		}
		if err = conn.Exec(conn.ctx, statement); err != nil {
			LogError(fmt.Errorf("error while executing %s: %w", statement, err))
			return err
		}
		return nil
	}, conn.ctx, "CreateTable")
}

func (conn *ClickHouseConnection) AlterTable(schemaName string, tableName string, ops []*AlterTableOp) error {
	return RetryNetError(func() error {
		statement, err := GetAlterTableStatement(schemaName, tableName, ops)
		if err != nil {
			return err
		}
		if err = conn.Exec(conn.ctx, statement); err != nil {
			LogError(fmt.Errorf("error while executing %s: %w", statement, err))
			return err
		}
		return nil
	}, conn.ctx, "AlterTable")
}

func (conn *ClickHouseConnection) TruncateTable(schemaName string, tableName string) error {
	return RetryNetError(func() error {
		statement, err := GetTruncateTableStatement(schemaName, tableName)
		if err != nil {
			return err
		}
		if err = conn.Exec(conn.ctx, statement); err != nil {
			LogError(fmt.Errorf("error while executing %s: %w", statement, err))
			return err
		}
		return nil
	}, conn.ctx, "TruncateTable")
}

func (conn *ClickHouseConnection) InsertBatch(
	fullTableName string,
	rows [][]interface{},
	skipIdx map[int]bool,
	opType WriteBatchOpType,
	async bool,
) error {
	ctx := conn.ctx
	if async {
		ctx = clickhouse.Context(conn.ctx, clickhouse.WithSettings(clickhouse.Settings{
			"async_insert":          1,
			"wait_for_async_insert": 1,
		}))
	}
	return RetryNetError(func() error {
		if len(skipIdx) == len(rows) {
			LogWarn(fmt.Sprintf("[InsertBatch - %s] All rows are skipped for %s", opType, fullTableName))
			return nil
		}
		batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", fullTableName))
		if err != nil {
			LogError(fmt.Errorf("error while preparing batch for %s: %w", fullTableName, err))
			return err
		}
		for i, row := range rows {
			if skipIdx[i] {
				continue
			}
			if err = batch.Append(row...); err != nil {
				return fmt.Errorf("error appending row to a batch for %s: %w", fullTableName, err)
			}
		}
		if err = batch.Send(); err != nil {
			LogError(fmt.Errorf("error while sending batch for %s: %w", fullTableName, err))
			return err
		}
		return nil
	}, ctx, fmt.Sprintf("InsertBatch - %s", opType))
}

func (conn *ClickHouseConnection) GetColumnTypes(schemaName string, tableName string) ([]driver.ColumnType, error) {
	return RetryNetErrorWithData(func() ([]driver.ColumnType, error) {
		query, err := GetColumnTypesQuery(schemaName, tableName)
		if err != nil {
			return nil, err
		}
		rows, err := conn.Query(conn.ctx, query)
		if err != nil {
			LogError(fmt.Errorf("error while executing %s: %w", query, err))
			return nil, err
		}
		defer rows.Close()
		return rows.ColumnTypes(), nil
	}, conn.ctx, "GetColumnTypes")
}

func (conn *ClickHouseConnection) SelectByPrimaryKeys(
	fullTableName string,
	columnTypes []driver.ColumnType,
	pkCols []*PrimaryKeyColumn,
	csv CSV,
) (RowsByPrimaryKeyValue, error) {
	return RetryNetErrorWithData(func() (RowsByPrimaryKeyValue, error) {
		query, err := CSVRowsToSelectQuery(csv, fullTableName, pkCols)
		if err != nil {
			return nil, err
		}
		scanRows := ColumnTypesToEmptyRows(columnTypes, uint32(len(csv)))
		rows, err := conn.Query(conn.ctx, query)
		if err != nil {
			LogError(fmt.Errorf("error while executing %s: %w", query, err))
			return nil, err
		}
		defer rows.Close()
		rowsByPKValues := make(map[string][]interface{})
		for i := 0; rows.Next(); i++ {
			if err = rows.Scan(scanRows[i]...); err != nil {
				return nil, err
			}
			mappingKey, err := GetDatabaseRowMappingKey(scanRows[i], pkCols)
			if err != nil {
				return nil, err
			}
			rowsByPKValues[mappingKey] = scanRows[i]
		}
		return rowsByPKValues, nil
	}, conn.ctx, "SelectByPrimaryKeys")
}

// ReplaceBatch inserts the records from one of "replace" CSV into the table.
// Inserts are done in sequence, `replaceBatchSize` records at a time,
// and the batch size should be relatively high, up to 100K+ records at a time,
// as we don't do any SELECT queries in advance, and we also don't use async_insert feature here.
//
// Any duplicates are handled by ReplacingMergeTree itself (during merges or when using SELECT FINAL),
// so it's safe to retry and not care about inserting the same record several times.
//
// NB: retries are handled by InsertBatch
func (conn *ClickHouseConnection) ReplaceBatch(
	schemaName string,
	table *pb.Table,
	csv CSV,
	nullStr string,
	batchSize uint,
) error {
	fullName, err := GetFullTableName(schemaName, table.Name)
	if err != nil {
		return err
	}
	// Each group here will contain only one slice, as we don't need parallel operations
	// Using it just for consistency with UpdateBatch and SoftDeleteBatch
	groups, err := CalcCSVSlicesGroupsForParallel(uint(len(csv)), batchSize, 1)
	if err != nil {
		return err
	}
	for _, group := range groups {
		for _, slice := range group {
			batch := csv[slice.Start:slice.End]
			insertRows := make([][]interface{}, len(batch))
			for j, csvRow := range batch {
				insertRow, err := CSVRowToInsertValues(csvRow, table, nullStr)
				if err != nil {
					return err
				}
				insertRows[j] = insertRow
			}
			err = conn.InsertBatch(fullName, insertRows, nil, Replace, false)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// UpdateBatch uses one of "update" CSV to insert the updated versions of the records into the table.
//
// Executes operations in groups of batches, i.e. `maxParallelOperations` x `updateBatchSize` records at a time in total.
// `deleteBatchSize` should be relatively low, as we generate a large SELECT statement (see CSVRowsToSelectQuery).
//
// In every batch, we select the records from the table by primary keys, update them in memory, and insert them back.
// If a record is not found in the table, it is skipped (though it should not usually happen).
// If a record is found, it is updated with the new values from the CSV.
// If a CSV column value equals to `unmodifiedStr`, that means that the original value should be preserved.
// If a CSV column value equals to `nullStr`, that means that the column value should be set to NULL.
//
// In the end, ReplacingMergeTree handles the merging of the updated records with their previous versions.
// Any duplicates are also handled by ReplacingMergeTree itself (during merges or when using SELECT FINAL),
// so it's safe to retry and not care about inserting the same record several times.
//
// NB: retries are handled by SelectByPrimaryKeys / InsertBatch for each task within a group separately.
func (conn *ClickHouseConnection) UpdateBatch(
	schemaName string,
	table *pb.Table,
	pkCols []*PrimaryKeyColumn,
	columnTypes []driver.ColumnType,
	csv CSV,
	nullStr string,
	unmodifiedStr string,
	batchSize uint,
	maxParallelOperations uint,
) error {
	fullName, err := GetFullTableName(schemaName, table.Name)
	if err != nil {
		return err
	}
	groups, err := CalcCSVSlicesGroupsForParallel(uint(len(csv)), batchSize, maxParallelOperations)
	if err != nil {
		return err
	}
	for _, group := range groups {
		eg := errgroup.Group{}
		for _, slice := range group {
			s := slice
			eg.Go(func() error {
				batch := csv[s.Start:s.End]
				selectRows, err := conn.SelectByPrimaryKeys(fullName, columnTypes, pkCols, batch)
				if err != nil {
					return err
				}
				insertRows, skipIdx, err := MergeUpdatedRows(batch, selectRows, pkCols, table, nullStr, unmodifiedStr)
				if err != nil {
					return err
				}
				return conn.InsertBatch(fullName, insertRows, skipIdx, Update, true)
			})
		}
		err = eg.Wait()
		if err != nil {
			return err
		}
	}
	return nil
}

// SoftDeleteBatch uses one of "delete" CSV to mark the records as deleted (_fivetran_deleted = True),
// and update their Fivetran sync time (_fivetran_synced) to the CSV row value;
// updated records are inserted into the table as usual.
//
// Executes operations in groups of batches, i.e. `maxParallelOperations` x `deleteBatchSize` records at a time in total.
// `deleteBatchSize` should be relatively low, as we generate a large SELECT statement (see CSVRowsToSelectQuery).
//
// In every batch, we select the records from the table by primary keys, update them in memory, and insert them back.
// If a record is not found in the table, it is skipped (though it should not usually happen).
// If a record is found, `_fivetran_deleted` is set to True,
// and `_fivetran_synced` is updated with a new value from the CSV.
//
// All other columns are left as-is in the original record (CSV does not contain these values, thus we need to SELECT).
//
// In the end, ReplacingMergeTree handles the merging of the "soft deleted" records with their previous versions.
// Any duplicates are also handled by ReplacingMergeTree itself (during merges or when using SELECT FINAL),
// so it's safe to retry and not care about inserting the same record several times.
//
// NB: retries are handled by SelectByPrimaryKeys / InsertBatch for each task within a group separately.
func (conn *ClickHouseConnection) SoftDeleteBatch(
	schemaName string,
	table *pb.Table,
	pkCols []*PrimaryKeyColumn,
	columnTypes []driver.ColumnType,
	csv CSV,
	fivetranSyncedIdx uint,
	fivetranDeletedIdx uint,
	batchSize uint,
	maxParallelOperations uint,
) error {
	fullName, err := GetFullTableName(schemaName, table.Name)
	if err != nil {
		return err
	}
	groups, err := CalcCSVSlicesGroupsForParallel(uint(len(csv)), batchSize, maxParallelOperations)
	if err != nil {
		return err
	}
	for _, group := range groups {
		eg := errgroup.Group{}
		for _, slice := range group {
			s := slice
			eg.Go(func() error {
				batch := csv[s.Start:s.End]
				selectRows, err := conn.SelectByPrimaryKeys(fullName, columnTypes, pkCols, batch)
				if err != nil {
					return err
				}
				insertRows, skipIdx, err := MergeSoftDeletedRows(batch, selectRows, pkCols, fivetranSyncedIdx, fivetranDeletedIdx)
				if err != nil {
					return err
				}
				return conn.InsertBatch(fullName, insertRows, skipIdx, SoftDelete, true)
			})
		}
		err = eg.Wait()
		if err != nil {
			return err
		}
	}
	return nil
}

func (conn *ClickHouseConnection) ConnectionTest() error {
	return RetryNetError(func() error {
		describeResult, err := conn.DescribeTable("system", "numbers")
		if err != nil {
			return err
		}
		colType, exists := describeResult.Mapping["number"]
		if !exists || colType != "UInt64" {
			return fmt.Errorf(
				"unexpected describe system.numbers output, expected result map to contain number:UInt64, got: %v",
				describeResult)
		}
		LogInfo("Connection check passed")
		return nil
	}, conn.ctx, "ConnectionTest")
}

func (conn *ClickHouseConnection) MutationTest() error {
	return RetryNetError(func() error {
		id := strings.Replace(uuid.New().String(), "-", "", -1)
		tableName := fmt.Sprintf("fivetran_destination_test_%s", id)

		// Create test table
		err := conn.CreateTable("", tableName, MakeTableDescription([]*ColumnDefinition{
			{Name: "Col1", Type: "UInt8", IsPrimaryKey: true},
			{Name: "Col2", Type: "String"},
			{Name: FivetranSynced, Type: "DateTime64(9, 'UTC')"},
		}))
		if err != nil {
			return err
		}

		// Insert test Data
		batch, err := conn.PrepareBatch(conn.ctx, fmt.Sprintf("INSERT INTO `%s`", tableName))
		if err != nil {
			LogError(fmt.Errorf("error while preparing a batch for %s: %w", tableName, err))
			return err
		}
		now := time.Now()
		err = batch.Append(uint8(42), "ClickHouse", now)
		if err != nil {
			return fmt.Errorf("error appending row to a batch for %s: %w", tableName, err)
		}
		err = batch.Send()
		if err != nil {
			LogError(fmt.Errorf("error while sending a batch to %s: %w", tableName, err))
			return err
		}

		// Alter
		dbType := "FixedString(10)"
		err = conn.AlterTable("", tableName, []*AlterTableOp{
			{Op: Modify, Column: "Col2", Type: &dbType},
		})
		if err != nil {
			return err
		}

		// Check the inserted Data
		row := conn.QueryRow(conn.ctx, fmt.Sprintf("SELECT * FROM `%s`", tableName))
		var (
			col1 uint8
			col2 string
			col3 *time.Time
		)
		if err = row.Scan(&col1, &col2, &col3); err != nil {
			LogError(fmt.Errorf("error while scanning row from %s: %w", tableName, err))
			return err
		}
		if col1 != uint8(42) || col2 != "ClickHouse" {
			return fmt.Errorf("unexpected Data check output, expected 42/ClickHouse/abc, got: %d/%s/%s", col1, col2, col3)
		}

		// Truncate
		err = conn.TruncateTable("", tableName)
		if err != nil {
			return err
		}

		// Check the table is empty
		row = conn.QueryRow(conn.ctx, fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName))
		var count uint64
		if err = row.Scan(&count); err != nil {
			LogError(fmt.Errorf("error while scanning count from %s: %w", tableName, err))
			return err
		}
		if count != 0 {
			return fmt.Errorf("truncated table count is not zero, got: %d", count)
		}

		// Drop
		if err = conn.Exec(conn.ctx, fmt.Sprintf("DROP TABLE `%s`", tableName)); err != nil {
			LogError(fmt.Errorf("error while dropping table %s: %w", tableName, err))
			return err
		}

		// Check the table does not exist
		row = conn.QueryRow(conn.ctx, fmt.Sprintf("SELECT COUNT(*) FROM system.tables WHERE name = '%s'", tableName))
		if err = row.Scan(&count); err != nil {
			LogError(fmt.Errorf("error while scanning count from system.tables: %w", err))
			return err
		}
		if count != 0 {
			return fmt.Errorf("table %s still exists after drop", tableName)
		}

		LogInfo("Mutation check passed")
		return nil
	}, conn.ctx, "MutationTest")
}
