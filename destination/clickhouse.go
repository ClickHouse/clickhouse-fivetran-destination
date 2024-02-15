package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"sync"
	"time"

	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

// ClickHouseConnection
// TODO: on premise cluster setup for DDL
type ClickHouseConnection struct {
	ctx context.Context
	driver.Conn
}

func GetClickHouseConnection(ctx context.Context, configuration map[string]string) (*ClickHouseConnection, error) {
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
		skipVerify := GetWithDefault(configuration, "ssl_skip_verification", "false")
		options.TLS = &tls.Config{InsecureSkipVerify: skipVerify == "true"}
	}
	conn, err := RetryNetErrorWithData(func() (driver.Conn, error) {
		return clickhouse.Open(options)
	}, ctx, string(getConnection), false)
	if err != nil {
		LogError(fmt.Errorf("error while opening a connection to ClickHouse: %w", err))
		return nil, err
	}
	return &ClickHouseConnection{ctx, conn}, nil
}

func (conn *ClickHouseConnection) DescribeTable(schemaName string, tableName string) (*TableDescription, error) {
	query, err := GetDescribeTableQuery(schemaName, tableName)
	if err != nil {
		return nil, err
	}
	rows, err := RetryNetErrorWithData(func() (driver.Rows, error) {
		return conn.Query(conn.ctx, query)
	}, conn.ctx, string(describeTable), false)
	if err != nil {
		LogError(fmt.Errorf("error while executing %s: %w", query, err))
		return nil, err
	}
	defer rows.Close()
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
}

func (conn *ClickHouseConnection) CreateTable(schemaName string, tableName string, tableDescription *TableDescription) error {
	statement, err := GetCreateTableStatement(schemaName, tableName, tableDescription)
	if err != nil {
		return err
	}
	err = RetryNetError(func() error {
		return conn.Exec(conn.ctx, statement)
	}, conn.ctx, string(createTable), false)
	if err != nil {
		LogError(fmt.Errorf("error while executing %s: %w", statement, err))
		return err
	}
	return nil
}

func (conn *ClickHouseConnection) AlterTable(schemaName string, tableName string, ops []*AlterTableOp) error {
	statement, err := GetAlterTableStatement(schemaName, tableName, ops)
	if err != nil {
		return err
	}
	err = RetryNetError(func() error {
		return conn.Exec(conn.ctx, statement)
	}, conn.ctx, string(alterTable), false)
	if err != nil {
		LogError(fmt.Errorf("error while executing %s: %w", statement, err))
		return err
	}
	return nil
}

func (conn *ClickHouseConnection) TruncateTable(schemaName string, tableName string) error {
	statement, err := GetTruncateTableStatement(schemaName, tableName)
	if err != nil {
		return err
	}
	err = RetryNetError(func() error {
		return conn.Exec(conn.ctx, statement)
	}, conn.ctx, string(truncateTable), false)
	if err != nil {
		LogError(fmt.Errorf("error while executing %s: %w", statement, err))
		return err
	}
	return nil
}

func (conn *ClickHouseConnection) InsertBatch(
	fullTableName string,
	rows [][]interface{},
	skipIdx map[int]bool,
	opName string,
) error {
	if len(skipIdx) == len(rows) {
		LogWarn(fmt.Sprintf("[%s] All rows are skipped for %s", opName, fullTableName))
		return nil
	}
	return RetryNetError(func() error {
		batch, err := conn.PrepareBatch(conn.ctx, fmt.Sprintf("INSERT INTO %s", fullTableName))
		if err != nil {
			LogError(fmt.Errorf("error while preparing batch for %s: %w", fullTableName, err))
			return err
		}
		for i, row := range rows {
			if skipIdx[i] {
				continue
			}
			err = batch.Append(row...)
			if err != nil {
				return fmt.Errorf("error appending row to a batch for %s: %w", fullTableName, err)
			}
		}
		err = batch.Send()
		if err != nil {
			LogError(fmt.Errorf("error while sending batch for %s: %w", fullTableName, err))
			return err
		}
		return nil
	}, conn.ctx, opName, true)
}

func (conn *ClickHouseConnection) GetColumnTypes(schemaName string, tableName string) ([]driver.ColumnType, error) {
	query, err := GetColumnTypesQuery(schemaName, tableName)
	if err != nil {
		return nil, err
	}
	rows, err := RetryNetErrorWithData(func() (driver.Rows, error) {
		return conn.Query(conn.ctx, query)
	}, conn.ctx, string(getColumnTypes), false)
	if err != nil {
		LogError(fmt.Errorf("error while executing %s: %w", query, err))
		return nil, err
	}
	defer rows.Close()
	return rows.ColumnTypes(), nil

}

// SelectByPrimaryKeys selects rows from the table by primary keys found in the CSV.
// The CSV is split into groups, and each group is processed in parallel.
// The results are merged into a map of primary key values to the rows.
func (conn *ClickHouseConnection) SelectByPrimaryKeys(
	fullTableName string,
	columnTypes []driver.ColumnType,
	pkCols []*PrimaryKeyColumn,
	csv CSV,
	selectBatchSize uint,
	maxParallelSelects uint,
) (RowsByPrimaryKeyValue, error) {
	return BenchmarkAndNoticeWithData(func() (RowsByPrimaryKeyValue, error) {
		scanRows := ColumnTypesToEmptyRows(columnTypes, uint(len(csv)))
		groups, err := CalcCSVSlicesGroupsForParallel(uint(len(csv)), selectBatchSize, maxParallelSelects)
		if err != nil {
			return nil, err
		}
		var mutex = new(sync.Mutex)
		rowsByPKValues := make(map[string][]interface{}, len(csv))
		for _, group := range groups {
			eg := errgroup.Group{}
			for _, slice := range group {
				s := slice
				eg.Go(func() error {
					batch := csv[s.Start:s.End]
					query, err := CSVRowsToSelectQuery(batch, fullTableName, pkCols)
					if err != nil {
						return err
					}
					rows, err := RetryNetErrorWithData(func() (driver.Rows, error) {
						return conn.Query(conn.ctx, query)
					}, conn.ctx, string(selectByPrimaryKeys), false)
					if err != nil {
						LogError(fmt.Errorf("error while executing %s: %w", query, err))
						return err
					}
					defer rows.Close()
					mutex.Lock()
					defer mutex.Unlock()
					for i := s.Num * selectBatchSize; rows.Next(); i++ {
						if err = rows.Scan(scanRows[i]...); err != nil {
							return err
						}
						mappingKey, err := GetDatabaseRowMappingKey(scanRows[i], pkCols)
						if err != nil {
							return err
						}
						_, ok := rowsByPKValues[mappingKey]
						if ok {
							LogError(fmt.Errorf("primary key mapping collision: %s", mappingKey))
						}
						rowsByPKValues[mappingKey] = scanRows[i]
					}
					return nil
				})
			}
			err = eg.Wait()
			if err != nil {
				return nil, err
			}
		}
		return rowsByPKValues, nil
	}, string(selectByPrimaryKeys))
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
	return BenchmarkAndNotice(func() error {
		fullName, err := GetFullTableName(schemaName, table.Name)
		if err != nil {
			return err
		}
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
				err = conn.InsertBatch(fullName, insertRows, nil, string(insertBatchReplaceTask))
				if err != nil {
					return err
				}
			}
		}
		return nil
	}, string(insertBatchReplace))
}

// UpdateBatch uses one of "update" CSV to insert the updated versions of the records into the table.
//
// Selects rows by PK found in CSV, merges these rows with the CSV values, and inserts them back.
//
// If a record is not found in the table, it is skipped (though it should not usually happen).
// If a CSV column value equals to `unmodifiedStr`, that means that the original value should be preserved.
// If a CSV column value equals to `nullStr`, that means that the column value should be set to NULL.
//
// In the end, ReplacingMergeTree handles the merging of the updated records with their previous versions.
// Any duplicates are also handled by ReplacingMergeTree itself (during merges or when using SELECT FINAL),
// so it's safe to retry and not care about inserting the same record several times.
//
// NB: retries are handled by SelectByPrimaryKeys and InsertBatch.
func (conn *ClickHouseConnection) UpdateBatch(
	schemaName string,
	table *pb.Table,
	pkCols []*PrimaryKeyColumn,
	columnTypes []driver.ColumnType,
	csv CSV,
	nullStr string,
	unmodifiedStr string,
	writeBatchSize uint,
	selectBatchSize uint,
	maxParallelSelects uint,
) error {
	return BenchmarkAndNotice(func() error {
		fullName, err := GetFullTableName(schemaName, table.Name)
		if err != nil {
			return err
		}
		groups, err := CalcCSVSlicesGroupsForParallel(uint(len(csv)), writeBatchSize, 1)
		if err != nil {
			return err
		}
		selectRows, err := conn.SelectByPrimaryKeys(fullName, columnTypes, pkCols, csv, selectBatchSize, maxParallelSelects)
		if err != nil {
			return err
		}
		for _, group := range groups {
			for _, slice := range group {
				batch := csv[slice.Start:slice.End]
				insertRows, skipIdx, err := MergeUpdatedRows(batch, selectRows, pkCols, table, nullStr, unmodifiedStr)
				if err != nil {
					return err
				}
				err = conn.InsertBatch(fullName, insertRows, skipIdx, string(insertBatchUpdateTask))
				if err != nil {
					return err
				}
			}
		}
		return nil
	}, string(insertBatchUpdate))
}

// SoftDeleteBatch uses one of "delete" CSV to mark the records as deleted (_fivetran_deleted = True),
// and update their Fivetran sync time (_fivetran_synced) to the CSV row value.
//
// Selects rows by PK found in CSV, merges these rows with the CSV values, and inserts them back.
//
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
// NB: retries are handled by SelectByPrimaryKeys and InsertBatch.
func (conn *ClickHouseConnection) SoftDeleteBatch(
	schemaName string,
	table *pb.Table,
	pkCols []*PrimaryKeyColumn,
	columnTypes []driver.ColumnType,
	csv CSV,
	fivetranSyncedIdx uint,
	fivetranDeletedIdx uint,
	writeBatchSize uint,
	selectBatchSize uint,
	maxParallelSelects uint,
) error {
	return BenchmarkAndNotice(func() error {
		fullName, err := GetFullTableName(schemaName, table.Name)
		if err != nil {
			return err
		}
		groups, err := CalcCSVSlicesGroupsForParallel(uint(len(csv)), writeBatchSize, 1)
		if err != nil {
			return err
		}
		selectRows, err := conn.SelectByPrimaryKeys(fullName, columnTypes, pkCols, csv, selectBatchSize, maxParallelSelects)
		if err != nil {
			return err
		}
		for _, group := range groups {
			for _, slice := range group {
				batch := csv[slice.Start:slice.End]
				insertRows, skipIdx, err := MergeSoftDeletedRows(batch, selectRows, pkCols, fivetranSyncedIdx, fivetranDeletedIdx)
				if err != nil {
					return err
				}
				err = conn.InsertBatch(fullName, insertRows, skipIdx, string(insertBatchDeleteTask))
				if err != nil {
					return err
				}
			}
		}
		return nil
	}, string(insertBatchDelete))
}

func (conn *ClickHouseConnection) ConnectionTest() error {
	describeResult, err := RetryNetErrorWithData(func() (*TableDescription, error) {
		return conn.DescribeTable("system", "numbers")
	}, conn.ctx, string(connectionTest), false)
	if err != nil {
		return err
	}
	col, exists := describeResult.Mapping["number"]
	if !exists || col.Type != "UInt64" {
		return fmt.Errorf(
			"unexpected describe system.numbers output, expected result map to contain number:UInt64, got: %v",
			describeResult)
	}
	LogInfo("Connection check passed")
	return nil
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
	}, conn.ctx, string(mutationTest), false)
}

type connectionOpType string

const (
	getConnection          connectionOpType = "GetClickHouseConnection"
	describeTable          connectionOpType = "DescribeTable"
	createTable            connectionOpType = "CreateTable"
	alterTable             connectionOpType = "AlterTable"
	truncateTable          connectionOpType = "TruncateTable"
	insertBatchReplace     connectionOpType = "InsertBatch(Replace)"
	insertBatchReplaceTask connectionOpType = "InsertBatch(Replace task)"
	insertBatchUpdate      connectionOpType = "InsertBatch(Update)"
	insertBatchUpdateTask  connectionOpType = "InsertBatch(Update task)"
	insertBatchDelete      connectionOpType = "InsertBatch(Delete)"
	insertBatchDeleteTask  connectionOpType = "InsertBatch(Delete task)"
	getColumnTypes         connectionOpType = "GetColumnTypes"
	selectByPrimaryKeys    connectionOpType = "SelectByPrimaryKeys"
	connectionTest         connectionOpType = "ConnectionTest"
	mutationTest           connectionOpType = "MutationTest"
)
