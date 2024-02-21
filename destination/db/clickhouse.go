package db

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"

	"fivetran.com/fivetran_sdk/destination/common"
	"fivetran.com/fivetran_sdk/destination/common/benchmark"
	"fivetran.com/fivetran_sdk/destination/common/flags"
	"fivetran.com/fivetran_sdk/destination/common/log"
	"fivetran.com/fivetran_sdk/destination/common/retry"
	"fivetran.com/fivetran_sdk/destination/common/types"
	"fivetran.com/fivetran_sdk/destination/db/config"
	"fivetran.com/fivetran_sdk/destination/db/sql"
	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"golang.org/x/sync/errgroup"
)

type ClickHouseConnection struct {
	driver.Conn
}

func GetClickHouseConnection(configuration map[string]string) (*ClickHouseConnection, error) {
	connConfig, err := config.Parse(configuration)
	if err != nil {
		return nil, fmt.Errorf("error while parsing configuration: %w", err)
	}
	settings := clickhouse.Settings{}
	var tlsConfig *tls.Config = nil
	if !connConfig.Local {
		tlsConfig = &tls.Config{InsecureSkipVerify: false}
		// https://clickhouse.com/docs/en/operations/settings/settings#alter-sync
		settings["alter_sync"] = 2
		// https://clickhouse.com/docs/en/operations/settings/settings#mutations_sync
		settings["mutations_sync"] = 2
		// https://clickhouse.com/docs/en/operations/settings/settings#select_sequential_consistency
		settings["select_sequential_consistency"] = 1
	}
	options := &clickhouse.Options{
		Addr: []string{connConfig.Host},
		Auth: clickhouse.Auth{
			Username: connConfig.Username,
			Password: connConfig.Password,
			Database: connConfig.Database,
		},
		Protocol:     clickhouse.Native,
		Settings:     settings,
		MaxOpenConns: int(*flags.MaxOpenConnections),
		MaxIdleConns: int(*flags.MaxIdleConnections),
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct {
				Name    string
				Version string
			}{
				{Name: "fivetran-destination", Version: common.Version},
			},
		},
		TLS: tlsConfig,
	}
	conn, err := clickhouse.Open(options)
	if err != nil {
		err = fmt.Errorf("error while opening a connection to ClickHouse: %w", err)
		log.Error(err)
		return nil, err
	}
	return &ClickHouseConnection{conn}, nil
}

func (conn *ClickHouseConnection) ExecDDL(ctx context.Context, statement string, op connectionOpType) error {
	err := retry.OnNetError(func() error {
		return conn.Exec(ctx, statement)
	}, ctx, string(op), false)
	if err != nil {
		err = fmt.Errorf("error while executing %s: %w", statement, err)
		log.Error(err)
		return err
	}
	return nil
}

func (conn *ClickHouseConnection) ExecQuery(
	ctx context.Context,
	query string,
	op connectionOpType,
	benchmark bool,
) (driver.Rows, error) {
	rows, err := retry.OnNetErrorWithData(func() (driver.Rows, error) {
		return conn.Query(ctx, query)
	}, ctx, string(op), benchmark)
	if err != nil {
		err = fmt.Errorf("error while executing %s: %w", query, err)
		log.Error(err)
		return nil, err
	}
	return rows, nil
}

func (conn *ClickHouseConnection) DescribeTable(
	ctx context.Context,
	schemaName string,
	tableName string,
) (*types.TableDescription, error) {
	query, err := sql.GetDescribeTableQuery(schemaName, tableName)
	if err != nil {
		return nil, err
	}
	rows, err := conn.ExecQuery(ctx, query, describeTable, false)
	if err != nil {
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
	var columns []*types.ColumnDefinition
	for rows.Next() {
		if err = rows.Scan(&colName, &colType, &colComment, &isPrimaryKey, &precision, &scale); err != nil {
			return nil, err
		}
		var decimalParams *pb.DecimalParams = nil
		if precision != nil && scale != nil {
			decimalParams = &pb.DecimalParams{Precision: uint32(*precision), Scale: uint32(*scale)}
		}
		columns = append(columns, &types.ColumnDefinition{
			Name:          colName,
			Type:          colType,
			Comment:       colComment,
			IsPrimaryKey:  isPrimaryKey == 1,
			DecimalParams: decimalParams,
		})
	}
	return types.MakeTableDescription(columns), nil
}

func (conn *ClickHouseConnection) GetColumnTypes(
	ctx context.Context,
	schemaName string,
	tableName string,
) ([]driver.ColumnType, error) {
	query, err := sql.GetColumnTypesQuery(schemaName, tableName)
	if err != nil {
		return nil, err
	}
	rows, err := conn.ExecQuery(ctx, query, getColumnTypes, false)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return rows.ColumnTypes(), nil
}

func (conn *ClickHouseConnection) CreateTable(
	ctx context.Context,
	schemaName string,
	tableName string,
	tableDescription *types.TableDescription,
) (err error) {
	statement, err := sql.GetCreateTableStatement(schemaName, tableName, tableDescription)
	if err != nil {
		return err
	}
	return conn.ExecDDL(ctx, statement, createTable)
}

func (conn *ClickHouseConnection) AlterTable(
	ctx context.Context,
	schemaName string,
	tableName string,
	from *types.TableDescription,
	to *types.TableDescription,
) (err error) {
	ops := GetAlterTableOps(from, to)
	statement, err := sql.GetAlterTableStatement(schemaName, tableName, ops)
	if err != nil {
		return err
	}
	return conn.ExecDDL(ctx, statement, alterTable)
}

func (conn *ClickHouseConnection) TruncateTable(ctx context.Context, schemaName string, tableName string) error {
	statement, err := sql.GetTruncateTableStatement(schemaName, tableName)
	if err != nil {
		return err
	}
	return conn.ExecDDL(ctx, statement, truncateTable)
}

func (conn *ClickHouseConnection) InsertBatch(
	ctx context.Context,
	fullTableName string,
	rows [][]interface{},
	skipIdx map[int]bool,
	opName string,
) error {
	if len(skipIdx) == len(rows) {
		log.Warn(fmt.Sprintf("[%s] All rows are skipped for %s", opName, fullTableName))
		return nil
	}
	return retry.OnNetError(func() error {
		batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", fullTableName))
		if err != nil {
			err = fmt.Errorf("error while preparing batch for %s: %w", fullTableName, err)
			log.Error(err)
			return err
		}
		for i, row := range rows {
			if skipIdx[i] {
				continue
			}
			err = batch.Append(row...)
			if err != nil {
				err = fmt.Errorf("error appending row to a batch for %s: %w", fullTableName, err)
				log.Error(err)
				return err
			}
		}
		err = batch.Send()
		if err != nil {
			err = fmt.Errorf("error while sending batch for %s: %w", fullTableName, err)
			log.Error(err)
			return err
		}
		return nil
	}, ctx, opName, true)
}

// SelectByPrimaryKeys selects rows from the table by primary keys found in the CSV.
// The CSV is split into groups, and each group is processed in parallel.
// The results are merged into a map of primary key values to the rows.
func (conn *ClickHouseConnection) SelectByPrimaryKeys(
	ctx context.Context,
	fullTableName string,
	columnTypes []driver.ColumnType,
	pkCols []*types.PrimaryKeyColumn,
	csv [][]string,
	selectBatchSize uint,
	maxParallelSelects uint,
) (RowsByPrimaryKeyValue, error) {
	return benchmark.RunAndNoticeWithData(func() (RowsByPrimaryKeyValue, error) {
		scanRows := ColumnTypesToEmptyScanRows(columnTypes, uint(len(csv)))
		groups, err := GroupSlices(uint(len(csv)), selectBatchSize, maxParallelSelects)
		if err != nil {
			return nil, err
		}
		var mutex = new(sync.Mutex)
		rowsByPKValues := make(map[string][]interface{}, len(csv))
		for _, group := range groups {
			eg := errgroup.Group{}
			for _, slice := range group {
				ctx := ctx
				s := slice
				eg.Go(func() error {
					batch := csv[s.Start:s.End]
					query, err := sql.GetSelectByPrimaryKeysQuery(batch, fullTableName, pkCols)
					if err != nil {
						return err
					}
					rows, err := conn.ExecQuery(ctx, query, selectByPrimaryKeys, false)
					if err != nil {
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
							// should never happen in practice
							log.Error(fmt.Errorf("primary key mapping collision: %s", mappingKey))
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
	ctx context.Context,
	schemaName string,
	table *pb.Table,
	csv [][]string,
	nullStr string,
	batchSize uint,
) error {
	return benchmark.RunAndNotice(func() error {
		fullName, err := sql.GetQualifiedTableName(schemaName, table.Name)
		if err != nil {
			return err
		}
		groups, err := GroupSlices(uint(len(csv)), batchSize, 1)
		if err != nil {
			return err
		}
		for _, group := range groups {
			for _, slice := range group {
				batch := csv[slice.Start:slice.End]
				insertRows := make([][]interface{}, len(batch))
				for j, csvRow := range batch {
					insertRow, err := ToInsertRow(csvRow, table, nullStr)
					if err != nil {
						return err
					}
					insertRows[j] = insertRow
				}
				err = conn.InsertBatch(ctx, fullName, insertRows, nil, string(insertBatchReplaceTask))
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
	ctx context.Context,
	schemaName string,
	table *pb.Table,
	pkCols []*types.PrimaryKeyColumn,
	columnTypes []driver.ColumnType,
	csv [][]string,
	nullStr string,
	unmodifiedStr string,
	writeBatchSize uint,
	selectBatchSize uint,
	maxParallelSelects uint,
) error {
	return benchmark.RunAndNotice(func() error {
		fullName, err := sql.GetQualifiedTableName(schemaName, table.Name)
		if err != nil {
			return err
		}
		groups, err := GroupSlices(uint(len(csv)), writeBatchSize, 1)
		if err != nil {
			return err
		}
		for _, group := range groups {
			for _, slice := range group {
				batch := csv[slice.Start:slice.End]
				selectRows, err := conn.SelectByPrimaryKeys(ctx, fullName, columnTypes, pkCols, batch, selectBatchSize, maxParallelSelects)
				if err != nil {
					return err
				}
				insertRows, skipIdx, err := MergeUpdatedRows(batch, selectRows, pkCols, table, nullStr, unmodifiedStr)
				if err != nil {
					return err
				}
				err = conn.InsertBatch(ctx, fullName, insertRows, skipIdx, string(insertBatchUpdateTask))
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
	ctx context.Context,
	schemaName string,
	table *pb.Table,
	pkCols []*types.PrimaryKeyColumn,
	columnTypes []driver.ColumnType,
	csv [][]string,
	fivetranSyncedIdx uint,
	fivetranDeletedIdx uint,
	writeBatchSize uint,
	selectBatchSize uint,
	maxParallelSelects uint,
) error {
	return benchmark.RunAndNotice(func() error {
		fullName, err := sql.GetQualifiedTableName(schemaName, table.Name)
		if err != nil {
			return err
		}
		groups, err := GroupSlices(uint(len(csv)), writeBatchSize, 1)
		if err != nil {
			return err
		}
		for _, group := range groups {
			for _, slice := range group {
				batch := csv[slice.Start:slice.End]
				selectRows, err := conn.SelectByPrimaryKeys(ctx, fullName, columnTypes, pkCols, batch, selectBatchSize, maxParallelSelects)
				if err != nil {
					return err
				}
				insertRows, skipIdx, err := MergeSoftDeletedRows(batch, selectRows, pkCols, fivetranSyncedIdx, fivetranDeletedIdx)
				if err != nil {
					return err
				}
				err = conn.InsertBatch(ctx, fullName, insertRows, skipIdx, string(insertBatchDeleteTask))
				if err != nil {
					return err
				}
			}
		}
		return nil
	}, string(insertBatchDelete))
}

func (conn *ClickHouseConnection) ConnectionTest(ctx context.Context) error {
	describeResult, err := conn.DescribeTable(ctx, "system", "numbers")
	if err != nil {
		return err
	}
	col, exists := describeResult.Mapping["number"]
	if !exists || col.Type != "UInt64" {
		return fmt.Errorf(
			"unexpected describe system.numbers output, expected result map to contain number:UInt64, got: %v",
			describeResult)
	}
	log.Info("Connection check passed")
	return nil
}

type connectionOpType string

const (
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
)
