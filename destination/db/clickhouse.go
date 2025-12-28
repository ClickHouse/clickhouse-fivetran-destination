package db

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"fivetran.com/fivetran_sdk/destination/common"
	"fivetran.com/fivetran_sdk/destination/common/benchmark"
	"fivetran.com/fivetran_sdk/destination/common/constants"
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

const (
	maxQueryLengthForLogging = 200
)

type ClickHouseConnection struct {
	driver.Conn
	username      string
	isLocal       bool
	connectTime   time.Time
	lastUsed      time.Time
	queryCount    int64
	errorCount    int64
	totalDuration time.Duration
}

func (conn *ClickHouseConnection) logConnectionStats() {
	log.Info(fmt.Sprintf("Connection stats - Queries: %d, Errors: %d, Avg Duration: %v",
		conn.queryCount,
		conn.errorCount,
		conn.totalDuration/time.Duration(conn.queryCount+1)))
}

func (conn *ClickHouseConnection) recordQuery(duration time.Duration, success bool) {
	conn.queryCount++
	conn.totalDuration += duration
	if !success {
		conn.errorCount++
	}
	// Log stats every 100 queries
	if conn.queryCount%100 == 0 {
		conn.logConnectionStats()
	}
}

func GetClickHouseConnection(ctx context.Context, configuration map[string]string) (*ClickHouseConnection, error) {
	connConfig, err := config.Parse(configuration)
	if err != nil {
		return nil, fmt.Errorf("error while parsing configuration: %w", err)
	}

	log.Info(fmt.Sprintf("Initializing ClickHouse connection to %s:%s",
		configuration[config.HostKey], configuration[config.PortKey]))

	settings := clickhouse.Settings{
		// support ISO DateTime formats from CSV
		// https://clickhouse.com/docs/en/operations/settings/formats#date_time_input_format
		"date_time_input_format": "best_effort",
	}
	var tlsConfig *tls.Config = nil
	if !connConfig.Local {
		tlsConfig = &tls.Config{InsecureSkipVerify: false}
		// https://clickhouse.com/docs/en/operations/settings/settings#alter-sync
		// https://github.com/ClickHouse/clickhouse-private/pull/12617
		settings["alter_sync"] = 3
		// https://clickhouse.com/docs/en/operations/settings/settings#mutations_sync
		// https://github.com/ClickHouse/clickhouse-private/pull/12617
		settings["mutations_sync"] = 3
		// https://clickhouse.com/docs/en/operations/settings/settings#select_sequential_consistency
		settings["select_sequential_consistency"] = 1
	}
	addr := fmt.Sprintf("%s:%d", connConfig.Host, connConfig.Port)
	options := &clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Username: connConfig.Username,
			Password: connConfig.Password,
			Database: "system",
		},
		Protocol:     clickhouse.Native,
		Settings:     settings,
		MaxOpenConns: int(*flags.MaxOpenConnections),
		MaxIdleConns: int(*flags.MaxIdleConnections),
		ReadTimeout:  *flags.RequestTimeoutDuration,
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
	err = retry.OnNetError(func() error {
		return conn.Ping(ctx)
	}, ctx, "ping", false)
	if err != nil {
		err = fmt.Errorf("ClickHouse connection error: %w", err)
		log.Error(err)
		return nil, err
	}
	log.Info("ClickHouse connection established successfully")
	return &ClickHouseConnection{Conn: conn, username: connConfig.Username, isLocal: connConfig.Local}, nil
}

func (conn *ClickHouseConnection) ExecStatement(
	ctx context.Context,
	statement string,
	op connectionOpType,
	benchmark bool,
) error {
	startTime := time.Now()
	logQuery := statement
	if len(logQuery) > maxQueryLengthForLogging {
		logQuery = statement[:maxQueryLengthForLogging] + "..."
	}

	log.Info(fmt.Sprintf("Executing %s: %s", op, logQuery))
	err := retry.OnNetError(func() error {
		return conn.Exec(ctx, statement)
	}, ctx, string(op), benchmark)
	conn.recordQuery(time.Since(startTime), err == nil)
	if err != nil {
		err = fmt.Errorf("error while executing %s: %w", statement, err)
		log.Error(err)
		return err
	}
	log.Info(fmt.Sprintf("Successfully executed %s in %v", op, time.Since(startTime)))
	return nil
}

func (conn *ClickHouseConnection) ExecQuery(
	ctx context.Context,
	query string,
	op connectionOpType,
	benchmark bool,
) (driver.Rows, error) {
	startTime := time.Now()
	logQuery := query
	if len(logQuery) > maxQueryLengthForLogging {
		logQuery = query[:maxQueryLengthForLogging] + "..."
	}

	log.Info(fmt.Sprintf("Executing query %s: %s", op, logQuery))
	rows, err := retry.OnNetErrorWithData(func() (driver.Rows, error) {
		return conn.Query(ctx, query)
	}, ctx, string(op), benchmark)
	conn.recordQuery(time.Since(startTime), err == nil)
	if err != nil {
		err = fmt.Errorf("error while executing %s: %w", query, err)
		log.Error(err)
		return nil, err
	}
	log.Info(fmt.Sprintf("Query %s completed in %v", op, time.Since(startTime)))
	return rows, nil
}

// ExecBoolQuery
// using ExecQuery, queries a single row with a single boolean value
func (conn *ClickHouseConnection) ExecBoolQuery(
	ctx context.Context,
	query string,
	op connectionOpType,
	benchmark bool,
) (bool, error) {
	rows, err := conn.ExecQuery(ctx, query, op, benchmark)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return false, fmt.Errorf("unexpected empty result from %s", query)
	}
	var result bool
	if err = rows.Scan(&result); err != nil {
		return false, err
	}
	return result, nil
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
		if hasDecimalPrefix(colType) && precision != nil && scale != nil {
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

// GetColumnTypes
// returns the information about the table columns as reported by the driver;
// columns have the same order as in the ClickHouse table definition.
// It is used to determine the scan types of the rows that we will insert into the table,
// as well as validate the CSV header and build a proper mapping of CSV -> database columns indices.
func (conn *ClickHouseConnection) GetColumnTypes(
	ctx context.Context,
	schemaName string,
	tableName string,
) ([]driver.ColumnType, error) {
	query, err := sql.GetColumnTypesQuery(schemaName, tableName)
	if err != nil {
		return nil, err
	}
	rows, err := conn.ExecQuery(ctx, query, getColumnTypesWithIndexMap, false)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return rows.ColumnTypes(), nil
}

func (conn *ClickHouseConnection) GetUserGrants(ctx context.Context) ([]*types.UserGrant, error) {
	query, err := sql.GetSelectFromSystemGrantsQuery(conn.username)
	if err != nil {
		return nil, err
	}
	rows, err := conn.ExecQuery(ctx, query, getUserGrants, false)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var (
		accessType string
		database   *string
		table      *string
		column     *string
	)
	grants := make([]*types.UserGrant, 0)
	for rows.Next() {
		if err = rows.Scan(&accessType, &database, &table, &column); err != nil {
			return nil, err
		}
		grants = append(grants, &types.UserGrant{
			AccessType: accessType,
			Database:   database,
			Table:      table,
			Column:     column,
		})
	}
	return grants, nil
}

func (conn *ClickHouseConnection) CheckDatabaseExists(
	ctx context.Context,
	schemaName string,
) (bool, error) {
	statement, err := sql.GetCheckDatabaseExistsStatement(schemaName)
	if err != nil {
		return false, err
	}
	rows, err := conn.ExecQuery(ctx, statement, checkDatabaseExists, false)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	var count uint64
	if !rows.Next() {
		return false, fmt.Errorf("unexpected empty result from %s", statement)
	}
	if err = rows.Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func (conn *ClickHouseConnection) CreateDatabase(
	ctx context.Context,
	schemaName string,
) error {
	statement, err := sql.GetCreateDatabaseStatement(schemaName)
	if err != nil {
		return err
	}
	err = conn.ExecStatement(ctx, statement, createDatabase, false)
	if err != nil {
		waitErr := conn.WaitDatabaseIsCreated(ctx, err, schemaName)
		if waitErr != nil {
			return waitErr
		}
		return nil
	}
	return nil
}

// CreateTable will additionally create a database if it does not exist yet.
// It is done since we don't always know the name of the "schema" that a particular connector might use.
func (conn *ClickHouseConnection) CreateTable(
	ctx context.Context,
	schemaName string,
	tableName string,
	tableDescription *types.TableDescription,
) error {
	databaseExists, err := conn.CheckDatabaseExists(ctx, schemaName)
	if err != nil {
		return err
	}
	if !databaseExists {
		err = conn.CreateDatabase(ctx, schemaName)
		if err != nil {
			return err
		}
	}
	statement, err := sql.GetCreateTableStatement(schemaName, tableName, tableDescription)
	if err != nil {
		return err
	}
	return conn.ExecStatement(ctx, statement, createTable, false)
}

// AlterTable will not execute any statements if both table definitions are identical.
func (conn *ClickHouseConnection) AlterTable(
	ctx context.Context,
	schemaName string,
	tableName string,
	from *types.TableDescription,
	to *types.TableDescription,
) (wasExecuted bool, err error) {
	ops, hasChangedPK, unchangedColNames, err := GetAlterTableOps(from, to)
	if err != nil {
		return false, err
	}
	// even though we set alter/mutations_sync=3, we check for all nodes availability and log warning if not all nodes are available
	err = conn.WaitAllNodesAvailable(ctx, schemaName, tableName)
	if err != nil {
		log.Warn(fmt.Sprintf("It seems like not all nodes are available: %v. We strongly recommend to check the cluster health and availability to avoid incosistency between replicas", err))
	}
	if hasChangedPK {
		unixMilli := time.Now().UnixMilli()
		newTableName := fmt.Sprintf("%s_new_%d", tableName, unixMilli)
		backupTableName := fmt.Sprintf("%s_backup_%d", tableName, unixMilli)
		log.Info(fmt.Sprintf("AlterTable with PK change detected; backup table name: %s, new table name: %s",
			backupTableName, newTableName))
		createTableStmt, err := sql.GetCreateTableStatement(schemaName, newTableName, to)
		if err != nil {
			return false, err
		}
		err = conn.ExecStatement(ctx, createTableStmt, alterTablePKCreateTable, false)
		if err != nil {
			return false, err
		}
		if len(unchangedColNames) > 0 {
			insertStmt, err := sql.GetInsertFromSelectStatement(schemaName, tableName, newTableName, unchangedColNames)
			if err != nil {
				return false, err
			}
			err = conn.ExecStatement(ctx, insertStmt, alterTablePKInsert, true)
			if err != nil {
				return false, err
			}
		}
		// two statements cause:
		// "Database ... is Replicated, it does not support renaming of multiple tables in single query"
		// from current table to the "backup" table, which will be not dropped
		err = conn.RenameTable(ctx, schemaName, tableName, backupTableName)
		if err != nil {
			return false, err
		}
		// from the new table to the resulting table with the initial name
		err = conn.RenameTable(ctx, schemaName, newTableName, tableName)
		if err != nil {
			return false, err
		}
	} else {
		if len(ops) == 0 {
			return false, nil
		}
		statement, err := sql.GetAlterTableStatement(schemaName, tableName, ops)
		if err != nil {
			return false, err
		}
		err = conn.ExecStatement(ctx, statement, alterTable, true)
		if err != nil {
			waitErr := conn.WaitAllMutationsCompleted(ctx, err, schemaName, tableName)
			if waitErr != nil {
				return false, waitErr
			}
			return true, nil
		}
	}
	return true, nil
}

func (conn *ClickHouseConnection) RenameTable(
	ctx context.Context,
	schemaName string,
	fromTableName string,
	toTableName string,
) error {
	renameStmt, err := sql.GetRenameTableStatement(schemaName, fromTableName, toTableName)
	if err != nil {
		return err
	}
	err = conn.ExecStatement(ctx, renameStmt, alterTablePKRename, false)
	if err != nil {
		return err
	}
	return nil
}

// TruncateTable
// softDeletedColumn switches between "hard" (nil) and "soft" (not nil) truncation (see sql.GetTruncateTableStatement)
func (conn *ClickHouseConnection) TruncateTable(
	ctx context.Context,
	schemaName string,
	tableName string,
	syncedColumn string,
	truncateBefore time.Time,
	softDeletedColumn *string,
) error {
	statement, err := sql.GetTruncateTableStatement(schemaName, tableName, syncedColumn, truncateBefore, softDeletedColumn)
	if err != nil {
		return err
	}
	var op connectionOpType
	if softDeletedColumn == nil {
		op = hardTruncateTable
	} else {
		op = softTruncateTable
	}
	// even though we set alter/mutations_sync=3, we check for all nodes availability and log warning if not all nodes are available
	err = conn.WaitAllNodesAvailable(ctx, schemaName, tableName)
	if err != nil {
		log.Warn(fmt.Sprintf("It seems like not all nodes are available: %v. We strongly recommend to check the cluster health and availability to avoid incosistency between replicas", err))
	}
	err = conn.ExecStatement(ctx, statement, op, true)
	if err != nil {
		waitErr := conn.WaitAllMutationsCompleted(ctx, err, schemaName, tableName)
		if waitErr != nil {
			return waitErr
		}
		return nil
	}
	return nil
}

func (conn *ClickHouseConnection) DropTable(
	ctx context.Context,
	qualifiedTableName sql.QualifiedTableName,
) error {
	statement, err := sql.GetDropTableStatement(qualifiedTableName)
	if err != nil {
		return err
	}
	return conn.ExecStatement(ctx, statement, dropTable, false)
}

func (conn *ClickHouseConnection) InsertBatch(
	ctx context.Context,
	qualifiedTableName sql.QualifiedTableName,
	rows [][]interface{},
	skipIdx map[int]bool,
	opName string,
) error {
	if len(skipIdx) == len(rows) {
		log.Warn(fmt.Sprintf("[%s] All rows are skipped for %s", opName, qualifiedTableName))
		return nil
	}
	return retry.OnNetError(func() error {
		batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", qualifiedTableName))
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				err = fmt.Errorf("error while preparing batch for %s: %w (context state: %v)", qualifiedTableName, err, ctx.Err())
			} else {
				err = fmt.Errorf("error while preparing batch for %s: %w", qualifiedTableName, err)
			}
			log.Error(err)
			return err
		}
		for i, row := range rows {
			if skipIdx[i] {
				continue
			}
			err = batch.Append(row...)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					err = fmt.Errorf("error appending row to a batch for %s: %w (context state: %v)", qualifiedTableName, err, ctx.Err())
				} else {
					err = fmt.Errorf("error appending row to a batch for %s: %w", qualifiedTableName, err)
				}
				log.Error(err)
				return err
			}
		}
		err = batch.Send()
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				err = fmt.Errorf("error while sending batch for %s: %w (context state: %v)", qualifiedTableName, err, ctx.Err())
			} else {
				err = fmt.Errorf("error while sending batch for %s: %w", qualifiedTableName, err)
			}
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
	qualifiedTableName sql.QualifiedTableName,
	driverColumns *types.DriverColumns,
	csvCols *types.CSVColumns,
	csv [][]string,
	isHistoryMode bool,
) (RowsByPrimaryKeyValue, error) {
	return benchmark.RunAndNoticeWithData(func() (RowsByPrimaryKeyValue, error) {
		scanRows := ColumnTypesToEmptyScanRows(driverColumns, uint(len(csv)))
		groups, err := GroupSlices(uint(len(csv)), *flags.SelectBatchSize, *flags.MaxParallelSelects)
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
					query, err := sql.GetSelectByPrimaryKeysQuery(batch, csvCols, qualifiedTableName, isHistoryMode)
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
					for i := s.Num * (*flags.SelectBatchSize); rows.Next(); i++ {
						if err = rows.Scan(scanRows[i]...); err != nil {
							return err
						}
						rowMappingKey, err := GetDatabaseRowMappingKey(scanRows[i], csvCols)
						if err != nil {
							return err
						}
						_, ok := rowsByPKValues[rowMappingKey]
						if ok {
							// should never happen in practice
							log.Error(fmt.Errorf("primary key mapping collision: %s", rowMappingKey))
						}
						rowsByPKValues[rowMappingKey] = scanRows[i]
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
	csvColumns *types.CSVColumns,
	nullStr string,
) error {
	return benchmark.RunAndNotice(func() error {
		qualifiedTableName, err := sql.GetQualifiedTableName(schemaName, table.Name)
		if err != nil {
			return err
		}
		groups, err := GroupSlices(uint(len(csv)), *flags.WriteBatchSize, 1)
		if err != nil {
			return err
		}
		for _, group := range groups {
			for _, slice := range group {
				batch := csv[slice.Start:slice.End]
				insertRows := make([][]interface{}, len(batch))
				for j, csvRow := range batch {
					insertRow, err := ToInsertRow(csvRow, csvColumns, nullStr)
					if err != nil {
						return err
					}
					insertRows[j] = insertRow
				}
				err = conn.InsertBatch(ctx, qualifiedTableName, insertRows, nil, string(insertBatchReplaceTask))
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
	driverColumns *types.DriverColumns,
	csvColumns *types.CSVColumns,
	csv [][]string,
	nullStr string,
	unmodifiedStr string,
	isHistoryMode bool,
) error {
	return benchmark.RunAndNotice(func() error {
		qualifiedTableName, err := sql.GetQualifiedTableName(schemaName, table.Name)
		if err != nil {
			return err
		}
		groups, err := GroupSlices(uint(len(csv)), *flags.WriteBatchSize, 1)
		if err != nil {
			return err
		}
		for _, group := range groups {
			for _, slice := range group {
				batch := csv[slice.Start:slice.End]
				selectRows, err := conn.SelectByPrimaryKeys(ctx, qualifiedTableName, driverColumns, csvColumns, batch, isHistoryMode)
				if err != nil {
					return err
				}
				insertRows, skipIdx, err := MergeUpdatedRows(batch, selectRows, csvColumns, nullStr, unmodifiedStr, isHistoryMode)
				if err != nil {
					return err
				}
				err = conn.InsertBatch(ctx, qualifiedTableName, insertRows, skipIdx, string(insertBatchUpdateTask))
				if err != nil {
					return err
				}
			}
		}
		return nil
	}, string(insertBatchUpdate))
}

// HardDelete is called when processing "delete" CSVs.
// Uses lightweight deletes to remove records from the table.
// See also: sql.GetHardDeleteStatement
func (conn *ClickHouseConnection) HardDelete(
	ctx context.Context,
	schemaName string,
	table *pb.Table,
	csv [][]string,
	csvColumns *types.CSVColumns,
) error {
	return benchmark.RunAndNotice(func() error {
		qualifiedTableName, err := sql.GetQualifiedTableName(schemaName, table.Name)
		if err != nil {
			return err
		}
		groups, err := GroupSlices(uint(len(csv)), *flags.HardDeleteBatchSize, 1)
		if err != nil {
			return err
		}
		for _, group := range groups {
			for _, slice := range group {
				batch := csv[slice.Start:slice.End]
				statement, err := sql.GetHardDeleteStatement(batch, csvColumns, qualifiedTableName)
				if err != nil {
					return err
				}
				err = conn.ExecStatement(ctx, statement, insertBatchHardDeleteTask, true)
				if err != nil {
					return err
				}
			}
		}
		return nil
	}, string(insertBatchHardDelete))
}

// HardDeleteWithTimestamp is similar to HardDelete but includes a timestamp condition
// for each row, combining primary key equality checks with a timestamp comparison.
// This is useful for deleting records that match both the primary key and a timestamp threshold.
// See also: sql.GetHardDeleteWithTimestampStatement
func (conn *ClickHouseConnection) HardDeleteForEarliestStartHistory(
	ctx context.Context,
	schemaName string,
	table *pb.Table,
	csv [][]string,
	csvColumns *types.CSVColumns,
) error {
	return benchmark.RunAndNotice(func() error {
		qualifiedTableName, err := sql.GetQualifiedTableName(schemaName, table.Name)
		if err != nil {
			return err
		}

		// Find the _fivetran_start column index and type
		fivetranStartIndex, fivetranStartType, err := findColumnInCSV(csvColumns, constants.FivetranStart)
		if err != nil {
			return err
		}

		groups, err := GroupSlices(uint(len(csv)), *flags.HardDeleteBatchSize, 1)
		if err != nil {
			return err
		}
		for _, group := range groups {
			for _, slice := range group {
				batch := csv[slice.Start:slice.End]
				statement, err := sql.GetHardDeleteWithTimestampStatement(
					batch,
					csvColumns,
					qualifiedTableName,
					constants.FivetranStart,
					fivetranStartIndex,
					fivetranStartType,
				)
				if err != nil {
					return err
				}
				err = conn.ExecStatement(ctx, statement, insertBatchHardDeleteTask, true)
				if err != nil {
					return err
				}
			}
		}
		return nil
	}, string(insertBatchHardDelete))
}

// WriteHistoryBatch updates history records by setting _fivetran_active to FALSE and
// _fivetran_end to the timestamp from the CSV (typically _fivetran_start - 1).
// This is used for history mode tables to close out existing active records when new versions arrive.
//
// The CSV should contain:
// - Primary key columns to identify which records to update
// - An "end timestamp" column (typically calculated as _fivetran_start - 1 of the new record)
//
// Example UPDATE generated:
//
//	ALTER TABLE schema.table UPDATE
//	  _fivetran_active = FALSE,
//	  _fivetran_end = CASE
//	    WHEN id = 1 THEN T1 - 1
//	    WHEN id = 2 THEN T2 - 1
//	  END
//	WHERE id IN (1, 2) AND _fivetran_active = TRUE
//
// See also: sql.GetUpdateHistoryActiveStatement
func (conn *ClickHouseConnection) UpdateForEarliestStartHistory(
	ctx context.Context,
	schemaName string,
	table *pb.Table,
	csv [][]string,
	csvColumns *types.CSVColumns,
	fivetranStartColumnName string,
) error {
	return benchmark.RunAndNotice(func() error {
		qualifiedTableName, err := sql.GetQualifiedTableName(schemaName, table.Name)
		if err != nil {
			return err
		}

		// Find the _fivetran_start column index and type
		fivetranStartColumnIndex, fivetranStartColumnType, err := findColumnInCSV(csvColumns, fivetranStartColumnName)
		if err != nil {
			return err
		}

		// even though we set alter/mutations_sync=3, we check for all nodes availability and log warning if not all nodes are available
		err = conn.WaitAllNodesAvailable(ctx, schemaName, table.Name)
		if err != nil {
			log.Warn(fmt.Sprintf("It seems like not all nodes are available: %v. We strongly recommend to check the cluster health and availability to avoid incosistency between replicas", err))
		}

		groups, err := GroupSlices(uint(len(csv)), *flags.WriteBatchSize, 1)
		if err != nil {
			return err
		}

		for _, group := range groups {
			for _, slice := range group {
				batch := csv[slice.Start:slice.End]
				statement, err := sql.GetUpdateHistoryActiveStatement(
					batch,
					csvColumns,
					qualifiedTableName,
					fivetranStartColumnIndex,
					fivetranStartColumnType,
				)
				if err != nil {
					return err
				}
				err = conn.ExecStatement(ctx, statement, updateHistoryBatch, true)
				if err != nil {
					// Wait for mutations to complete if there's an error
					waitErr := conn.WaitAllMutationsCompleted(ctx, err, schemaName, table.Name)
					if waitErr != nil {
						return waitErr
					}
					return nil
				}
			}
		}
		return nil
	}, string(updateHistoryBatch))
}

// findColumnInCSV searches for a column by name in csvColumns and returns its index and type.
// Returns an error if the column is not found.
func findColumnInCSV(csvColumns *types.CSVColumns, columnName string) (uint, pb.DataType, error) {
	if csvColumns == nil || csvColumns.All == nil {
		return 0, pb.DataType_UNSPECIFIED, fmt.Errorf("csvColumns is nil or empty")
	}

	for _, col := range csvColumns.All {
		if col.Name == columnName {
			return col.Index, col.Type, nil
		}
	}

	return 0, pb.DataType_UNSPECIFIED, fmt.Errorf("column %s not found in CSV columns", columnName)
}

// WaitAllNodesAvailable
// Before alter/mutation_sync=3 were introduced, we used to check and wait for all replicas to be active, in order to avoid errors like: using the query generated by sql.GetAllReplicasActiveQuery, and retrying it until all the replicas are active,
//
//code: 341, message: Mutation is not finished because some replicas are inactive right now
//
// We keep this check for monitoring reasons.

func (conn *ClickHouseConnection) WaitAllNodesAvailable(
	ctx context.Context,
	schemaName string,
	tableName string,
) error {
	// disable this check with the local ClickHouse in a Docker; the result will be always empty there
	if conn.isLocal {
		return nil
	}

	query, err := sql.GetAllReplicasActiveQuery(schemaName, tableName)
	if err != nil {
		return err
	}

	// Measure the total execution (or, more precisely, waiting) time of all the operations here
	err = benchmark.RunAndNotice(func() error {
		return retry.OnFalseWithFixedDelay(func() (bool, error) {
			allActive, err := conn.ExecBoolQuery(ctx, query, allReplicasActive, false)
			if err != nil {
				return false, err
			}
			return allActive, nil
		}, ctx, query, *flags.MaxInactiveReplicaCheckRetries, *flags.InactiveReplicaCheckInterval)
	}, string(allReplicasActive))

	if err != nil {
		return fmt.Errorf("error while waiting for all nodes to be available: %w; please verify that all nodes in the cluster are running and healthy (including read-only replicas)", err)
	}
	return nil
}

// WaitAllMutationsCompleted
// if mutation_sync=3 and alter_sync=3 was not enough,
// and one of the nodes went down exactly at the time of the ALTER TABLE statement execution,
// we will still get the error code 341, which indicates that the mutations will still be completed asynchronously;
// wait until all the nodes are available again, and all mutations are completed before sending the response.
func (conn *ClickHouseConnection) WaitAllMutationsCompleted(
	ctx context.Context,
	mutationError error,
	schemaName string,
	tableName string,
) error {
	// disable this check with the local ClickHouse in a Docker; the result will be always empty there
	if conn.isLocal || !isIncompleteMutationErr(mutationError) {
		return mutationError
	}
	// even though we set alter/mutations_sync=3, we check for all nodes availability and log warning if not all nodes are available
	err := conn.WaitAllNodesAvailable(ctx, schemaName, tableName)
	if err != nil {
		log.Warn(fmt.Sprintf("It seems like not all nodes are available: %v. We strongly recommend to check the cluster health and availability to avoid incosistency between replicas", err))
	}

	query, err := sql.GetAllMutationsCompletedQuery(schemaName, tableName)
	if err != nil {
		return fmt.Errorf("error while generating the mutations status query: %w; initial cause: %w", err, mutationError)
	}

	// Measure the total execution (or, more precisely, waiting) time of all the operations here
	err = benchmark.RunAndNotice(func() error {
		return retry.OnFalseWithFixedDelay(func() (bool, error) {
			allCompleted, err := conn.ExecBoolQuery(ctx, query, allMutationsCompleted, false)
			if err != nil {
				return false, err
			}
			return allCompleted, nil
		}, ctx, query, *flags.MaxAsyncMutationsCheckRetries, *flags.AsyncMutationsCheckInterval)
	}, string(allMutationsCompleted))

	if err != nil {
		return fmt.Errorf("error while waiting for all mutations to be completed: %w; initial cause: %w", err, mutationError)
	}
	return nil
}

// WaitDatabaseIsCreated
// if there are parallel requests to create tables in a particular database which does not exist yet,
// and some of these requests will get "false" on database existence check,
// each of these requests will try to create the database by itself.
// Despite having NOT EXISTS modifier on the database creation statement,
// we could still _rarely_ get a "Database ... is currently dropped or renamed" error (code 81).
//
// As there is no guarantee that there will be only one instance of the app running at a time, we can't use a mutex.
// Instead, we will try to wait until the database is created (as it is likely being created by some other request).
//
// NB: another (more robust) option is to use a distributed lock (maybe via a KeeperMap table engine),
// but since this error is very rare, it is probably not worth to overcomplicate.
func (conn *ClickHouseConnection) WaitDatabaseIsCreated(
	ctx context.Context,
	mutationError error,
	schemaName string,
) error {
	if !isDatabaseBeingCreatedErr(mutationError) {
		return mutationError
	}

	// Measure the total execution (or, more precisely, waiting) time of all the operations here
	err := benchmark.RunAndNotice(func() error {
		return retry.OnFalseWithFixedDelay(func() (bool, error) {
			dbExists, err := conn.CheckDatabaseExists(ctx, schemaName)
			if err != nil {
				return false, err
			}
			return dbExists, nil
		}, ctx, string(waitDatabaseIsCreated), *flags.MaxDatabaseCreatedCheckRetries, *flags.DatabaseCreatedCheckInterval)
	}, string(waitDatabaseIsCreated))

	if err != nil {
		return fmt.Errorf("error while waiting for the database %s to be created: %w; initial cause: %w",
			schemaName, err, mutationError)
	}
	return nil
}

func (conn *ClickHouseConnection) ConnectionTest(ctx context.Context) error {
	rows, err := conn.ExecQuery(ctx, "SELECT toInt8(42) AS fivetran_connection_check", connectionTest, false)
	if err != nil {
		return err
	}
	var result int8
	if !rows.Next() {
		return fmt.Errorf("unexpected empty result from the connection check query")
	}
	if err = rows.Scan(&result); err != nil {
		return err
	}
	if result != 42 {
		return fmt.Errorf("unexpected result from the connection check query: %d", result)
	}
	return nil
}

func (conn *ClickHouseConnection) GrantsTest(ctx context.Context) error {
	// assuming that the default user should always have all grants
	if conn.username == "default" {
		return nil
	}
	grants, err := conn.GetUserGrants(ctx)
	if err != nil {
		return err
	}
	verifiedGrants := map[grantType]bool{
		createDatabaseGrant: false,
		createTableGrant:    false,
		insertGrant:         false,
		selectGrant:         false,
		alterGrant:          false,
	}
	if len(grants) == 0 {
		return fmt.Errorf("user is missing the required grants on *.*: %s", joinMissingGrants(verifiedGrants))
	}
	for _, grant := range grants {
		_, ok := verifiedGrants[grant.AccessType]
		if ok && grant.Database == nil && grant.Table == nil && grant.Column == nil {
			verifiedGrants[grant.AccessType] = true
		}
	}
	joinedMissingGrants := joinMissingGrants(verifiedGrants)
	if joinedMissingGrants != "" {
		return fmt.Errorf("user is missing the required grants on *.*: %s", joinedMissingGrants)
	}
	return nil
}

func joinMissingGrants(userGrants map[grantType]bool) string {
	var missingGrants []grantType
	for grant, verified := range userGrants {
		if !verified {
			missingGrants = append(missingGrants, grant)
		}
	}
	if len(missingGrants) > 0 {
		sort.Strings(missingGrants)
		return strings.Join(missingGrants, ", ")
	}
	return ""
}

func hasDecimalPrefix(colType string) bool {
	return strings.HasPrefix(colType, "Decimal(") || strings.HasPrefix(colType, "Nullable(Decimal(")
}

// A sample exception: code: 341, message: Mutation is not finished because some replicas are inactive right now
func isIncompleteMutationErr(err error) bool {
	var exception *clickhouse.Exception
	ok := errors.As(err, &exception)
	if !ok || exception.Code != 341 {
		return false
	}
	return true
}

func isDatabaseBeingCreatedErr(err error) bool {
	var exception *clickhouse.Exception
	ok := errors.As(err, &exception)
	if !ok || exception.Code != 81 {
		return false
	}
	return true
}

type connectionOpType string

const (
	createDatabase             connectionOpType = "CreateDatabase"
	checkDatabaseExists        connectionOpType = "CheckDatabaseExists"
	createTable                connectionOpType = "CreateTable"
	describeTable              connectionOpType = "DescribeTable"
	alterTable                 connectionOpType = "AlterTable"
	alterTablePKCreateTable    connectionOpType = "AlterTable(PK, Create table)"
	alterTablePKInsert         connectionOpType = "AlterTable(PK, Insert from select)"
	alterTablePKRename         connectionOpType = "AlterTable(PK, Rename tables)"
	softTruncateTable          connectionOpType = "SoftTruncateTable"
	hardTruncateTable          connectionOpType = "HardTruncateTable"
	dropTable                  connectionOpType = "DropTable"
	insertBatchReplace         connectionOpType = "InsertBatch(Replace)"
	insertBatchReplaceTask     connectionOpType = "InsertBatch(Replace task)"
	insertBatchUpdate          connectionOpType = "InsertBatch(Update)"
	insertBatchUpdateTask      connectionOpType = "InsertBatch(Update task)"
	insertBatchHardDelete      connectionOpType = "InsertBatch(Hard delete)"
	insertBatchHardDeleteTask  connectionOpType = "InsertBatch(Hard delete task)"
	updateHistoryBatch         connectionOpType = "UpdateHistoryBatch"
	getColumnTypesWithIndexMap connectionOpType = "GetColumnTypes"
	selectByPrimaryKeys        connectionOpType = "SelectByPrimaryKeys"
	getUserGrants              connectionOpType = "GetUserGrants"
	connectionTest             connectionOpType = "ConnectionTest"
	allReplicasActive          connectionOpType = "AllReplicasActive"
	allMutationsCompleted      connectionOpType = "AllMutationsCompleted"
	waitDatabaseIsCreated      connectionOpType = "WaitDatabaseIsCreated"
)

type grantType = string

const (
	createDatabaseGrant grantType = "CREATE DATABASE"
	createTableGrant    grantType = "CREATE TABLE"
	insertGrant         grantType = "INSERT"
	selectGrant         grantType = "SELECT"
	alterGrant          grantType = "ALTER"
)
