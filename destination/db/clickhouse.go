package db

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"iter"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"fivetran.com/fivetran_sdk/destination/common"
	"fivetran.com/fivetran_sdk/destination/common/benchmark"
	"fivetran.com/fivetran_sdk/destination/common/constants"
	csvfile "fivetran.com/fivetran_sdk/destination/common/csv"
	"fivetran.com/fivetran_sdk/destination/common/fivetran"
	"fivetran.com/fivetran_sdk/destination/common/flags"
	"fivetran.com/fivetran_sdk/destination/common/log"
	"fivetran.com/fivetran_sdk/destination/common/retry"
	"fivetran.com/fivetran_sdk/destination/common/types"
	"fivetran.com/fivetran_sdk/destination/db/config"
	"fivetran.com/fivetran_sdk/destination/db/sql"
	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

const (
	maxQueryLengthForLogging = 200
)

type ClickHouseConnection struct {
	driver.Conn
	username      string
	isLocal       bool
	queryCount    int64
	errorCount    int64
	totalDuration time.Duration
}

func (conn *ClickHouseConnection) logConnectionStats() {
	avgDuration := time.Duration(0)
	if conn.queryCount > 0 {
		avgDuration = conn.totalDuration / time.Duration(conn.queryCount)
	}

	log.Info(fmt.Sprintf("Connection stats - Queries: %d, Errors: %d, Avg Duration: %v",
		conn.queryCount,
		conn.errorCount,
		avgDuration))
}

func (conn *ClickHouseConnection) recordQuery(duration time.Duration, success bool) {
	conn.queryCount++
	conn.totalDuration += duration
	if !success {
		conn.errorCount++
	} else {
		conn.queryCount++
		conn.totalDuration += duration
	}
	// Log stats every 100 queries
	if conn.queryCount%100 == 0 {
		conn.logConnectionStats()
	}
}

func GetClickHouseConnection(ctx context.Context, connConfig *config.Config) (*ClickHouseConnection, error) {
	log.Info(fmt.Sprintf("Initializing ClickHouse connection to %s:%d",
		connConfig.Host, connConfig.Port))

	settings := clickhouse.Settings{
		// support ISO DateTime formats from CSV
		// https://clickhouse.com/docs/en/operations/settings/formats#date_time_input_format
		"date_time_input_format": "best_effort",
		// https://clickhouse.com/docs/en/operations/settings/settings#alter-sync
		// https://github.com/ClickHouse/clickhouse-private/pull/12617
		"alter_sync": 3,
		// https://clickhouse.com/docs/en/operations/settings/settings#mutations_sync
		// https://github.com/ClickHouse/clickhouse-private/pull/12617
		"mutations_sync": 3,
		// https://clickhouse.com/docs/en/operations/settings/settings#lightweight_deletes_sync
		"lightweight_deletes_sync": 3,
	}
	var tlsConfig *tls.Config = nil
	if !connConfig.Local {
		tlsConfig = &tls.Config{InsecureSkipVerify: false}

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
		return nil, fmt.Errorf("error while opening a connection to ClickHouse: %w", err)
	}
	err = retry.OnNetError(func() error {
		return conn.Ping(ctx)
	}, ctx, "ping", false)
	if err != nil {
		return nil, fmt.Errorf("ClickHouse connection error: %w", err)
	}
	chConn := &ClickHouseConnection{Conn: conn, username: connConfig.Username, isLocal: connConfig.Local}
	version, err := chConn.GetVersion(ctx)
	if err != nil {
		// Non-fatal: the version query is informational and the connection was already verified via Ping.
		log.Warn(fmt.Sprintf("Failed to query the ClickHouse server version: %v", err))
		version = "unknown"
	}
	log.Info(fmt.Sprintf("ClickHouse connection established successfully, server version: %s", version))
	return chConn, nil
}

func (conn *ClickHouseConnection) ExecStatement(
	ctx context.Context,
	statement string,
	op connectionOpType,
	benchmark bool,
) error {
	// Generate unique query ID
	queryID := uuid.New().String()

	ctx = clickhouse.Context(ctx, clickhouse.WithQueryID(queryID))

	// Add as comment for visibility in query text
	statementWithComment := fmt.Sprintf("-- query_id: %s, operation: %s\n%s", queryID, op, statement)

	startTime := time.Now()
	logQuery := statement
	if len(logQuery) > maxQueryLengthForLogging {
		logQuery = statement[:maxQueryLengthForLogging] + "..."
	}

	log.Info(fmt.Sprintf("Executing %s [query_id=%s]: %s", op, queryID, logQuery))
	err := retry.OnNetError(func() error {
		return conn.Exec(ctx, statementWithComment)
	}, ctx, string(op), benchmark)

	// Calculate duration once for consistent reporting
	duration := time.Since(startTime)
	conn.recordQuery(duration, err == nil)

	if err != nil {
		return fmt.Errorf("error while executing %s [query_id=%s]: %w", logQuery, queryID, err)
	}
	log.Info(fmt.Sprintf("Successfully executed %s [query_id=%s] in %v", op, queryID, duration))
	return nil
}

func (conn *ClickHouseConnection) ExecQuery(
	ctx context.Context,
	query string,
	op connectionOpType,
	benchmark bool,
) (driver.Rows, error) {
	// Generate unique query ID
	queryID := uuid.New().String()

	ctx = clickhouse.Context(ctx, clickhouse.WithQueryID(queryID))

	// Add query ID as SQL comment at the beginning of the query
	queryWithID := fmt.Sprintf("-- query_id: %s\n%s", queryID, query)

	startTime := time.Now()
	logQuery := query
	if len(logQuery) > maxQueryLengthForLogging {
		logQuery = query[:maxQueryLengthForLogging] + "..."
	}

	log.Info(fmt.Sprintf("Executing query %s [query_id=%s]: %s", op, queryID, logQuery))
	rows, err := retry.OnNetErrorWithData(func() (driver.Rows, error) {
		return conn.Query(ctx, queryWithID)
	}, ctx, string(op), benchmark)

	// Calculate duration once for consistent reporting
	duration := time.Since(startTime)
	conn.recordQuery(duration, err == nil)

	if err != nil {
		return nil, fmt.Errorf("error while executing %s [query_id=%s]: %w", logQuery, queryID, err)
	}
	log.Info(fmt.Sprintf("Query %s [query_id=%s] completed in %v", op, queryID, duration))
	return rows, nil
}

// ExecBoolQuery queries a single row with a single boolean value using ExecQuery.
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
	defer rows.Close() //nolint:errcheck
	if !rows.Next() {
		return false, fmt.Errorf("unexpected empty result from %s", query)
	}
	var result bool
	if err = rows.Scan(&result); err != nil {
		return false, err
	}
	return result, nil
}

// execMutation runs a ClickHouse mutation (ALTER UPDATE/DELETE, lightweight DELETE,
// TRUNCATE, etc.) with the standard envelope: a pre-flight WaitAllNodesAvailable check
// (warns on failure, non-fatal) followed by ExecStatement, and on failure a
// WaitAllMutationsCompleted fallback that handles ClickHouse error code 341 (incomplete
// mutation, typically caused by a replica being unavailable during the ALTER) by waiting
// for the async mutation to finish.
func (conn *ClickHouseConnection) execMutation(
	ctx context.Context,
	statement string,
	schemaName string,
	tableName string,
	op connectionOpType,
) error {
	if err := conn.WaitAllNodesAvailable(ctx, schemaName, tableName); err != nil {
		log.Warn(fmt.Sprintf("Not all nodes available for %s.%s: %v", schemaName, tableName, err))
	}
	err := conn.ExecStatement(ctx, statement, op, true)
	if err != nil {
		if waitErr := conn.WaitAllMutationsCompleted(ctx, err, schemaName, tableName); waitErr != nil {
			return waitErr
		}
	}
	return nil
}

// execAlterTableOps runs an ALTER TABLE composed of the given ops. Used for metadata-only
// alters (ADD COLUMN, etc.); callers that need the mutation envelope should use
// execMutation directly.
func (conn *ClickHouseConnection) execAlterTableOps(
	ctx context.Context,
	schemaName string,
	tableName string,
	ops []*types.AlterTableOp,
	op connectionOpType,
) error {
	stmt, err := sql.GetAlterTableStatement(schemaName, tableName, ops)
	if err != nil {
		return err
	}
	return conn.ExecStatement(ctx, stmt, op, false)
}

// execInsertFromSelect runs INSERT INTO toTable SELECT cols FROM fromTable.
func (conn *ClickHouseConnection) execInsertFromSelect(
	ctx context.Context,
	schemaName string,
	fromTable string,
	toTable string,
	colNames []string,
	op connectionOpType,
) error {
	stmt, err := sql.GetInsertFromSelectStatement(schemaName, fromTable, toTable, colNames)
	if err != nil {
		return err
	}
	return conn.ExecStatement(ctx, stmt, op, true)
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
	defer rows.Close() //nolint:errcheck
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

// GetColumnTypes returns the information about the table columns as reported by the driver;
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
	defer rows.Close() //nolint:errcheck
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
	defer rows.Close() //nolint:errcheck
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
	return conn.scanExistsResult(ctx, statement, checkDatabaseExists)
}

// CheckTableExists returns true if the table exists in the given schema.
func (conn *ClickHouseConnection) CheckTableExists(
	ctx context.Context,
	schemaName string,
	tableName string,
) (bool, error) {
	statement, err := sql.GetCheckTableExistsStatement(schemaName, tableName)
	if err != nil {
		return false, err
	}
	return conn.scanExistsResult(ctx, statement, checkTableExists)
}

// scanExistsResult runs an `EXISTS …` statement and returns whether the
// object was reported to exist. ClickHouse returns a single UInt8 column
// (1 if present, 0 otherwise) for these statements.
func (conn *ClickHouseConnection) scanExistsResult(
	ctx context.Context,
	statement string,
	op connectionOpType,
) (bool, error) {
	rows, err := conn.ExecQuery(ctx, statement, op, false)
	if err != nil {
		return false, err
	}
	defer rows.Close() //nolint:errcheck
	if !rows.Next() {
		return false, fmt.Errorf("unexpected empty result from %s", statement)
	}
	var result uint8
	if err = rows.Scan(&result); err != nil {
		return false, err
	}
	return result == 1, nil
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
			if err := conn.execInsertFromSelect(
				ctx, schemaName, tableName, newTableName, unchangedColNames, alterTablePKInsert); err != nil {
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
		if err := conn.execMutation(ctx, statement, schemaName, tableName, alterTable); err != nil {
			return false, err
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
	err = conn.ExecStatement(ctx, renameStmt, renameTable, false)
	if err == nil {
		return nil
	}

	// this method makes the operation safe to retry by recovering from the specific case where
	// a previous attempt completed at the server but the response was lost on the
	// wire. After a successful first attempt the source table is gone and the
	// destination table exists; the retry then fails with code 57
	// (TABLE_ALREADY_EXISTS) because ClickHouse checks the destination collision
	// before resolving the missing source.
	if !isTableAlreadyExistsErr(err) {
		return err
	}
	sourceExists, checkErr := conn.CheckTableExists(ctx, schemaName, fromTableName)
	if checkErr != nil {
		return fmt.Errorf("error checking if source table %s.%s exists after rename failure: %w; initial cause: %w",
			schemaName, fromTableName, checkErr, err)
	}
	if sourceExists {
		return err
	}
	log.Info(fmt.Sprintf(
		"RenameTable: source %s.%s is gone and destination %s.%s exists; treating rename as already applied",
		schemaName, fromTableName, schemaName, toTableName))
	return nil
}

// TruncateTable truncates a table; softDeletedColumn switches between "hard" (nil) and "soft" (not nil) truncation
// (see sql.GetTruncateTableStatement).
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
	return conn.execMutation(ctx, statement, schemaName, tableName, op)
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

// mapErr lazily transforms a sequence with a function that may fail, yielding
// each transformed value together with its error. Iterating the result never
// materializes the transformed set: values are produced one at a time.
// The result is re-iterable as long as seq is.
func mapErr[S, T any](seq iter.Seq[S], f func(S) (T, error)) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for v := range seq {
			if !yield(f(v)) {
				return
			}
		}
	}
}

// InsertBatch prepares an INSERT batch, appends every row yielded by rows, and
// sends it, retrying the whole operation on transient errors. A non-nil error
// yielded by rows (e.g. a CSV conversion failure) aborts the insert.
//
// Because the operation is retried, rows may be iterated once per attempt: it
// MUST be re-iterable, e.g. a lazy transformation over an in-memory slice.
// Never pass a single-use iterator (such as one consuming a file or network
// stream) — a retry would silently insert incomplete data.
func (conn *ClickHouseConnection) InsertBatch(
	ctx context.Context,
	qualifiedTableName sql.QualifiedTableName,
	rows iter.Seq2[[]any, error],
	opName string,
) error {
	return retry.OnNetError(func() error {
		batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", qualifiedTableName))
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				// ctx.Err() is diagnostic context, not the primary error; %v is nil-safe.
				return fmt.Errorf("error while preparing batch for %s: %w (context state: %v)", qualifiedTableName, err, ctx.Err()) //nolint:errorlint
			}
			return fmt.Errorf("error while preparing batch for %s: %w", qualifiedTableName, err)
		}
		for row, err := range rows {
			if err != nil {
				// Abort releases the batch's connection back to the pool;
				// batch.Append and batch.Send handle that themselves on failure.
				_ = batch.Abort()
				return fmt.Errorf("[%s] error converting row for %s: %w", opName, qualifiedTableName, err)
			}
			if err := batch.Append(row...); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					// ctx.Err() is diagnostic context, not the primary error; %v is nil-safe.
					return fmt.Errorf("error appending row to a batch for %s: %w (context state: %v)", qualifiedTableName, err, ctx.Err()) //nolint:errorlint
				}
				return fmt.Errorf("error appending row to a batch for %s: %w", qualifiedTableName, err)
			}
		}
		err = batch.Send()
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				// ctx.Err() is diagnostic context, not the primary error; %v is nil-safe.
				return fmt.Errorf("error while sending batch for %s: %w (context state: %v)", qualifiedTableName, err, ctx.Err()) //nolint:errorlint
			}
			return fmt.Errorf("error while sending batch for %s: %w", qualifiedTableName, err)
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
					defer rows.Close() //nolint:errcheck
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
	reader *csvfile.CSVFileReader,
	csvColumns *types.CSVColumns,
	nullStr string,
) (int, error) {
	return benchmark.RunAndNoticeWithData(func() (int, error) {
		qualifiedTableName, err := sql.GetQualifiedTableName(schemaName, table.Name)
		if err != nil {
			return 0, err
		}
		totalRows := 0
		for {
			batch, err := reader.ReadBatch(*flags.WriteBatchSize)
			if err != nil {
				return totalRows, err
			}
			if batch == nil {
				break
			}
			totalRows += len(batch)
			log.Notice(fmt.Sprintf("[%s] Read batch of %d rows (total so far: %d)", insertBatchReplace, len(batch), totalRows))
			toInsertRow := func(csvRow []string) ([]any, error) {
				return ToInsertRow(csvRow, csvColumns, nullStr)
			}
			err = conn.InsertBatch(ctx, qualifiedTableName, mapErr(slices.Values(batch), toInsertRow), string(insertBatchReplaceTask))
			if err != nil {
				return totalRows, err
			}
		}
		return totalRows, nil
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
	reader *csvfile.CSVFileReader,
	nullStr string,
	unmodifiedStr string,
	isHistoryMode bool,
) (int, error) {
	return benchmark.RunAndNoticeWithData(func() (int, error) {
		qualifiedTableName, err := sql.GetQualifiedTableName(schemaName, table.Name)
		if err != nil {
			return 0, err
		}
		totalRows := 0
		for {
			batch, err := reader.ReadBatch(*flags.WriteBatchSize)
			if err != nil {
				return totalRows, err
			}
			if batch == nil {
				break
			}
			totalRows += len(batch)
			log.Notice(fmt.Sprintf("[%s] Read batch of %d rows (total so far: %d)", insertBatchUpdate, len(batch), totalRows))
			selectRows, err := conn.SelectByPrimaryKeys(ctx, qualifiedTableName, driverColumns, csvColumns, batch, isHistoryMode)
			if err != nil {
				return totalRows, err
			}
			insertRows, err := MergeUpdatedRows(batch, selectRows, csvColumns, nullStr, unmodifiedStr, isHistoryMode)
			if err != nil {
				return totalRows, err
			}
			if len(insertRows) == 0 {
				log.Warn(fmt.Sprintf("[%s] No rows to insert for %s", insertBatchUpdate, qualifiedTableName))
				continue
			}
			noConversion := func(row []any) ([]any, error) { return row, nil }
			err = conn.InsertBatch(ctx, qualifiedTableName, mapErr(slices.Values(insertRows), noConversion), string(insertBatchUpdateTask))
			if err != nil {
				return totalRows, err
			}
		}
		return totalRows, nil
	}, string(insertBatchUpdate))
}

// StagingTable is a helper table holding one chunk (up to staging_batch_size rows) of a batch file.
// Created by stageChunk, consumed by the operation's statements (e.g. DeleteOverlappingHistory,
// CloseActiveHistoryRows), removed by DropStagingTable.
type StagingTable struct {
	QualifiedTableName sql.QualifiedTableName
	Rows               int                // rows copied from the file
	columns            []*types.CSVColumn // staging table columns, in order
	orderBy            []string           // also the join columns against the destination table
}

// OrderBy are the key column names
func (s *StagingTable) OrderBy() []string {
	return s.orderBy
}

// stageChunk copies the next staging_batch_size rows of reader into a new <table>_fivetran_tmp_<operation>_<unix millis>
// table made of columns, or returns nil when the reader is exhausted. On failure the staging table is
// dropped before returning; on success the caller owns it.
func (conn *ClickHouseConnection) stageChunk(
	ctx context.Context,
	schemaName string,
	table *pb.Table,
	reader *csvfile.CSVFileReader,
	driverColumns *types.DriverColumns,
	operation string,
	columns []*types.CSVColumn,
	orderBy []string,
) (*StagingTable, error) {
	return benchmark.RunAndNoticeWithData(func() (*StagingTable, error) {
		qualifiedTableName, err := sql.GetQualifiedTableName(schemaName, stagingTableName(schemaName, table.Name, operation))
		if err != nil {
			return nil, err
		}
		staging := &StagingTable{QualifiedTableName: qualifiedTableName, columns: columns, orderBy: orderBy}
		limit := int(*flags.StagingBatchSize)
		for staging.Rows < limit {
			batch, err := reader.ReadBatch(min(*flags.WriteBatchSize, uint(limit-staging.Rows)))
			if err == nil && batch == nil {
				break
			}
			if err == nil && staging.Rows == 0 {
				createStmt := sql.GetCreateStagingTableStatement(staging.QualifiedTableName, staging.columns, staging.orderBy, driverColumns)
				if err = conn.ExecStatement(ctx, createStmt, stagingCreate, false); err != nil {
					return nil, err
				}
			}
			if err == nil {
				staging.Rows += len(batch)
				log.Notice(fmt.Sprintf("[%s] Read batch of %d rows (total so far: %d)", stagingChunk, len(batch), staging.Rows))
				toRow := func(csvRow []string) ([]any, error) { return ToStagingRow(csvRow, staging.columns) }
				err = conn.InsertBatch(ctx, staging.QualifiedTableName, mapErr(slices.Values(batch), toRow), string(stagingInsert))
			}
			if err != nil {
				conn.DropStagingTable(ctx, staging)
				return nil, err
			}
		}
		if staging.Rows == 0 {
			return nil, nil
		}
		return staging, nil
	}, string(stagingChunk))
}

// stagingTableName builds <table>_fivetran_tmp_<operation>_<unix millis>. ClickHouse 26.3 accepts table
// names of at most 213 bytes minus the database name, so a longer result is cut: it keeps a prefix of the
// table name and a hash of the full name, which keeps distinct tables apart.
func stagingTableName(schemaName string, tableName string, operation string) string {
	suffix := fmt.Sprintf("_fivetran_tmp_%s_%d", operation, time.Now().UnixMilli())
	base := tableName
	stagingTableNameLength := len(schemaName) + len(base) + len(suffix)
	if stagingTableNameLength > maxTableNameLength {
		sum := sha256.Sum256([]byte(tableName))
		hash := hex.EncodeToString(sum[:4])
		excess := stagingTableNameLength - maxTableNameLength + len(hash) + 1
		prefix := base[:max(len(base)-excess, 0)]
		for !utf8.ValidString(prefix) {
			prefix = prefix[:len(prefix)-1]
		}
		base = hash
		if prefix != "" {
			base = prefix + "_" + hash
		}
	}
	return base + suffix
}

const maxTableNameLength = 213

// DropStagingTable drops the staging table even if ctx is already cancelled.
// A leftover helper table is clutter, not a data issue, so failures are only logged.
func (conn *ClickHouseConnection) DropStagingTable(ctx context.Context, staging *StagingTable) {
	dropCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
	defer cancel()
	if err := conn.DropTable(dropCtx, staging.QualifiedTableName); err != nil {
		log.Warn(fmt.Sprintf("[%s] Failed to drop staging table %s: %v", stagingChunk, staging.QualifiedTableName, err))
	}
}

// StageDeleteChunk stages the next chunk of a delete file: the primary keys, which are also the ORDER BY.
func (conn *ClickHouseConnection) StageDeleteChunk(
	ctx context.Context,
	schemaName string,
	table *pb.Table,
	reader *csvfile.CSVFileReader,
	csvColumns *types.CSVColumns,
	driverColumns *types.DriverColumns,
) (*StagingTable, error) {
	if len(csvColumns.PrimaryKeys) == 0 {
		return nil, fmt.Errorf("[%s] %s.%s: expected at least one primary key", stagingChunk, schemaName, table.Name)
	}
	orderBy := make([]string, 0, len(csvColumns.PrimaryKeys))
	for _, col := range csvColumns.PrimaryKeys {
		orderBy = append(orderBy, col.Name)
	}
	return conn.stageChunk(ctx, schemaName, table, reader, driverColumns, "delete", csvColumns.PrimaryKeys, orderBy)
}

// HardDelete runs sql.GetHardDeleteStatement against the staged keys.
func (conn *ClickHouseConnection) HardDelete(
	ctx context.Context,
	schemaName string,
	table *pb.Table,
	staging *StagingTable,
) error {
	qualifiedTableName, err := sql.GetQualifiedTableName(schemaName, table.Name)
	if err != nil {
		return err
	}
	statement := sql.GetHardDeleteStatement(qualifiedTableName, staging.QualifiedTableName, staging.OrderBy())
	return conn.execMutation(ctx, statement, schemaName, table.Name, hardDelete)
}

// StageEarliestStartChunk stages the next chunk of an earliest-start file: the history keys, then _fivetran_start.
func (conn *ClickHouseConnection) StageEarliestStartChunk(
	ctx context.Context,
	schemaName string,
	table *pb.Table,
	reader *csvfile.CSVFileReader,
	csvColumns *types.CSVColumns,
	driverColumns *types.DriverColumns,
) (*StagingTable, error) {
	return conn.stageHistoryChunk(ctx, schemaName, table, reader, csvColumns, driverColumns, "earliest_start", constants.FivetranStart)
}

// StageHistoryDeleteChunk stages the next chunk of a history-mode delete file: the history keys, then _fivetran_end.
func (conn *ClickHouseConnection) StageHistoryDeleteChunk(
	ctx context.Context,
	schemaName string,
	table *pb.Table,
	reader *csvfile.CSVFileReader,
	csvColumns *types.CSVColumns,
	driverColumns *types.DriverColumns,
) (*StagingTable, error) {
	return conn.stageHistoryChunk(ctx, schemaName, table, reader, csvColumns, driverColumns, "delete", constants.FivetranEnd)
}

// stageHistoryChunk stages the primary keys without _fivetran_start, which are also the ORDER BY, followed by
// valueColumn. See stageChunk.
func (conn *ClickHouseConnection) stageHistoryChunk(
	ctx context.Context,
	schemaName string,
	table *pb.Table,
	reader *csvfile.CSVFileReader,
	csvColumns *types.CSVColumns,
	driverColumns *types.DriverColumns,
	operation string,
	valueColumn string,
) (*StagingTable, error) {
	var columns []*types.CSVColumn
	var orderBy []string
	// _fivetran_start is a version boundary, never a key to match on; see sql.GetDeleteOverlappingHistoryStatement
	for _, col := range csvColumns.PrimaryKeys {
		if col.Name != constants.FivetranStart {
			columns = append(columns, col)
			orderBy = append(orderBy, col.Name)
		}
	}
	if len(orderBy) == 0 {
		return nil, fmt.Errorf("[%s] %s.%s: expected at least one primary key besides %s", stagingChunk, schemaName, table.Name, constants.FivetranStart)
	}
	value, err := csvColumns.FindColumn(valueColumn)
	if err != nil {
		return nil, err
	}
	return conn.stageChunk(ctx, schemaName, table, reader, driverColumns, operation, append(columns, value), orderBy)
}

// DeleteOverlappingHistory runs sql.GetDeleteOverlappingHistoryStatement against the staged file.
func (conn *ClickHouseConnection) DeleteOverlappingHistory(
	ctx context.Context,
	schemaName string,
	table *pb.Table,
	staging *StagingTable,
) error {
	qualifiedTableName, err := sql.GetQualifiedTableName(schemaName, table.Name)
	if err != nil {
		return err
	}
	statement := sql.GetDeleteOverlappingHistoryStatement(qualifiedTableName, staging.QualifiedTableName, staging.OrderBy())
	return conn.execMutation(ctx, statement, schemaName, table.Name, earliestStartDelete)
}

// CloseActiveHistoryRows runs sql.GetCloseActiveHistoryRowsStatement against the staged file.
func (conn *ClickHouseConnection) CloseActiveHistoryRows(
	ctx context.Context,
	schemaName string,
	table *pb.Table,
	staging *StagingTable,
	driverColumns *types.DriverColumns,
	endColumn string,
) error {
	qualifiedTableName, err := sql.GetQualifiedTableName(schemaName, table.Name)
	if err != nil {
		return err
	}
	columnNames := make([]string, 0, len(driverColumns.Columns))
	for _, col := range driverColumns.Columns {
		columnNames = append(columnNames, col.Name)
	}
	statement, err := sql.GetCloseActiveHistoryRowsStatement(qualifiedTableName, staging.QualifiedTableName, columnNames, staging.OrderBy(), endColumn)
	if err != nil {
		return err
	}
	return conn.ExecStatement(ctx, statement, historyCloseActive, true)
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

// WaitAllMutationsCompleted waits for all async mutations to complete.
// If mutation_sync=3 and alter_sync=3 is not enough if
// one of the nodes went down exactly at the time of the ALTER TABLE statement execution,
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
		log.Warn(fmt.Sprintf("It seems like not all nodes are available: %v. We strongly recommend to check the cluster health and availability to avoid inconsistency between replicas", err))
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

// WaitDatabaseIsCreated waits for the database to be created when concurrent creation requests collide.
// If there are parallel requests to create tables in a particular database which does not exist yet,
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

// GetVersion queries the ClickHouse server version. The Fivetran runtime metadata, if available,
// is included as a SQL comment so it is visible in the ClickHouse query log for supportability.
func (conn *ClickHouseConnection) GetVersion(ctx context.Context) (string, error) {
	query := "SELECT version()"
	if metadata := fivetran.GetMetadata().String(); metadata != "" {
		query = fmt.Sprintf("-- %s\n%s", metadata, query)
	}
	rows, err := conn.ExecQuery(ctx, query, getVersion, false)
	if err != nil {
		return "", err
	}
	defer rows.Close() //nolint:errcheck
	if !rows.Next() {
		return "", fmt.Errorf("unexpected empty result from the version query")
	}
	var version string
	if err = rows.Scan(&version); err != nil {
		return "", err
	}
	return version, nil
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

// isTableAlreadyExistsErr reports whether err is (or wraps) a ClickHouse
// server exception with code 57 (TABLE_ALREADY_EXISTS).
func isTableAlreadyExistsErr(err error) bool {
	var exception *clickhouse.Exception
	ok := errors.As(err, &exception)
	if !ok || exception.Code != 57 {
		return false
	}
	return true
}

type connectionOpType string

const (
	createDatabase             connectionOpType = "CreateDatabase"
	checkDatabaseExists        connectionOpType = "CheckDatabaseExists"
	checkTableExists           connectionOpType = "CheckTableExists"
	createTable                connectionOpType = "CreateTable"
	describeTable              connectionOpType = "DescribeTable"
	alterTable                 connectionOpType = "AlterTable"
	alterTablePKCreateTable    connectionOpType = "AlterTable(PK, Create table)"
	alterTablePKInsert         connectionOpType = "AlterTable(PK, Insert from select)"
	renameTable                connectionOpType = "RenameTable"
	softTruncateTable          connectionOpType = "SoftTruncateTable"
	hardTruncateTable          connectionOpType = "HardTruncateTable"
	dropTable                  connectionOpType = "DropTable"
	insertBatchReplace         connectionOpType = "InsertBatch(Replace)"
	insertBatchReplaceTask     connectionOpType = "InsertBatch(Replace task)"
	insertBatchUpdate          connectionOpType = "InsertBatch(Update)"
	insertBatchUpdateTask      connectionOpType = "InsertBatch(Update task)"
	hardDelete                 connectionOpType = "HardDelete"
	stagingChunk               connectionOpType = "Staging(Chunk)"
	stagingCreate              connectionOpType = "Staging(Create table)"
	stagingInsert              connectionOpType = "Staging(Insert)"
	earliestStartDelete        connectionOpType = "EarliestStart(Delete overlapping versions)"
	historyCloseActive         connectionOpType = "History(Close active rows)"
	getColumnTypesWithIndexMap connectionOpType = "GetColumnTypes"
	selectByPrimaryKeys        connectionOpType = "SelectByPrimaryKeys"
	getUserGrants              connectionOpType = "GetUserGrants"
	connectionTest             connectionOpType = "ConnectionTest"
	getVersion                 connectionOpType = "GetVersion"
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
