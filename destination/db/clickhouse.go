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
	deploymentType config.DeploymentType
	driver.Conn
}

func GetClickHouseConnection(configuration map[string]string) (*ClickHouseConnection, error) {
	connConfig, err := config.Parse(configuration)
	if err != nil {
		return nil, fmt.Errorf("error while parsing configuration: %w", err)
	}
	settings := clickhouse.Settings{}
	if connConfig.DeploymentType == config.DeploymentTypeClickHouseCloud ||
		connConfig.DeploymentType == config.DeploymentTypeCluster {
		addDefaultClusterSettings(settings)
	}
	hostname := fmt.Sprintf("%s:%d", connConfig.Hostname, connConfig.Port)
	options := &clickhouse.Options{
		Addr: []string{hostname},
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
	}
	if connConfig.SSL.Enabled {
		options.TLS = &tls.Config{InsecureSkipVerify: connConfig.SSL.SkipVerify}
	}
	conn, err := clickhouse.Open(options)
	if err != nil {
		err = fmt.Errorf("error while opening a connection to ClickHouse: %w", err)
		log.Error(err)
		return nil, err
	}
	return &ClickHouseConnection{connConfig.DeploymentType, conn}, nil
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

// GetOnPremiseClusterMacros introspects macros values from system.macros table for on-premise cluster deployments.
// The server nodes are expected to have the following entries in their configuration:
//
//	<macros>
//	 <cluster>my_cluster</cluster>
//	 <replica>clickhouse1</replica>
//	 <shard>1</shard>
//	</macros>
func (conn *ClickHouseConnection) GetOnPremiseClusterMacros(ctx context.Context) (*types.ClusterMacros, error) {
	// instead of running a simple SELECT and getting a result set with multiple rows like this:
	//
	// ┌─macro───┬─substitution─┐
	// │ cluster │ test_cluster │
	// │ replica │ clickhouse1  │
	// │ shard   │ 1            │
	// └─────────┴──────────────┘
	// we can prepare a convenience map in advance, querying it as a single row:
	//
	// {'cluster':'test_cluster','replica':'clickhouse1','shard':'1'}
	rows, err := conn.ExecQuery(ctx,
		"SELECT mapFromArrays(flatten(groupArray([macro])), flatten(groupArray([substitution]))) FROM system.macros",
		getOnPremiseClusterMacros, false)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var macroMap map[string]string
	var macros *types.ClusterMacros = nil
	if !rows.Next() {
		return nil, fmt.Errorf("no rows returned from system.macros")
	}
	if err = rows.Scan(&macroMap); err != nil {
		return nil, err
	}
	macros, err = types.ToClusterMacros(macroMap)
	return macros, nil
}

// GetInsertQuorumSettings
// introspects a proper value for insert_quorum ClickHouse setting and indicates if it should be set.
// Only makes sense for on-premise clusters to ensure that the data is written to all the replicas.
// It should be ignored otherwise (types.InsertQuorumSettings.Enabled = false).
// See also: https://clickhouse.com/docs/en/operations/settings/settings#insert_quorum
func (conn *ClickHouseConnection) GetInsertQuorumSettings(ctx context.Context) (*types.InsertQuorumSettings, error) {
	if conn.deploymentType != config.DeploymentTypeCluster {
		return &types.InsertQuorumSettings{Value: 0, Enabled: false}, nil
	}
	rows, err := conn.ExecQuery(ctx,
		"SELECT count(*) FROM system.clusters c JOIN system.macros m ON c.cluster = m.substitution",
		getInsertQuorumValue, false)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var count uint64
	if !rows.Next() {
		return nil, fmt.Errorf("no rows returned from system.macros")
	}
	if err = rows.Scan(&count); err != nil {
		return nil, err
	}
	return &types.InsertQuorumSettings{Value: count, Enabled: true}, nil
}

func (conn *ClickHouseConnection) CreateTable(
	ctx context.Context,
	schemaName string,
	tableName string,
	tableDescription *types.TableDescription,
) (err error) {
	var macros *types.ClusterMacros = nil
	if conn.deploymentType == config.DeploymentTypeCluster {
		macros, err = conn.GetOnPremiseClusterMacros(ctx)
		if err != nil {
			return err
		}
	}
	statement, err := sql.GetCreateTableStatement(schemaName, tableName, tableDescription, macros)
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
	var macros *types.ClusterMacros = nil
	if conn.deploymentType == config.DeploymentTypeCluster {
		macros, err = conn.GetOnPremiseClusterMacros(ctx)
		if err != nil {
			return err
		}
	}
	ops := GetAlterTableOps(from, to)
	statement, err := sql.GetAlterTableStatement(schemaName, tableName, ops, macros)
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
	quorumSettings *types.InsertQuorumSettings,
) error {
	if len(skipIdx) == len(rows) {
		log.Warn(fmt.Sprintf("[%s] All rows are skipped for %s", opName, fullTableName))
		return nil
	}
	ctx, err := conn.withInsertQuorum(ctx, quorumSettings)
	if err != nil {
		return err
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
	quorumSettings *types.InsertQuorumSettings,
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
				err = conn.InsertBatch(ctx, fullName, insertRows, nil, string(insertBatchReplaceTask), quorumSettings)
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
	quorumSettings *types.InsertQuorumSettings,
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
				err = conn.InsertBatch(ctx, fullName, insertRows, skipIdx, string(insertBatchUpdateTask), quorumSettings)
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
	quorumSettings *types.InsertQuorumSettings,
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
				err = conn.InsertBatch(ctx, fullName, insertRows, skipIdx, string(insertBatchDeleteTask), quorumSettings)
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

func addDefaultClusterSettings(settings clickhouse.Settings) {
	// https://clickhouse.com/docs/en/operations/settings/settings#alter-sync
	settings["alter_sync"] = 2
	// https://clickhouse.com/docs/en/operations/settings/settings#mutations_sync
	settings["mutations_sync"] = 2
}

// ClickHouse Cloud manages quorum settings for inserts automatically.
// We still need to enable select_sequential_consistency for UpdateBatch or SoftDeleteBatch.
func (conn *ClickHouseConnection) withSelectConsistencySettings(
	ctx context.Context,
) context.Context {
	if conn.deploymentType == config.DeploymentTypeClickHouseCloud {
		return clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
			// https://clickhouse.com/docs/en/operations/settings/settings#select_sequential_consistency
			"select_sequential_consistency": 1,
		}))
	}
	return ctx
}

// For on-premise clusters, we need to set insert_quorum equal to the number of replicas.
// The exact value is introspected via GetInsertQuorumSettings before calling UpdateBatch or SoftDeleteBatch.
func (conn *ClickHouseConnection) withInsertQuorum(
	ctx context.Context,
	quorum *types.InsertQuorumSettings,
) (context.Context, error) {
	if conn.deploymentType == config.DeploymentTypeCluster && quorum != nil && quorum.Enabled {
		if quorum.Value < 2 { // if it's a cluster, we expect more than 1 node there
			return nil, fmt.Errorf("insert_quorum value must be at least 2, got: %d", quorum.Value)
		}
		return clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
			// https://clickhouse.com/docs/en/operations/settings/settings#insert_quorum
			"insert_quorum": quorum.Value,
		})), nil
	}
	return ctx, nil
}

type connectionOpType string

const (
	describeTable             connectionOpType = "DescribeTable"
	createTable               connectionOpType = "CreateTable"
	alterTable                connectionOpType = "AlterTable"
	truncateTable             connectionOpType = "TruncateTable"
	insertBatchReplace        connectionOpType = "InsertBatch(Replace)"
	insertBatchReplaceTask    connectionOpType = "InsertBatch(Replace task)"
	insertBatchUpdate         connectionOpType = "InsertBatch(Update)"
	insertBatchUpdateTask     connectionOpType = "InsertBatch(Update task)"
	insertBatchDelete         connectionOpType = "InsertBatch(Delete)"
	insertBatchDeleteTask     connectionOpType = "InsertBatch(Delete task)"
	getColumnTypes            connectionOpType = "GetColumnTypes"
	getOnPremiseClusterMacros connectionOpType = "GetOnPremiseClusterMacros"
	getInsertQuorumValue      connectionOpType = "GetInsertQuorumSettings"
	selectByPrimaryKeys       connectionOpType = "SelectByPrimaryKeys"
)
