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
			//"async_insert":                   1,
			//"wait_for_async_insert":          1,
		},
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
		return nil, err
	}
	return &ClickHouseConnection{database, ctx, conn}, nil
}

func (conn *ClickHouseConnection) DescribeTable(schemaName string, tableName string) (*TableDescription, error) {
	query, err := GetDescribeTableQuery(schemaName, tableName)
	if err != nil {
		return nil, err
	}
	rows, err := conn.Query(conn.ctx, query)
	if err != nil {
		return nil, err
	}
	var (
		colName      string
		colType      string
		isPrimaryKey uint8
	)
	var columns []*ColumnDefinition
	for rows.Next() {
		if err := rows.Scan(&colName, &colType, &isPrimaryKey); err != nil {
			return nil, err
		}
		columns = append(columns, &ColumnDefinition{
			Name:         colName,
			Type:         colType,
			IsPrimaryKey: isPrimaryKey == 1,
		})
	}
	return MakeTableDescription(columns), nil
}

func (conn *ClickHouseConnection) CreateTable(schemaName string, tableName string, tableDescription *TableDescription) error {
	statement, err := GetCreateTableStatement(schemaName, tableName, tableDescription)
	if err != nil {
		return err
	}
	if err := conn.Exec(conn.ctx, statement); err != nil {
		return fmt.Errorf("error while executing %s: %w", statement, err)
	}
	return nil
}

func (conn *ClickHouseConnection) AlterTable(schemaName string, tableName string, ops []*AlterTableOp) error {
	statement, err := GetAlterTableStatement(schemaName, tableName, ops)
	if err != nil {
		return err
	}
	if err := conn.Exec(conn.ctx, statement); err != nil {
		return fmt.Errorf("error while executing %s: %w", statement, err)
	}
	return nil
}

func (conn *ClickHouseConnection) TruncateTable(schemaName string, tableName string) error {
	statement, err := GetTruncateTableStatement(schemaName, tableName)
	if err != nil {
		return err
	}
	if err := conn.Exec(conn.ctx, statement); err != nil {
		return fmt.Errorf("error while executing %s: %w", statement, err)
	}
	return nil
}

func (conn *ClickHouseConnection) InsertBatch(
	fullTableName string,
	rows [][]interface{},
	skipIdx map[int]bool,
	opType WriteBatchOpType,
) error {
	if len(skipIdx) == len(rows) {
		LogWarn(fmt.Sprintf("[InsertBatch - %s] All rows are skipped for %s", opType, fullTableName))
		return nil
	}
	batch, err := conn.PrepareBatch(conn.ctx, fmt.Sprintf("INSERT INTO %s", fullTableName))
	if err != nil {
		return fmt.Errorf("error while preparing a batch for %s: %w", fullTableName, err)
	}
	for i, row := range rows {
		if skipIdx[i] {
			continue
		}
		if err := batch.Append(row...); err != nil {
			return fmt.Errorf("error appending row to a batch for %s: %w", fullTableName, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("error while sending a batch to %s: %w", fullTableName, err)
	}
	return nil
}

func (conn *ClickHouseConnection) GetColumnTypes(schemaName string, tableName string) ([]driver.ColumnType, error) {
	query, err := GetColumnTypesQuery(schemaName, tableName)
	if err != nil {
		return nil, err
	}
	rows, err := conn.Query(conn.ctx, query)
	if err != nil {
		return nil, fmt.Errorf("error while executing %s: %w", query, err)
	}
	defer rows.Close()
	return rows.ColumnTypes(), nil
}

func (conn *ClickHouseConnection) SelectByPrimaryKeys(
	fullTableName string,
	columnTypes []driver.ColumnType,
	pkCols []*PrimaryKeyColumn,
	csv CSV,
) (RowsByPrimaryKeyValue, error) {
	query, err := CSVRowsToSelectQuery(csv, fullTableName, pkCols)
	if err != nil {
		return nil, err
	}
	scanRows := ColumnTypesToEmptyRows(columnTypes, uint32(len(csv)))
	rows, err := conn.Query(conn.ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	rowsByPKValues := make(map[string][]interface{})
	for i := 0; rows.Next(); i++ {
		if err := rows.Scan(scanRows[i]...); err != nil {
			return nil, err
		}
		mappingKey, err := GetDatabaseRowMappingKey(scanRows[i], pkCols)
		if err != nil {
			return nil, err
		}
		rowsByPKValues[mappingKey] = scanRows[i]
	}
	return rowsByPKValues, nil
}

func (conn *ClickHouseConnection) ReplaceBatch(
	schemaName string,
	table *pb.Table,
	csv CSV,
	nullStr string,
	batchSize int,
) error {
	fullName, err := GetFullTableName(schemaName, table.Name)
	if err != nil {
		return err
	}
	for i := 0; i < len(csv); i += batchSize {
		end := i + batchSize
		if end > len(csv) {
			end = len(csv)
		}
		batch := csv[i:end]
		if len(batch) == 0 {
			break
		}
		insertRows := make([][]interface{}, len(batch))
		for j, csvRow := range batch {
			insertRow, err := CSVRowToInsertValues(csvRow, table, nullStr)
			if err != nil {
				return err
			}
			insertRows[j] = insertRow
		}
		err = conn.InsertBatch(fullName, insertRows, nil, Replace)
		if err != nil {
			return err
		}
	}
	return nil
}

func (conn *ClickHouseConnection) UpdateBatch(
	schemaName string,
	table *pb.Table,
	pkCols []*PrimaryKeyColumn,
	columnTypes []driver.ColumnType,
	csv CSV,
	nullStr string,
	unmodifiedStr string,
	batchSize int,
) error {
	fullName, err := GetFullTableName(schemaName, table.Name)
	if err != nil {
		return err
	}
	for i := 0; i < len(csv); i += batchSize {
		end := i + batchSize
		if end > len(csv) {
			end = len(csv)
		}
		batch := csv[i:end]
		if len(batch) == 0 {
			break
		}
		selectRows, err := conn.SelectByPrimaryKeys(fullName, columnTypes, pkCols, batch)
		if err != nil {
			return err
		}
		insertRows := make([][]interface{}, len(batch))
		skipIdx := make(map[int]bool)
		for j, csvRow := range batch {
			mappingKey, err := GetCSVRowMappingKey(csvRow, pkCols)
			if err != nil {
				return err
			}
			dbRow, exists := selectRows[mappingKey]
			if exists {
				updatedRow, err := CSVRowToUpdatedDBRow(csvRow, dbRow, table, nullStr, unmodifiedStr)
				if err != nil {
					return err
				}
				insertRows[j] = updatedRow
			} else {
				// Shouldn't happen
				LogWarn(fmt.Sprintf("[UpdateBatch] Row with PK mapping %s does not exist", mappingKey))
				skipIdx[j] = true
				continue
			}
		}
		err = conn.InsertBatch(fullName, insertRows, skipIdx, Update)
		if err != nil {
			return err
		}
	}
	return nil
}

func (conn *ClickHouseConnection) SoftDeleteBatch(
	schemaName string,
	table *pb.Table,
	pkCols []*PrimaryKeyColumn,
	columnTypes []driver.ColumnType,
	csv CSV,
	batchSize int,
	fivetranSyncedIdx int,
	fivetranDeletedIdx int,
) error {
	fullName, err := GetFullTableName(schemaName, table.Name)
	if err != nil {
		return err
	}
	for i := 0; i < len(csv); i += batchSize {
		end := i + batchSize
		if end > len(csv) {
			end = len(csv)
		}
		batch := csv[i:end]
		if len(batch) == 0 {
			break
		}
		selectRows, err := conn.SelectByPrimaryKeys(fullName, columnTypes, pkCols, batch)
		if err != nil {
			return err
		}
		insertRows := make([][]interface{}, len(batch))
		skipIdx := make(map[int]bool)
		for j, csvRow := range batch {
			mappingKey, err := GetCSVRowMappingKey(csvRow, pkCols)
			if err != nil {
				return err
			}
			dbRow, exists := selectRows[mappingKey]
			if exists {
				softDeletedRow, err := CSVRowToSoftDeletedRow(csvRow, dbRow, fivetranSyncedIdx, fivetranDeletedIdx)
				if err != nil {
					return err
				}
				insertRows[j] = softDeletedRow
			} else {
				// Shouldn't happen
				LogWarn(fmt.Sprintf("[SoftDeleteBatch] Row with PK mapping %s does not exist", mappingKey))
				skipIdx[j] = true
				continue
			}
		}
		err = conn.InsertBatch(fullName, insertRows, skipIdx, SoftDelete)
		if err != nil {
			return err
		}
	}
	return nil
}

func (conn *ClickHouseConnection) ConnectionTest() error {
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
}

func (conn *ClickHouseConnection) MutationTest() error {
	id := strings.Replace(uuid.New().String(), "-", "", -1)
	tableName := fmt.Sprintf("fivetran_destination_test_%s", id)

	// Create test table
	err := conn.CreateTable("", tableName, MakeTableDescription([]*ColumnDefinition{
		{Name: "Col1", Type: "UInt8", IsPrimaryKey: true},
		{Name: "Col2", Type: "String"},
		{Name: FivetranSynced, Type: "DateTime64(9, 'UTC')"},
	}))
	if err != nil {
		return fmt.Errorf("error while creating table %s: %w", tableName, err)
	}

	// Insert test Data
	batch, err := conn.PrepareBatch(conn.ctx, fmt.Sprintf("INSERT INTO `%s`", tableName))
	if err != nil {
		return fmt.Errorf("error while preparing a batch for %s: %w", tableName, err)
	}
	now := time.Now()
	err = batch.Append(uint8(42), "ClickHouse", now)
	if err != nil {
		return fmt.Errorf("error appending row to a batch for %s: %w", tableName, err)
	}
	err = batch.Send()
	if err != nil {
		return fmt.Errorf("error while sending a batch to %s: %w", tableName, err)
	}

	// Alter
	dbType := "FixedString(10)"
	err = conn.AlterTable("", tableName, []*AlterTableOp{
		{Op: Modify, Column: "Col2", Type: &dbType},
	})
	if err != nil {
		return fmt.Errorf("error while altering table %s: %w", tableName, err)
	}

	// Check the inserted Data
	row := conn.QueryRow(conn.ctx, fmt.Sprintf("SELECT * FROM `%s`", tableName))
	var (
		col1 uint8
		col2 string
		col3 *time.Time
	)
	if err := row.Scan(&col1, &col2, &col3); err != nil {
		return fmt.Errorf("error while scanning row from %s: %w", tableName, err)
	}
	if col1 != uint8(42) || col2 != "ClickHouse" {
		return fmt.Errorf("unexpected Data check output, expected 42/ClickHouse/abc, got: %d/%s/%s", col1, col2, col3)
	}

	// Truncate
	err = conn.TruncateTable("", tableName)
	if err != nil {
		return fmt.Errorf("error while truncating table %s: %w", tableName, err)
	}

	// Check the table is empty
	row = conn.QueryRow(conn.ctx, fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName))
	var (
		count uint64
	)
	if err := row.Scan(&count); err != nil {
		return fmt.Errorf("error while scanning count from %s: %w", tableName, err)
	}
	if count != 0 {
		return fmt.Errorf("truncated table count is not zero, got: %d", count)
	}

	// Drop
	if err := conn.Exec(conn.ctx, fmt.Sprintf("DROP TABLE `%s`", tableName)); err != nil {
		return fmt.Errorf("error while dropping table %s: %w", tableName, err)
	}

	// Check the table does not exist
	row = conn.QueryRow(conn.ctx, fmt.Sprintf("SELECT COUNT(*) FROM system.tables WHERE name = '%s'", tableName))
	if err := row.Scan(&count); err != nil {
		return fmt.Errorf("error while scanning count from system.tables: %w", err)
	}
	if count != 0 {
		return fmt.Errorf("table %s still exists after drop", tableName)
	}

	LogInfo("Mutation check passed")
	return nil
}
