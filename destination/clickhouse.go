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

func GetWithDefault(configuration map[string]string, key string, default_ string) string {
	value, ok := configuration[key]
	if !ok || value == "" {
		return default_
	}
	return value
}

func (conn *ClickHouseConnection) DescribeTable(schemaName string, tableName string) (*TableDescription, error) {
	query := fmt.Sprintf("SELECT name, type, is_in_primary_key FROM system.columns WHERE database = '%s' AND table = '%s'", schemaName, tableName)
	logger.Printf("Executing query: %s", query)

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
	fullName := GetFullTableName(schemaName, tableName)
	logger.Printf("Creating table %s with columns %v", fullName, tableDescription.Columns)

	var orderByCols []string
	var columnsBuilder strings.Builder
	count := 0
	for _, col := range tableDescription.Columns {
		columnsBuilder.WriteString(fmt.Sprintf("%s %s", col.Name, col.Type))
		if col.IsPrimaryKey {
			orderByCols = append(orderByCols, col.Name)
		}
		if count < len(tableDescription.Columns)-1 {
			columnsBuilder.WriteString(", ")
		}
		count++
	}
	columns := columnsBuilder.String()

	query := fmt.Sprintf("CREATE TABLE %s (%s) ENGINE = ReplacingMergeTree(%s) ORDER BY (%s)",
		fullName, columns, FivetranSynced, strings.Join(orderByCols, ", "))
	logger.Printf("Executing query: %s", query)

	if err := conn.Exec(conn.ctx, query); err != nil {
		return err
	}
	return nil
}

func (conn *ClickHouseConnection) AlterTable(schemaName string, tableName string, ops []*AlterTableOp) error {
	fullName := GetFullTableName(schemaName, tableName)
	logger.Printf("Altering table %s", fullName)

	total := len(ops)
	if total == 0 {
		logger.Printf("No statements to execute for altering table %s", fullName)
		return nil
	}

	count := 0
	var statementsBuilder strings.Builder
	for _, op := range ops {
		switch op.Op {
		case Add:
			statementsBuilder.WriteString(fmt.Sprintf("ADD COLUMN %s %s", op.Column, *op.Type))
		case Modify:
			statementsBuilder.WriteString(fmt.Sprintf("MODIFY COLUMN %s %s", op.Column, *op.Type))
		case Drop:
			statementsBuilder.WriteString(fmt.Sprintf("DROP COLUMN %s", op.Column))
		}
		if count < total-1 {
			statementsBuilder.WriteString(", ")
		}
		count++
	}

	statements := statementsBuilder.String()
	query := fmt.Sprintf("ALTER TABLE %s %s", fullName, statements)
	logger.Printf("Executing query: %s", query)

	if err := conn.Exec(conn.ctx, query); err != nil {
		return err
	}
	return nil
}

func (conn *ClickHouseConnection) TruncateTable(schemaName string, tableName string) error {
	fullName := GetFullTableName(schemaName, tableName)
	logger.Printf("Truncating %s", fullName)

	query := fmt.Sprintf("TRUNCATE TABLE %s", fullName)
	logger.Printf("Executing query: %s", query)

	if err := conn.Exec(conn.ctx, query); err != nil {
		return err
	}
	return nil
}

func (conn *ClickHouseConnection) InsertBatch(fullTableName string, rows [][]interface{}) error {
	batch, err := conn.PrepareBatch(conn.ctx, fmt.Sprintf("INSERT INTO %s", fullTableName))
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := batch.Append(row...); err != nil {
			fmt.Printf("batch append error: %v\n", err)
			return err
		}
	}
	if err := batch.Send(); err != nil {
		fmt.Printf("batch send error: %v\n", err)
		return err
	}
	return nil
}

func (conn *ClickHouseConnection) GetColumnTypes(schemaName string, tableName string) ([]driver.ColumnType, error) {
	fullName := GetFullTableName(schemaName, tableName)
	rows, err := conn.Query(conn.ctx, fmt.Sprintf("SELECT * FROM %s WHERE false", fullName))
	if err != nil {
		return nil, err
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
		mappingKey := GetDatabaseRowMappingKey(scanRows[i], pkCols)
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
	fullName := GetFullTableName(schemaName, table.Name)
	for i := 0; i < len(csv); i += batchSize {
		end := i + batchSize
		if end > len(csv) {
			end = len(csv)
		}
		batch := csv[i:end]
		insertRows := make([][]interface{}, len(batch))
		for i, csvRow := range batch {
			insertRow, err := CSVRowToInsertValues(csvRow, table, nullStr)
			if err != nil {
				return err
			}
			insertRows[i] = insertRow
		}
		err := conn.InsertBatch(fullName, insertRows)
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
	fullName := GetFullTableName(schemaName, table.Name)
	for i := 0; i < len(csv); i += batchSize {
		end := i + batchSize
		if end > len(csv) {
			end = len(csv)
		}
		batch := csv[i:end]
		selectRows, err := conn.SelectByPrimaryKeys(fullName, columnTypes, pkCols, batch)
		if err != nil {
			return err
		}
		insertRows := make([][]interface{}, len(batch))
		for i, csvRow := range batch {
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
				insertRows[i] = updatedRow
			} else {
				insertRow, err := CSVRowToInsertValues(csvRow, table, nullStr)
				if err != nil {
					return err
				}
				insertRows[i] = insertRow
			}
		}
		err = conn.InsertBatch(fullName, insertRows)
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
	fullName := GetFullTableName(schemaName, table.Name)
	for i := 0; i < len(csv); i += batchSize {
		end := i + batchSize
		if end > len(csv) {
			end = len(csv)
		}
		batch := csv[i:end]
		selectRows, err := conn.SelectByPrimaryKeys(fullName, columnTypes, pkCols, batch)
		if err != nil {
			return err
		}
		insertRows := make([][]interface{}, len(batch))
		for i, csvRow := range batch {
			mappingKey, err := GetCSVRowMappingKey(csvRow, pkCols)
			if err != nil {
				return err
			}
			dbRow, exists := selectRows[mappingKey]
			if exists {
				updatedRow, err := CSVRowToSoftDeletedRow(csvRow, dbRow, fivetranSyncedIdx, fivetranDeletedIdx)
				if err != nil {
					return err
				}
				insertRows[i] = updatedRow
			} else {
				// Shouldn't happen
				logger.Printf("Row with PK mapping %s does not exist", mappingKey)
				continue
			}
		}
		err = conn.InsertBatch(fullName, insertRows)
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

	logger.Printf("Connection check passed")
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
		return err
	}

	// Insert test Data
	logger.Printf("Inserting test Data into %s", tableName)
	batch, err := conn.PrepareBatch(conn.ctx, fmt.Sprintf("INSERT INTO `%s`", tableName))
	if err != nil {
		return err
	}
	now := time.Now()
	err = batch.Append(uint8(42), "ClickHouse", now)
	if err != nil {
		return err
	}
	err = batch.Send()
	if err != nil {
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
	logger.Printf("Checking test Data in %s", tableName)
	row := conn.QueryRow(conn.ctx, fmt.Sprintf("SELECT * FROM `%s`", tableName))
	var (
		col1 uint8
		col2 string
		col3 *time.Time
	)
	if err := row.Scan(&col1, &col2, &col3); err != nil {
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
	logger.Printf("Checking if %s is empty", tableName)
	row = conn.QueryRow(conn.ctx, fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName))
	var (
		count uint64
	)
	if err := row.Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return fmt.Errorf("truncated table count is not zero, got: %d", count)
	}

	// Drop
	logger.Printf("Dropping %s", tableName)
	if err := conn.Exec(conn.ctx, fmt.Sprintf("DROP TABLE `%s`", tableName)); err != nil {
		return err
	}

	// Check the table does not exist
	logger.Printf("Checking that %s does not exist", tableName)
	row = conn.QueryRow(conn.ctx, fmt.Sprintf("SELECT COUNT(*) FROM system.tables WHERE name = '%s'", tableName))
	if err := row.Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return fmt.Errorf("table %s still exists after drop", tableName)
	}

	logger.Printf("Mutation check passed")
	return nil
}
