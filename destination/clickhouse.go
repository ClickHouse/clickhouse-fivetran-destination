package main

import (
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
)

type ClickHouseConnection struct {
	*sql.DB
}

func GetClickHouseConnection(configuration map[string]string) *ClickHouseConnection {
	options := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%s",
			GetWithDefault(configuration, "host", "localhost"),
			GetWithDefault(configuration, "port", "9000"))},
		Auth: clickhouse.Auth{
			Username: GetWithDefault(configuration, "username", "default"),
			Password: GetWithDefault(configuration, "password", ""),
			Database: GetWithDefault(configuration, "database", "default"),
		},
		Protocol: clickhouse.Native,
		Settings: clickhouse.Settings{
			"allow_experimental_object_type": 1,
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
	if GetWithDefault(configuration, "ssl", "false") == "true" {
		options.TLS = &tls.Config{InsecureSkipVerify: true}
	}
	conn := clickhouse.OpenDB(options)
	conn.SetMaxOpenConns(1)
	return &ClickHouseConnection{conn}
}

func GetWithDefault(configuration map[string]string, key string, default_ string) string {
	value, ok := configuration[key]
	if !ok || value == "" {
		return default_
	}
	return value
}

func (conn *ClickHouseConnection) DescribeTable(schemaName string, tableName string) (*TableDescription, error) {
	fullName := GetFullTableName(schemaName, tableName)
	query := fmt.Sprintf("DESCRIBE %s", fullName)
	logger.Printf("Executing query: %s", query)

	rows, err := conn.Query(query)
	if err != nil {
		return nil, err
	}
	var (
		colName string
		colType string
		ignore  string
	)
	var columns []*ColumnDefinition
	for rows.Next() {
		if err := rows.Scan(&colName, &colType, &ignore, &ignore, &ignore, &ignore, &ignore); err != nil {
			return nil, err
		}
		columns = append(columns, &ColumnDefinition{
			Name: colName,
			Type: colType,
		})
	}
	return MakeTableDescription(columns), nil
}

func (conn *ClickHouseConnection) CreateTable(schemaName string, tableName string, tableDescription *TableDescription, engine string) error {
	fullName := GetFullTableName(schemaName, tableName)
	logger.Printf("Creating table %s with columns %s", fullName, tableDescription)

	var columnsBuilder strings.Builder
	count := 0
	for _, colName := range tableDescription.Columns {
		colType := tableDescription.Mapping[colName]
		columnsBuilder.WriteString(fmt.Sprintf("%s %s", colName, colType))
		if count < len(tableDescription.Columns)-1 {
			columnsBuilder.WriteString(", ")
		}
		count++
	}

	columns := columnsBuilder.String()
	query := fmt.Sprintf("CREATE TABLE %s (%s) Engine = %s", fullName, columns, engine)
	logger.Printf("Executing query: %s", query)

	if _, err := conn.Exec(query); err != nil {
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

	if _, err := conn.Exec(query); err != nil {
		return err
	}
	return nil
}

func (conn *ClickHouseConnection) TruncateTable(schemaName string, tableName string) error {
	fullName := GetFullTableName(schemaName, tableName)
	logger.Printf("Truncating %s", fullName)

	query := fmt.Sprintf("TRUNCATE TABLE %s", fullName)
	logger.Printf("Executing query: %s", query)

	if _, err := conn.Exec(query); err != nil {
		return err
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
		return errors.New(
			fmt.Sprintf(
				"Unexpected describe system.numbers output, expected result map to contain number:UInt64, got: %s",
				describeResult))
	}

	logger.Printf("Connection check passed")
	return nil
}

func (conn *ClickHouseConnection) MutationTest() error {
	id := strings.Replace(uuid.New().String(), "-", "", -1)
	tableName := fmt.Sprintf("fivetran_destination_test_%s", id)

	// Create test table
	err := conn.CreateTable("", tableName, MakeTableDescription([]*ColumnDefinition{
		{Name: "Col1", Type: "UInt8"},
		{Name: "Col2", Type: "String"},
	}), "Memory")
	if err != nil {
		return err
	}

	// Insert test Data
	logger.Printf("Inserting test Data into %s", tableName)
	scope, err := conn.Begin()
	if err != nil {
		return err
	}
	batch, err := scope.Prepare(fmt.Sprintf("INSERT INTO `%s`", tableName))
	if err != nil {
		return err
	}
	_, err = batch.Exec(uint8(42), "ClickHouse")
	if err != nil {
		return err
	}
	err = scope.Commit()
	if err != nil {
		return err
	}

	// Alter
	dbType := "UInt16"
	err = conn.AlterTable("", tableName, []*AlterTableOp{
		{Op: Modify, Column: "Col1", Type: &dbType},
	})
	if err != nil {
		return err
	}

	// Check the inserted Data
	logger.Printf("Checking test Data in %s", tableName)
	row := conn.QueryRow(fmt.Sprintf("SELECT * FROM `%s`", tableName))
	var (
		col1 uint16
		col2 string
	)
	if err := row.Scan(&col1, &col2); err != nil {
		return err
	}
	if col1 != 42 || col2 != "ClickHouse" {
		return errors.New(
			fmt.Sprintf("Unexpected Data check output, expected 42/ClickHouse, got: %d/%s", col1, col2))
	}

	// Truncate
	err = conn.TruncateTable("", tableName)
	if err != nil {
		return err
	}

	// Check the table is empty
	logger.Printf("Checking if %s is empty", tableName)
	row = conn.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName))
	var (
		count uint64
	)
	if err := row.Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return errors.New(fmt.Sprintf("Truncated table count is not zero, got: %d", count))
	}

	// Drop
	logger.Printf("Dropping %s", tableName)
	if _, err := conn.Exec(fmt.Sprintf("DROP TABLE `%s`", tableName)); err != nil {
		return err
	}

	// Check the table does not exist
	logger.Printf("Checking that %s does not exist", tableName)
	row = conn.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM system.tables WHERE name = '%s'", tableName))
	if err := row.Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return errors.New(fmt.Sprintf("Table %s still exists after drop", tableName))
	}

	logger.Printf("Mutation check passed")
	return nil
}

func GetFullTableName(schemaName string, tableName string) string {
	var fullName string
	if schemaName == "" {
		fullName = fmt.Sprintf("`%s`", tableName)
	} else {
		fullName = fmt.Sprintf("`%s`.`%s`", schemaName, tableName)
	}
	return fullName
}
