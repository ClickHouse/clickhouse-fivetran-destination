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

func (conn *ClickHouseConnection) DescribeTable(database string, tableName string) (map[string]string, error) {
	rows, err := conn.Query(fmt.Sprintf("DESCRIBE `%s`.`%s`", database, tableName))
	if err != nil {
		return nil, err
	}
	var (
		colName string
		colType string
		ignore  string
	)
	result := make(map[string]string)
	for rows.Next() {
		if err := rows.Scan(&colName, &colType, &ignore, &ignore, &ignore, &ignore, &ignore); err != nil {
			return nil, err
		}
		result[colName] = colType
	}
	return result, nil
}

func (conn *ClickHouseConnection) ConnectionTest() error {
	describeResult, err := conn.DescribeTable("system", "numbers")
	if err != nil {
		return err
	}
	colType, exists := describeResult["number"]

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
	logger.Printf("Creating table %s", tableName)
	createTableQuery := fmt.Sprintf("CREATE TABLE %s (Col1 UInt8, Col2 String) Engine = Memory", tableName)
	if _, err := conn.Exec(createTableQuery); err != nil {
		return err
	}

	// Insert test data
	logger.Printf("Inserting test data into %s", tableName)
	scope, err := conn.Begin()
	if err != nil {
		return err
	}
	batch, err := scope.Prepare(fmt.Sprintf("INSERT INTO %s", tableName))
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
	logger.Printf("Alter %s.Col1 into UInt16", tableName)
	alterTableQuery := fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN Col1 UInt16", tableName)
	if _, err := conn.Exec(alterTableQuery); err != nil {
		return err
	}

	// Check the inserted data
	logger.Printf("Checking test data in %s", tableName)
	row := conn.QueryRow(fmt.Sprintf("SELECT * FROM %s", tableName))
	var (
		col1 uint16
		col2 string
	)
	if err := row.Scan(&col1, &col2); err != nil {
		return err
	}
	if col1 != 42 || col2 != "ClickHouse" {
		return errors.New(
			fmt.Sprintf("Unexpected data check output, expected 42/ClickHouse, got: %d/%s", col1, col2))
	}

	// Truncate
	logger.Printf("Truncating %s", tableName)
	if _, err := conn.Exec(fmt.Sprintf("TRUNCATE TABLE %s", tableName)); err != nil {
		return err
	}

	// Check the table is empty
	logger.Printf("Checking if %s is empty", tableName)
	row = conn.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName))
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
	if _, err := conn.Exec(fmt.Sprintf("DROP TABLE %s", tableName)); err != nil {
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
