package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fivetran.com/fivetran_sdk/destination/cmd"
	"fivetran.com/fivetran_sdk/destination/common/flags"
	"fivetran.com/fivetran_sdk/destination/db/config"
	"github.com/stretchr/testify/require"
)

// Runs the destination app, invokes SDK tester with a given input file, and verifies the output from ClickHouse.
// See also:
// - https://github.com/fivetran/fivetran_sdk/tree/main/tools/destination-tester for the SDK tester docs
// - Makefile:test for the SDK tester run command
// - sdk_tests/*.json for the input files

func TestAllDataTypes(t *testing.T) {
	fileName := "input_all_data_types.json"
	tableName := "all_data_types"
	startServer(t)
	runSDKTestCommand(t, fileName, true)
	assertTableRowsWithFivetranId(t, tableName, [][]string{
		{"true", "42", "144", "100500", "100.5", "200.5", "42.42",
			"2024-05-07", "2024-04-05 15:33:14", "2024-02-03 12:44:22.123456789",
			"foo", "{\"a\": 1,\"b\": 2}", "<a>1</a>", "FFFA", "15:00", "abc-123-xyz"},
		{"false", "-42", "-144", "-100500", "-100.5", "-200.5", "-42.42",
			"2021-02-03", "2021-06-15 04:15:16", "2021-02-03 14:47:45.234567890",
			"bar", "{\"c\": 3,\"d\": 4}", "<b>42</b>", "FFFE", "12:42", "vbn-543-hjk"}})
	assertTableColumns(t, tableName, [][]string{
		{"b", "Nullable(Bool)", ""},
		{"i16", "Nullable(Int16)", ""},
		{"i32", "Nullable(Int32)", ""},
		{"i64", "Nullable(Int64)", ""},
		{"f32", "Nullable(Float32)", ""},
		{"f64", "Nullable(Float64)", ""},
		{"dec", "Nullable(Decimal(10, 4))", ""},
		{"d", "Nullable(Date32)", ""},
		{"dt", "Nullable(DateTime64(0, 'UTC'))", ""},
		{"utc", "Nullable(DateTime64(9, 'UTC'))", ""},
		{"s", "Nullable(String)", ""},
		{"j", "Nullable(String)", "JSON"},
		{"x", "Nullable(String)", "XML"},
		{"bin", "Nullable(String)", "BINARY"},
		{"nt", "Nullable(String)", "NAIVE_TIME"},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""},
		{"_fivetran_id", "String", ""}})
}

func TestMutateAfterAlter(t *testing.T) {
	fileName := "input_mutate_after_alter.json"
	tableName := "mutate_after_alter"
	startServer(t)
	runSDKTestCommand(t, fileName, true)
	assertTableRowsWithPK(t, tableName, [][]string{
		{"1", "200", "asd", "zxc", "\\N", "\\N"},
		{"2", "50", "\\N", "\\N", "<c>99</c>", "DD"},
		{"4", "20.5", "x", "\\N", "<d>77</d>", "\\N"}})
	assertTableColumns(t, tableName, [][]string{
		{"id", "Int32", ""},
		{"amount", "Nullable(Float32)", ""},
		{"s1", "Nullable(String)", ""},
		{"s2", "Nullable(String)", ""},
		{"s3", "Nullable(String)", "XML"},
		{"s4", "Nullable(String)", "BINARY"},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""}})
}

func TestUpdateAndHardDelete(t *testing.T) {
	fileName := "input_update_and_hard_delete.json"
	tableName := "update_and_delete"
	startServer(t)
	runSDKTestCommand(t, fileName, true)
	assertTableRowsWithPK(t, tableName, [][]string{
		{"1", "1111"},
		{"2", "two"}})
	assertTableColumns(t, tableName, [][]string{
		{"id", "Int32", ""},
		{"name", "Nullable(String)", ""},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""}})
}

func TestSoftDelete(t *testing.T) {
	fileName := "input_soft_delete.json"
	tableName := "soft_delete_table"
	startServer(t)
	runSDKTestCommand(t, fileName, true)
	assertTableRowsWithPK(t, tableName, [][]string{
		{"1", "\\N", "false"},
		{"2", "two", "false"},
		{"3", "three", "true"},
		{"4", "four", "true"},
		{"5", "\\N", "true"}})
	assertTableColumns(t, tableName, [][]string{
		{"id", "Int32", ""},
		{"name", "Nullable(String)", ""},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""},
		{"_fivetran_deleted", "Bool", ""}})
}

func TestUTCDateTimePrimaryKey(t *testing.T) {
	fileName := "input_utc_datetime_pk.json"
	tableName := "utc_datetime_pk"
	startServer(t)
	runSDKTestCommand(t, fileName, true)
	assertTableRowsWithPKColumns(t, tableName, [][]string{
		{"144", "2024-01-14 15:13:12.000000000"},
		{"2", "2024-01-14 15:13:12.123000000"}},
		"ts")
	assertTableColumns(t, tableName, [][]string{
		{"i", "Nullable(Int32)", ""},
		{"ts", "DateTime64(9, 'UTC')", ""},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""}})
}

func TestNaiveDateTimePK(t *testing.T) {
	fileName := "input_naive_datetime_pk.json"
	tableName := "naive_datetime_pk"
	startServer(t)
	runSDKTestCommand(t, fileName, true)
	assertTableRowsWithPKColumns(t, tableName, [][]string{
		{"144", "2022-06-01 18:44:13"}},
		"dt")
	assertTableColumns(t, tableName, [][]string{
		{"i", "Nullable(Int32)", ""},
		{"dt", "DateTime64(0, 'UTC')", ""},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""}})
}

func TestNaiveDatePK(t *testing.T) {
	fileName := "input_naive_date_pk.json"
	tableName := "naive_date_pk"
	startServer(t)
	runSDKTestCommand(t, fileName, true)
	assertTableRowsWithPKColumns(t, tableName, [][]string{
		{"144", "2022-06-01"}},
		"d")
	assertTableColumns(t, tableName, [][]string{
		{"i", "Nullable(Int32)", ""},
		{"d", "Date32", ""},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""}})
}

func TestCompositeFloatPK(t *testing.T) {
	fileName := "input_composite_floats_pk.json"
	tableName := "composite_floats_pk"
	startServer(t)
	runSDKTestCommand(t, fileName, true)
	assertTableRowsWithPKColumns(t, tableName, [][]string{
		{"144", "300.3", "3.3", "4.4"}},
		"dec, f32, f64")
	assertTableColumns(t, tableName, [][]string{
		{"i", "Nullable(Int32)", ""},
		{"dec", "Decimal(10, 4)", ""},
		{"f32", "Float32", ""},
		{"f64", "Float64", ""},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""}})
}

func TestStringPK(t *testing.T) {
	fileName := "input_string_pk.json"
	tableName := "string_pk"
	startServer(t)
	runSDKTestCommand(t, fileName, true)
	assertTableRowsWithPKColumns(t, tableName, [][]string{
		{"144", "qaz"}},
		"s")
	assertTableColumns(t, tableName, [][]string{
		{"i", "Nullable(Int32)", ""},
		{"s", "String", ""},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""}})
}

func TestCompositePKWithBoolean(t *testing.T) {
	fileName := "input_composite_pk_with_boolean.json"
	tableName := "composite_pk_with_boolean"
	startServer(t)
	runSDKTestCommand(t, fileName, true)
	assertTableRowsWithPKColumns(t, tableName, [][]string{
		{"144", "3", "true"}}, "l, b")
	assertTableColumns(t, tableName, [][]string{
		{"i", "Nullable(Int32)", ""},
		{"l", "Int64", ""},
		{"b", "Bool", ""},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""}})
}

func TestNonExistentRecordUpdatesAndDeletes(t *testing.T) {
	fileName := "input_non_existent_updates.json"
	tableName := "non_existent_updates"
	startServer(t)
	runSDKTestCommand(t, fileName, true)
	assertTableRowsWithPK(t, tableName, [][]string{
		{"1", "\\N"}})
	assertTableColumns(t, tableName, [][]string{
		{"id", "Int32", ""},
		{"name", "Nullable(String)", ""},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""}})
}

func TestSoftTruncateBefore(t *testing.T) {
	fileName := "input_soft_truncate_before.json"
	tableName := "table_to_truncate"
	startServer(t)
	runSDKTestCommand(t, fileName, true)
	assertTableRowsWithPK(t, tableName, [][]string{
		{"1", "foo", "true"},
		{"2", "bar", "false"},
		{"3", "qaz", "false"}})
	assertTableColumns(t, tableName, [][]string{
		{"id", "Int32", ""},
		{"name", "Nullable(String)", ""},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""},
		{"_fivetran_deleted", "Bool", ""}})
}

func TestHardTruncateBefore(t *testing.T) {
	fileName := "input_hard_truncate_before.json"
	tableName := "table_to_truncate"
	startServer(t)
	runSDKTestCommand(t, fileName, true)
	assertTableRowsWithPK(t, tableName, [][]string{
		{"2", "bar"},
		{"3", "qaz"}})
	assertTableColumns(t, tableName, [][]string{
		{"id", "Int32", ""},
		{"name", "Nullable(String)", ""},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""}})
}

func TestTableNotFound(t *testing.T) {
	fileName := "input_table_not_found.json"
	startServer(t)
	runSDKTestCommand(t, fileName, true) // verify at least no SDK tester errors
}

func TestChangePK(t *testing.T) {
	fileName := "input_change_pk.json"
	tableName := "change_pk"
	startServer(t)
	runSDKTestCommand(t, fileName, true)
	assertTableRowsWithPKColumns(t, tableName, [][]string{
		{"1", "200", "foo"},
		{"2", "50", "bar"},
		{"4", "20.5", "qaz"}},
		"id, s")
	assertTableColumns(t, tableName, [][]string{
		{"id", "Int32", ""},
		{"amount", "Nullable(Float32)", ""},
		{"s", "String", ""}, // non-nullable, now a PK
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""}})
}

func TestDropPK(t *testing.T) {
	fileName := "input_drop_pk.json"
	tableName := "drop_pk"
	startServer(t)
	runSDKTestCommand(t, fileName, true)
	assertTableRowsWithPKColumns(t, tableName, [][]string{
		{"1", "200", "foo"},
		{"2", "50", "bar"},
		{"4", "20.5", "qaz"}},
		"id")
	assertTableColumns(t, tableName, [][]string{
		{"id", "Int32", ""},
		{"amount", "Nullable(Float32)", ""},
		{"s", "Nullable(String)", ""}, // was PK, now it is Nullable (as a non-PK column)
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""}})
}

func TestChangePKAndAllColumns(t *testing.T) {
	fileName := "input_change_pk_and_all_columns.json"
	tableName := "change_pk_and_all_columns"
	startServer(t)
	runSDKTestCommand(t, fileName, true)
	assertTableRowsWithPKColumns(t, tableName, [][]string{
		{"1", "foo"},
		{"2", "bar"},
		{"4", "qaz"}},
		"id")
	assertTableColumns(t, tableName, [][]string{
		{"id", "Int32", ""}, // the only PK now
		{"s", "Nullable(String)", ""},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""}})
}

func TestTruncateDateValues(t *testing.T) {
	fileName := "input_truncate_date_values.json"
	tableName := "truncate_date_values"
	startServer(t)
	runSDKTestCommand(t, fileName, true)
	assertTableRowsWithPK(t, tableName, [][]string{
		{"1", "1900-01-01", "1900-01-01 00:00:00", "1900-01-01 00:00:00.000000000"},
		{"2", "2299-12-31", "2262-04-11 23:47:16", "2262-04-11 23:47:16.000000000"}})
	assertTableColumns(t, tableName, [][]string{
		{"id", "Int32", ""},
		{"d", "Nullable(Date32)", ""},
		{"dt", "Nullable(DateTime64(0, 'UTC'))", ""},
		{"utc", "Nullable(DateTime64(9, 'UTC'))", ""},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""}})
}

func TestLargeInputFile(t *testing.T) {
	t.Skip("Skip large input file test - SDK tester hangs")
	tableName := "input_large_file"
	startServer(t)

	expectedCSV := generateAndWriteInputFile(t, tableName, 150_000)
	runSDKTestCommand(t, fmt.Sprintf("%s.json", tableName), true)

	dbRecordsCSVStr := runQuery(t, fmt.Sprintf("SELECT * EXCEPT _fivetran_synced FROM tester.%s FINAL ORDER BY id FORMAT CSV", tableName))
	assertDatabaseRecordsFailFast(t, expectedCSV, dbRecordsCSVStr)
	assertTableColumns(t, tableName, [][]string{
		{"id", "Int64", ""},
		{"data", "Nullable(String)", ""},
		{"created_at", "Nullable(DateTime64(0, 'UTC'))", ""},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""}})
}

func TestHistoryMode(t *testing.T) {
	fileName := "input_history_mode.json"
	tableName := "users"
	startServer(t)
	runSDKTestCommand(t, fileName, true)
	// Verify table columns include history mode columns
	assertTableColumns(t, tableName, [][]string{
		{"id", "Int32", ""},
		{"name", "Nullable(String)", ""},
		{"status", "Nullable(String)", ""},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""},
		{"_fivetran_start", "DateTime64(9, 'UTC')", ""},
		{"_fivetran_end", "Nullable(DateTime64(9, 'UTC'))", ""},
		{"_fivetran_active", "Nullable(Bool)", ""}})

	// Verify that records were inserted with correct history mode tracking
	// Query all columns including history tracking fields
	query := fmt.Sprintf("SELECT id, name, status, _fivetran_start, _fivetran_end, _fivetran_active FROM tester.%s FINAL ORDER BY id FORMAT CSV SETTINGS select_sequential_consistency=1", tableName)
	dbRecordsCSVStr := runQuery(t, query)
	assertDatabaseRecords(t, [][]string{
		{"1", "name 1", "TODO", "2025-11-10 20:57:00.000000000", "2025-11-11 20:56:59.999000000", "false"},
		{"1", "name 11", "TODO", "2025-11-11 20:57:00.000000000", "2262-04-11 23:47:16.000000000", "true"},
		{"2", "name 2", "TODO", "2025-11-10 20:57:00.000000000", "2025-11-11 20:56:59.999000000", "false"},
		{"2", "name 22", "TODO", "2025-11-11 20:57:00.000000000", "2262-04-11 23:47:16.000000000", "true"},
		{"3", "name 3", "TODO", "2025-11-10 20:57:00.000000000", "2262-04-11 23:47:16.000000000", "true"},
		{"4", "name 4", "TODO", "2025-11-10 20:57:00.000000000", "2262-04-11 23:47:16.000000000", "true"},
		{"5", "name 5", "TODO", "2025-11-10 20:57:00.000000000", "2262-04-11 23:47:16.000000000", "true"}}, dbRecordsCSVStr)
}

// fail on the first mismatch, to prevent long console output
func assertDatabaseRecordsFailFast(t *testing.T, expectedRecords [][]string, dbRecordsCSVStr string) {
	csvReader := csv.NewReader(strings.NewReader(dbRecordsCSVStr))
	for _, expected := range expectedRecords {
		dbRecord, _ := csvReader.Read()
		require.Equal(t, expected, dbRecord)
	}
}

func assertDatabaseRecords(t *testing.T, expectedRecords [][]string, dbRecordsCSVStr string) {
	dbRecords, err := csv.NewReader(strings.NewReader(dbRecordsCSVStr)).ReadAll()
	require.NoError(t, err)
	require.Equal(t, expectedRecords, dbRecords)
}

func assertTableRowsWithPK(t *testing.T, tableName string, expectedOutput [][]string) {
	query := fmt.Sprintf("SELECT * EXCEPT _fivetran_synced FROM tester.%s FINAL ORDER BY id FORMAT CSV SETTINGS select_sequential_consistency=1", tableName)
	dbRecordsCSVStr := runQuery(t, query)
	assertDatabaseRecords(t, expectedOutput, dbRecordsCSVStr)
}

func assertTableRowsWithPKColumns(t *testing.T, tableName string, expectedOutput [][]string, pkCol string) {
	query := fmt.Sprintf("SELECT * EXCEPT _fivetran_synced FROM tester.%s FINAL ORDER BY %s FORMAT CSV SETTINGS select_sequential_consistency=1", tableName, pkCol)
	dbRecordsCSVStr := runQuery(t, query)
	assertDatabaseRecords(t, expectedOutput, dbRecordsCSVStr)
}

func assertTableRowsWithFivetranId(t *testing.T, tableName string, expectedOutput [][]string) {
	query := fmt.Sprintf("SELECT * EXCEPT _fivetran_synced FROM tester.%s FINAL ORDER BY _fivetran_id FORMAT CSV SETTINGS select_sequential_consistency=1", tableName)
	dbRecordsCSVStr := runQuery(t, query)
	assertDatabaseRecords(t, expectedOutput, dbRecordsCSVStr)
}

func assertTableColumns(t *testing.T, tableName string, expectedOutput [][]string) {
	query := fmt.Sprintf("SELECT name, type, comment FROM system.columns WHERE database = 'tester' AND table = '%s' FORMAT CSV", tableName)
	dbRecordsCSVStr := runQuery(t, query)
	assertDatabaseRecords(t, expectedOutput, dbRecordsCSVStr)
}

type TableDefinitionColumns struct {
	Id        string `json:"id"`
	Data      string `json:"data,omitempty"`
	CreatedAt string `json:"created_at,omitempty"`
}

type TableDefinition struct {
	Columns    TableDefinitionColumns `json:"columns"`
	PrimaryKey []string               `json:"primary_key"`
}

type Row struct {
	Id              uint    `json:"id"`
	Data            *string `json:"data,omitempty"`
	CreatedAt       *string `json:"created_at,omitempty"`
	FivetranDeleted *bool   `json:"_fivetran_deleted,omitempty"`
}

type Operation struct {
	Upsert map[string][]Row `json:"upsert,omitempty"`
	Update map[string][]Row `json:"update,omitempty"`
	Delete map[string][]Row `json:"delete,omitempty"`
}

type InputFile struct {
	CreateTable   map[string]TableDefinition `json:"create_table"`
	DescribeTable []string                   `json:"describe_table"`
	Ops           []Operation                `json:"ops"`
}

func generateAndWriteInputFile(t *testing.T, tableName string, n uint) [][]string {
	rows := make([]Row, n)
	updateRows := make([]Row, n/20)
	deleteRows := make([]Row, n/50)
	assertRows := make([][]string, n) // should exactly match ClickHouse CSV format output after input.json is processed
	createdAt := time.Date(2021, 2, 15, 14, 13, 12, 0, time.UTC)
	for i := uint(0); i < n; i++ {
		rowCreatedAt := createdAt.Add(time.Duration(i) * time.Second)
		createdAtStr := rowCreatedAt.Format("2006-01-02T15:04:05")
		data := fmt.Sprintf("original for %d", i)
		row := Row{Id: i, Data: &data, CreatedAt: &createdAtStr}
		rows[i] = row
		// id, data, created_at, _fivetran_deleted
		assertRows[i] = make([]string, 4)
		assertRows[i][0] = fmt.Sprintf("%d", row.Id)
		assertRows[i][1] = *row.Data
		assertRows[i][2] = rowCreatedAt.Format("2006-01-02 15:04:05") // ClickHouse "simple" format (no T)
		assertRows[i][3] = "false"
		if i%20 == 0 {
			j := i / 20
			updatedData := fmt.Sprintf("updated for %d", i)
			updateRows[j] = Row{Id: row.Id, Data: &updatedData}
			assertRows[i][1] = updatedData
		}
		if i%50 == 0 {
			j := i / 50
			deleteRows[j] = Row{Id: row.Id}
			assertRows[i][3] = "true"
		}
	}
	cwd := getProjectRootDir(t)
	inputFile := InputFile{
		CreateTable: map[string]TableDefinition{
			tableName: {
				Columns:    TableDefinitionColumns{Id: "LONG", Data: "STRING", CreatedAt: "NAIVE_DATETIME"},
				PrimaryKey: []string{"id"},
			},
		},
		DescribeTable: []string{tableName},
		Ops: []Operation{
			{Upsert: map[string][]Row{tableName: rows}},
			{Update: map[string][]Row{tableName: updateRows}},
			{Delete: map[string][]Row{tableName: deleteRows}},
		},
	}
	content, err := json.Marshal(inputFile)
	require.NoError(t, err)
	err = os.WriteFile(fmt.Sprintf("%s/sdk_tests/%s.json", cwd, tableName), content, 0644)
	require.NoError(t, err)
	return assertRows
}

const dialTimeout = 10 * time.Millisecond
const maxDialRetries = 300

var configMap atomic.Value
var connConfig atomic.Value

func startServer(t *testing.T) {
	if isPortReady(t, *flags.Port) {
		return
	}
	go cmd.StartServer()
	waitPortIsReady(t, *flags.Port)
}

func getProjectRootDir(t *testing.T) string {
	cwd, err := os.Getwd()
	require.NoError(t, err)
	var result string
	// CLI vs IDE test run
	if strings.HasSuffix(cwd, "/destination") {
		result = cwd[:len(cwd)-12]
	}
	return result
}

func readConfigMap(t *testing.T) map[string]string {
	if configMap.Load() != nil {
		return configMap.Load().(map[string]string)
	}
	rootDir := getProjectRootDir(t)
	configBytes, err := os.ReadFile(fmt.Sprintf("%s/sdk_tests/configuration.json", rootDir))
	require.NoError(t, err,
		"copy the default configuration first: cp sdk_tests/default_configuration.json sdk_tests/configuration.json")
	m := make(map[string]string)
	err = json.Unmarshal(configBytes, &m)
	require.NoError(t, err)
	configMap.Store(m)
	return m
}

func readConfig(t *testing.T) *config.Config {
	if connConfig.Load() != nil {
		return connConfig.Load().(*config.Config)
	}
	m := readConfigMap(t)
	res, err := config.Parse(m)
	require.NoError(t, err)
	connConfig.Store(res)
	return res
}

func runQuery(t *testing.T, query string) string {
	conf := readConfig(t)
	cmdArgs := []string{
		"exec", "fivetran-destination-clickhouse-server",
		"clickhouse-client", "--query", query,
		"--host", conf.Host,
		"--port", fmt.Sprint(conf.Port),
		"--user", conf.Username,
		"--password", conf.Password,
	}
	if !conf.Local {
		cmdArgs = append(cmdArgs, "--secure")
	}
	command := exec.Command("docker", cmdArgs...)
	out, err := command.Output()
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		t.Fatalf(string(exitError.Stderr))
	}
	require.NoError(t, err)
	return string(out)
}

func isPortReady(t *testing.T, port uint) (isOpen bool) {
	address := net.JoinHostPort("localhost", fmt.Sprintf("%d", port))
	conn, _ := net.DialTimeout("tcp", address, dialTimeout)
	if conn != nil {
		err := conn.Close()
		require.NoError(t, err)
		return true
	}
	return false
}

func waitPortIsReady(t *testing.T, port uint) {
	count := 0
	for count < maxDialRetries {
		count++
		if isPortReady(t, port) {
			return
		}
	}
	t.Fatalf("Port is not ready after %d retries", maxDialRetries)
}

func runSDKTestCommand(t *testing.T, inputFileName string, recreateDatabase bool) {
	if recreateDatabase {
		runQuery(t, "DROP DATABASE IF EXISTS tester")
		runQuery(t, "CREATE DATABASE IF NOT EXISTS tester")
	}
	projectRootDir := getProjectRootDir(t)
	command := exec.Command("make", "sdk-test")
	command.Dir = projectRootDir
	command.Env = os.Environ()
	command.Env = append(command.Env, fmt.Sprintf("TEST_ARGS=--input-file=%s", inputFileName))
	_, err := command.Output()
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		t.Error(string(exitError.Stderr))
	}
	require.NoError(t, err)
}
