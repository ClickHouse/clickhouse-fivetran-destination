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
	"github.com/stretchr/testify/assert"
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
	runSDKTestCommand(t, fileName)
	assertTableRowsWithFivetranId(t, tableName, [][]string{
		{"true", "42", "144", "100500", "100.5", "200.5", "42.42",
			"2024-05-07", "2024-04-05 15:33:14", "2024-02-03 12:44:22.123456789",
			"foo", "{\"a\": 1,\"b\": 2}", "<a>1</a>", "FFFA", "false", "abc-123-xyz"},
		{"false", "-42", "-144", "-100500", "-100.5", "-200.5", "-42.42",
			"2021-02-03", "2021-06-15 04:15:16", "2021-02-03 14:47:45.234567890",
			"bar", "{\"c\": 3,\"d\": 4}", "<b>42</b>", "FFFE", "false", "vbn-543-hjk"}})
	assertTableColumns(t, tableName, [][]string{
		{"b", "Nullable(Bool)", ""},
		{"i16", "Nullable(Int16)", ""},
		{"i32", "Nullable(Int32)", ""},
		{"i64", "Nullable(Int64)", ""},
		{"f32", "Nullable(Float32)", ""},
		{"f64", "Nullable(Float64)", ""},
		{"dec", "Nullable(Decimal(10, 4))", ""},
		{"d", "Nullable(Date)", ""},
		{"dt", "Nullable(DateTime)", ""},
		{"utc", "Nullable(DateTime64(9, 'UTC'))", ""},
		{"s", "Nullable(String)", ""},
		{"j", "Nullable(String)", "JSON"},
		{"x", "Nullable(String)", "XML"},
		{"bin", "Nullable(String)", "BINARY"},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""},
		{"_fivetran_deleted", "Bool", ""},
		{"_fivetran_id", "String", ""}})
}

func TestMutateAfterAlter(t *testing.T) {
	fileName := "input_mutate_after_alter.json"
	tableName := "mutate_after_alter"
	startServer(t)
	runSDKTestCommand(t, fileName)
	assertTableRowsWithPK(t, tableName, [][]string{
		{"1", "200", "asd", "zxc", "\\N", "\\N", "false"},
		{"2", "50", "\\N", "\\N", "<c>99</c>", "DD", "false"},
		{"3", "777.777", "<b>42</b>", "AA", "qaz", "qux", "true"},
		{"4", "20.5", "x", "\\N", "<d>77</d>", "\\N", "false"}})
	assertTableColumns(t, tableName, [][]string{
		{"id", "Int32", ""},
		{"amount", "Nullable(Float32)", ""},
		{"s1", "Nullable(String)", ""},
		{"s2", "Nullable(String)", ""},
		{"s3", "Nullable(String)", "XML"},
		{"s4", "Nullable(String)", "BINARY"},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""},
		{"_fivetran_deleted", "Bool", ""}})
}

func TestUpdateAndDelete(t *testing.T) {
	fileName := "input_update_and_delete.json"
	tableName := "update_and_delete"
	startServer(t)
	runSDKTestCommand(t, fileName)
	assertTableRowsWithPK(t, tableName, [][]string{
		{"1", "1111", "false"},
		{"2", "two", "false"},
		{"3", "three", "true"},
		{"4", "four-four", "true"},
		{"5", "it's 5", "true"}})
	assertTableColumns(t, tableName, [][]string{
		{"id", "Int32", ""},
		{"name", "Nullable(String)", ""},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""},
		{"_fivetran_deleted", "Bool", ""}})
}

func TestNonExistentRecordUpdatesAndDeletes(t *testing.T) {
	fileName := "input_non_existent_updates.json"
	tableName := "non_existent_updates"
	startServer(t)
	runSDKTestCommand(t, fileName)
	assertTableRowsWithPK(t, tableName, [][]string{
		{"1", "\\N", "false"}})
	assertTableColumns(t, tableName, [][]string{
		{"id", "Int32", ""},
		{"name", "Nullable(String)", ""},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""},
		{"_fivetran_deleted", "Bool", ""}})
}

func TestTruncate(t *testing.T) {
	fileName := "input_truncate.json"
	tableName := "table_to_truncate"
	startServer(t)
	runSDKTestCommand(t, fileName)
	assertTableRowsWithPK(t, tableName, nil)
	assertTableColumns(t, tableName, [][]string{
		{"id", "Int32", ""},
		{"name", "Nullable(String)", ""},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""},
		{"_fivetran_deleted", "Bool", ""}})
}

func TestTableNotFound(t *testing.T) {
	fileName := "input6_table_not_found.json"
	startServer(t)
	runSDKTestCommand(t, fileName) // verify at least no SDK tester errors
}

func TestLargeInputFile(t *testing.T) {
	fileName := "input_large_file"
	startServer(t)
	expectedCSV := generateAndWriteInputFile(t, fileName, 150_000)
	runSDKTestCommand(t, fmt.Sprintf("%s.json", fileName))
	assertTableRowsWithPK(t, fileName, expectedCSV)
	assertTableColumns(t, fileName, [][]string{
		{"id", "Int64", ""},
		{"data", "Nullable(String)", ""},
		{"created_at", "Nullable(DateTime)", ""},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""},
		{"_fivetran_deleted", "Bool", ""}})
}

func assertDatabaseRecords(t *testing.T, expectedRecords [][]string, dbRecordsCSVStr string) {
	dbRecords, err := csv.NewReader(strings.NewReader(dbRecordsCSVStr)).ReadAll()
	assert.NoError(t, err)
	assert.Equal(t, len(expectedRecords), len(dbRecords),
		"Expected %d, but got %d database records", len(expectedRecords), len(dbRecords))
	for i, expected := range expectedRecords {
		if !assert.Equal(t, expected, dbRecords[i]) {
			t.Fatal("Expected:", expected, "Actual:", dbRecords[i])
		}
	}
}

func assertTableRowsWithPK(t *testing.T, tableName string, expectedOutput [][]string) {
	query := fmt.Sprintf("SELECT * EXCEPT _fivetran_synced FROM tester.%s FINAL ORDER BY id FORMAT CSV", tableName)
	dbRecordsCSVStr := runQuery(t, query)
	assertDatabaseRecords(t, expectedOutput, dbRecordsCSVStr)
}

func assertTableRowsWithFivetranId(t *testing.T, tableName string, expectedOutput [][]string) {
	query := fmt.Sprintf("SELECT * EXCEPT _fivetran_synced FROM tester.%s FINAL ORDER BY _fivetran_id FORMAT CSV", tableName)
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
	assert.NoError(t, err)
	err = os.WriteFile(fmt.Sprintf("%s/sdk_tests/%s.json", cwd, tableName), content, 0644)
	assert.NoError(t, err)
	return assertRows
}

const dialTimeout = 10 * time.Millisecond
const maxDialRetries = 300

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
	assert.NoError(t, err)
	var result string
	// CLI vs IDE test run
	if strings.HasSuffix(cwd, "/destination") {
		result = cwd[:len(cwd)-12]
	}
	return result
}

func readConfig(t *testing.T) *config.Config {
	if connConfig.Load() != nil {
		return connConfig.Load().(*config.Config)
	}
	rootDir := getProjectRootDir(t)
	configBytes, err := os.ReadFile(fmt.Sprintf("%s/sdk_tests/configuration.json", rootDir))
	assert.NoError(t, err,
		"copy the default configuration first: cp sdk_tests/default_configuration.json sdk_tests/configuration.json")
	configMap := make(map[string]string)
	err = json.Unmarshal(configBytes, &configMap)
	assert.NoError(t, err)
	res, err := config.Parse(configMap)
	assert.NoError(t, err)
	connConfig.Store(res)
	return res
}

func runQuery(t *testing.T, query string) string {
	conf := readConfig(t)
	cmdArgs := []string{
		"exec", "fivetran-destination-clickhouse-server",
		"clickhouse-client", "--query", query,
		"--host", conf.Hostname,
		"--port", fmt.Sprint(conf.Port),
		"--database", conf.Database,
		"--user", conf.Username,
		"--password", conf.Password,
	}
	if conf.SSL.Enabled {
		cmdArgs = append(cmdArgs, "--secure")
	}
	command := exec.Command("docker", cmdArgs...)
	out, err := command.Output()
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		t.Error(string(exitError.Stderr))
	}
	assert.NoError(t, err)
	return string(out)
}

func isPortReady(t *testing.T, port uint) (isOpen bool) {
	address := net.JoinHostPort("localhost", fmt.Sprintf("%d", port))
	conn, _ := net.DialTimeout("tcp", address, dialTimeout)
	if conn != nil {
		err := conn.Close()
		assert.NoError(t, err)
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

func runSDKTestCommand(t *testing.T, inputFileName string) {
	runQuery(t, "DROP DATABASE IF EXISTS tester")
	runQuery(t, "CREATE DATABASE IF NOT EXISTS tester")
	projectRootDir := getProjectRootDir(t)
	command := exec.Command("make", "sdk-test")
	command.Dir = projectRootDir
	command.Env = os.Environ()
	command.Env = append(command.Env, fmt.Sprintf("TEST_ARGS=--input-file=%s", inputFileName))
	byteOut, err := command.Output()
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		t.Error(string(exitError.Stderr))
	}
	assert.NoError(t, err)
	out := string(byteOut)
	assert.Contains(t, out, "[Test connection and basic operations]: PASSED")
}
