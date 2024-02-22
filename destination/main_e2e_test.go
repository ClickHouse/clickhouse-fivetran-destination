package main

import (
	"context"
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
	"fivetran.com/fivetran_sdk/destination/db"
	"fivetran.com/fivetran_sdk/destination/db/config"
	"fivetran.com/fivetran_sdk/destination/service"
	pb "fivetran.com/fivetran_sdk/proto"
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
	runSDKTestCommand(t, fileName, true)
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
	runSDKTestCommand(t, fileName, true)
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
	runSDKTestCommand(t, fileName, true)
	assertTableRowsWithPK(t, tableName, [][]string{
		{"1", "\\N", "false"}})
	assertTableColumns(t, tableName, [][]string{
		{"id", "Int32", ""},
		{"name", "Nullable(String)", ""},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""},
		{"_fivetran_deleted", "Bool", ""}})
}

// Currently, SDK tester will always send the truncate operation last, no matter its position in the input file,
// and it is also a "soft" truncate only. See TestTruncateBefore for workaround to test "truncate before" + "hard".
// See TestTruncateExistingRecordsThenSync that verifies if we can do a full sync after truncating the table.
func TestTruncate(t *testing.T) {
	fileName := "input_truncate.json"
	tableName := "table_to_truncate"
	startServer(t)
	runSDKTestCommand(t, fileName, true)
	assertTableRowsWithPK(t, tableName, [][]string{
		{"1", "foo", "true"},
		{"2", "bar", "true"},
		{"3", "qaz", "true"}})
	assertTableColumns(t, tableName, [][]string{
		{"id", "Int32", ""},
		{"name", "Nullable(String)", ""},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""},
		{"_fivetran_deleted", "Bool", ""}})
}

func TestTruncateBefore(t *testing.T) {
	// FIXME:
	//  should be possible to test it "properly" with future SDK releases, currently it's a workaround,
	//  as truncate operation is always last and all the input.json rows are merged into one "replace" CSV file.
	//  With a fixed version, direct conn calls should be replaced with SDK tester calls instead.
	fileName := "input_truncate_create_table.json"
	tableName := "table_to_truncate"

	startServer(t)
	conf := readConfigMap(t)
	conn, err := db.GetClickHouseConnection(conf)
	require.NoError(t, err)
	defer conn.Close()

	// create a table via SDK first, then store a record with _fivetran_synced in the future;
	// verify that this record is not "soft deleted" after truncate
	upsertRecordsWithSDK := func() {
		runSDKTestCommand(t, fileName, true)
		assertTableColumns(t, tableName, [][]string{
			{"id", "Int32", ""},
			{"name", "Nullable(String)", ""},
			{"_fivetran_synced", "DateTime64(9, 'UTC')", ""},
			{"_fivetran_deleted", "Bool", ""}})

		syncTime := time.Now().Add(1 * time.Minute).UTC()
		runQuery(t, fmt.Sprintf("INSERT INTO tester.%s VALUES (1, 'foo', %d, false)",
			tableName, syncTime.UnixNano()))
	}

	/// soft (ALTER TABLE UPDATE)
	upsertRecordsWithSDK()
	err = conn.TruncateTable(context.Background(), schemaName, tableName, syncedColumn, time.Now(), &softDeletedColumn)
	require.NoError(t, err)
	assertTableRowsWithPK(t, tableName, [][]string{
		{"1", "foo", "false"},
		{"2", "bar", "true"},
		{"3", "qaz", "true"}})

	/// hard (ALTER TABLE DELETE)
	upsertRecordsWithSDK()
	err = conn.TruncateTable(context.Background(), schemaName, tableName, syncedColumn, time.Now(), nil)
	require.NoError(t, err)
	assertTableRowsWithPK(t, tableName, [][]string{
		{"1", "foo", "false"},
	})
}

// FIXME: should be possible to fully simulate using just SDK tester.
func TestTruncateExistingRecordsThenSync(t *testing.T) {
	fileName := "input_truncate_then_sync.json"
	tableName := "truncate_then_sync"

	startServer(t)
	conf := readConfigMap(t)
	conn, err := db.GetClickHouseConnection(conf)
	require.NoError(t, err)
	defer conn.Close()

	runSDKTestCommand(t, fileName, true)
	assertTableRowsWithPK(t, tableName, [][]string{
		{"1", "name-truncated-1", "desc-truncated-1", "true"},
		{"2", "name-truncated-2", "desc-truncated-2", "true"}})
	assertTableColumns(t, tableName, [][]string{
		{"id", "Int32", ""},
		{"name", "Nullable(String)", ""},
		{"desc", "Nullable(String)", ""},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""},
		{"_fivetran_deleted", "Bool", ""}})

	ctx := context.Background()
	table := &pb.Table{
		Name: tableName,
		Columns: []*pb.Column{
			{Name: "id", Type: pb.DataType_INT, PrimaryKey: true},
			{Name: "name", Type: pb.DataType_STRING, PrimaryKey: false},
			{Name: "desc", Type: pb.DataType_STRING, PrimaryKey: false},
			{Name: "_fivetran_synced", Type: pb.DataType_UTC_DATETIME, PrimaryKey: false},
			{Name: "_fivetran_deleted", Type: pb.DataType_BOOLEAN, PrimaryKey: false},
		}}

	metadata, err := service.GetPrimaryKeysAndMetadataColumns(table)
	require.NoError(t, err)

	colTypes, err := conn.GetColumnTypes(ctx, schemaName, tableName)
	require.NoError(t, err)

	nullStr := "this-is-null"
	unmodifiedStr := "do-not-modify"
	syncTime := time.Now().Format("2006-01-02T15:04:05.000000000Z")
	replaceCSV := [][]string{
		{"1", "name-replaced-1", "desc-replaced-1", syncTime, "false"},
		{"2", "name-replaced-2", "desc-replaced-2", syncTime, "false"},
	}
	updateCSV := [][]string{
		{"1", unmodifiedStr, "desc-updated-1", syncTime, "false"},
	}
	deleteCSV := [][]string{
		{"2", nullStr, nullStr, syncTime, "true"},
	}

	err = conn.ReplaceBatch(ctx, schemaName, table, replaceCSV, nullStr, 100)
	require.NoError(t, err)
	err = conn.UpdateBatch(ctx, schemaName, table, metadata.PrimaryKeys, colTypes, updateCSV, nullStr, unmodifiedStr, 100, 100, 5)
	require.NoError(t, err)
	err = conn.SoftDeleteBatch(ctx, schemaName, table, metadata.PrimaryKeys, colTypes, deleteCSV, metadata.FivetranSyncedIdx, metadata.FivetranDeletedIdx, 100, 100, 5)
	require.NoError(t, err)

	// FIXME: this is actually wrong.
	//  While the final values are correct, we are missing previous "soft truncated" records.
	//  Those were replaced with their newer versions via ReplacingMergeTree.
	assertTableRowsWithPK(t, tableName, [][]string{
		{"1", "name-replaced-1", "desc-updated-1", "false"},
		{"2", "name-replaced-2", "desc-replaced-2", "true"}})
}

func TestTableNotFound(t *testing.T) {
	fileName := "input6_table_not_found.json"
	startServer(t)
	runSDKTestCommand(t, fileName, true) // verify at least no SDK tester errors
}

func TestLargeInputFile(t *testing.T) {
	tableName := "input_large_file"
	startServer(t)

	expectedCSV := generateAndWriteInputFile(t, tableName, 150_000)
	runSDKTestCommand(t, fmt.Sprintf("%s.json", tableName), true)

	dbRecordsCSVStr := runQuery(t, fmt.Sprintf("SELECT * EXCEPT _fivetran_synced FROM tester.%s FINAL ORDER BY id FORMAT CSV", tableName))
	assertDatabaseRecordsFailFast(t, expectedCSV, dbRecordsCSVStr)
	assertTableColumns(t, tableName, [][]string{
		{"id", "Int64", ""},
		{"data", "Nullable(String)", ""},
		{"created_at", "Nullable(DateTime)", ""},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""},
		{"_fivetran_deleted", "Bool", ""}})
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
	split := strings.Split(conf.Host, ":")
	require.Len(t, split, 2)
	cmdArgs := []string{
		"exec", "fivetran-destination-clickhouse-server",
		"clickhouse-client", "--query", query,
		"--host", split[0],
		"--port", split[1],
		"--database", conf.Database,
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
	byteOut, err := command.Output()
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		t.Error(string(exitError.Stderr))
	}
	require.NoError(t, err)
	out := string(byteOut)
	require.Contains(t, out, "[Test connection and basic operations]: PASSED")
}

var schemaName = "tester"
var softDeletedColumn = "_fivetran_deleted"
var syncedColumn = "_fivetran_synced"
