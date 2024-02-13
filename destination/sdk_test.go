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

	"github.com/stretchr/testify/assert"
)

// See also:
// - https://github.com/fivetran/fivetran_sdk/tree/main/tools/destination-tester for the SDK tester docs
// - Makefile:test for the SDK tester run command
// - sdk_tests/*.json for the input files

var isStarted atomic.Bool

func TestUpsertAfterAlter(t *testing.T) {
	startServer(t)
	runSDKTestCommand(t, "input1_upsert_after_alter.json")
	assertTableRowsWithPK(t, "input1", [][]string{
		{"1", "200", "\\N", "false"},
		{"2", "33.345", "three-three", "false"},
		{"3", "777.777", "seven-seven-seven", "true"},
		{"4", "50", "fifty", "false"}})
	assertTableColumns(t, "input1", [][]string{
		{"id", "Int32", ""},
		{"amount", "Nullable(Float32)", ""},
		{"desc", "Nullable(String)", ""},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""},
		{"_fivetran_deleted", "Bool", ""}})
}

func TestUpdateAndDelete(t *testing.T) {
	startServer(t)
	runSDKTestCommand(t, "input2_update_and_delete.json")
	assertTableRowsWithPK(t, "input2", [][]string{
		{"1", "1111", "false"},
		{"2", "two", "false"},
		{"3", "three", "true"},
		{"4", "four-four", "true"},
		{"5", "it's 5", "true"}})
	assertTableColumns(t, "input2", [][]string{
		{"id", "Int32", ""},
		{"name", "Nullable(String)", ""},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""},
		{"_fivetran_deleted", "Bool", ""}})
}

func TestAllDataTypes(t *testing.T) {
	startServer(t)
	runSDKTestCommand(t, "input3_all_data_types.json")
	assertTableRowsWithFivetranId(t, "input3", [][]string{
		{"true", "42", "144", "100500", "100.5", "200.5", "42.42",
			"2024-05-07", "2024-04-05 15:33:14", "2024-02-03 12:44:22.123456789",
			"foo", "1", "2", "0", "0", "<a>1</a>", "YmFzZTY0", "false", "abc-123-xyz"},
		{"false", "-42", "-144", "-100500", "-100.5", "-200.5", "-42.42",
			"2021-02-03", "2021-06-15 04:15:16", "2021-02-03 14:47:45.234567890",
			"bar", "0", "0", "3", "4", "<b>42</b>", "YmFzZTY0", "false", "vbn-543-hjk"}})
	assertTableColumns(t, "input3", [][]string{
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
		{"j", "Object('json')", ""},
		{"x", "Nullable(String)", "XML"},
		{"bin", "Nullable(String)", "BINARY"},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""},
		{"_fivetran_deleted", "Bool", ""},
		{"_fivetran_id", "String", ""}})
}

func TestNonExistentRecordUpdatesAndDeletes(t *testing.T) {
	startServer(t)
	runSDKTestCommand(t, "input4_non_existent_updates.json")
	assertTableRowsWithPK(t, "input4", [][]string{
		{"1", "\\N", "false"}})
	assertTableColumns(t, "input4", [][]string{
		{"id", "Int32", ""},
		{"name", "Nullable(String)", ""},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""},
		{"_fivetran_deleted", "Bool", ""}})
}

func TestTruncate(t *testing.T) {
	startServer(t)
	runSDKTestCommand(t, "input5_truncate.json")
	assertTableRowsWithPK(t, "input5", nil)
	assertTableColumns(t, "input5", [][]string{
		{"id", "Int32", ""},
		{"name", "Nullable(String)", ""},
		{"_fivetran_synced", "DateTime64(9, 'UTC')", ""},
		{"_fivetran_deleted", "Bool", ""}})
}

func TestTableNotFound(t *testing.T) {
	startServer(t)
	runSDKTestCommand(t, "input6_table_not_found.json") // verify at least no SDK tester errors
}

func TestLargeInputFile(t *testing.T) {
	startServer(t)
	fileName := "input_large_file"
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

func runSDKTestCommand(t *testing.T, inputFileName string) {
	projectRootDir := getProjectRootDir(t)
	cmd := exec.Command("make", "test")
	cmd.Dir = projectRootDir
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, fmt.Sprintf("TEST_ARGS=--input-file=%s", inputFileName))
	byteOut, err := cmd.Output()
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		t.Error(string(exitError.Stderr))
	}
	assert.NoError(t, err)
	out := string(byteOut)
	assert.Contains(t, out, "[Test connection and basic operations]: PASSED")
	assert.Contains(t, out, "[Test mutation operations]: PASSED")
}

func runQuery(t *testing.T, query string) string {
	projectRootDir := getProjectRootDir(t)
	cmd := exec.Command("docker", "exec", "fivetran-destination-clickhouse-server",
		"clickhouse-client", "--query", query)
	cmd.Dir = projectRootDir
	byteOut, err := cmd.Output()
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		t.Error(string(exitError.Stderr))
	}
	assert.NoError(t, err)
	return string(byteOut)
}

func assertTableRowsWithPK(t *testing.T, tableName string, expectedOutput [][]string) {
	query := fmt.Sprintf("SELECT * EXCEPT _fivetran_synced FROM tester.%s FINAL ORDER BY id FORMAT CSV", tableName)
	out := runQuery(t, query)
	records, err := csv.NewReader(strings.NewReader(out)).ReadAll()
	assert.NoError(t, err)
	assert.Equal(t, expectedOutput, records)
}

func assertTableRowsWithFivetranId(t *testing.T, tableName string, expectedOutput [][]string) {
	query := fmt.Sprintf("SELECT * EXCEPT _fivetran_synced FROM tester.%s FINAL ORDER BY _fivetran_id FORMAT CSV", tableName)
	out := runQuery(t, query)
	records, err := csv.NewReader(strings.NewReader(out)).ReadAll()
	assert.NoError(t, err)
	assert.Equal(t, expectedOutput, records)
}

func assertTableColumns(t *testing.T, tableName string, expectedOutput [][]string) {
	query := fmt.Sprintf("SELECT name, type, comment FROM system.columns WHERE database = 'tester' AND table = '%s' FORMAT CSV", tableName)
	out := runQuery(t, query)
	records, err := csv.NewReader(strings.NewReader(out)).ReadAll()
	assert.NoError(t, err)
	assert.Equal(t, expectedOutput, records)
}

func getProjectRootDir(t *testing.T) string {
	cwd, err := os.Getwd()
	assert.NoError(t, err)
	var result string
	if strings.HasSuffix(cwd, "/destination") {
		result = cwd[:len(cwd)-12]
	}
	return result
}

func startServer(t *testing.T) {
	if !isStarted.Load() {
		go main()
		timeout := 10 * time.Millisecond
		count := 0
		for count < 300 {
			count++
			address := net.JoinHostPort("localhost", fmt.Sprintf("%d", *port))
			conn, _ := net.DialTimeout("tcp", address, timeout)
			if conn != nil {
				err := conn.Close()
				assert.NoError(t, err)
				isStarted.Store(true)
				break
			}
		}
	}
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
	Id              uint   `json:"id"`
	Data            string `json:"data,omitempty"`
	CreatedAt       string `json:"created_at,omitempty"`
	FivetranDeleted bool   `json:"_fivetran_deleted,omitempty"`
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
		row := Row{
			Id:        i + 1,
			Data:      fmt.Sprintf("%d:%d:%d", i, i+1, i+2),
			CreatedAt: rowCreatedAt.Format("2006-01-02T15:04:05"),
		}
		rows[i] = row
		// id, data, created_at, _fivetran_deleted
		assertRows[i] = make([]string, 4)
		assertRows[i][0] = fmt.Sprintf("%d", row.Id)
		assertRows[i][1] = row.Data
		assertRows[i][2] = rowCreatedAt.Format("2006-01-02 15:04:05") // ClickHouse "simple" format (no T)
		assertRows[i][3] = "false"
		if i%20 == 0 {
			updatedData := fmt.Sprintf("%d:%d:%d", i+1, i+2, i+3)
			updateRows[i/20] = Row{
				Id:   i + 1,
				Data: updatedData,
			}
			assertRows[i][1] = updatedData
		}
		if i%50 == 0 {
			deleteRows[i/50] = Row{
				Id: i + 1,
			}
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
