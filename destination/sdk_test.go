package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// See https://github.com/fivetran/fivetran_sdk/tree/main/tools/destination-tester for the SDK tester docs
// See Makefile:test for the SDK tester run command
// See sdk_tests/*.json for the input files

func TestSDKInputs(t *testing.T) {
	go main()
	time.Sleep(1 * time.Second)

	runSDKTestCommand(t, "input1_upsert_after_alter.json")
	assertTableRowsWithPK(t, "input1",
		"1,200,\\N,false\n"+
			"2,33.345,\"three-three\",false\n"+
			"3,777.777,\"seven-seven-seven\",true\n"+
			"4,50,\"fifty\",false\n")
	assertTableColumns(t, "input1",
		"\"id\",\"Int32\"\n"+
			"\"amount\",\"Nullable(Float32)\"\n"+
			"\"desc\",\"Nullable(String)\"\n"+
			"\"_fivetran_synced\",\"DateTime64(9, 'UTC')\"\n"+
			"\"_fivetran_deleted\",\"Bool\"\n")

	runSDKTestCommand(t, "input2_update_and_delete.json")
	assertTableRowsWithPK(t, "input2",
		"1,\"1111\",false\n"+
			"2,\"two\",false\n"+
			"3,\"three\",true\n"+
			"4,\"four-four\",true\n"+
			"5,\"it's 5\",true\n")
	assertTableColumns(t, "input2",
		"\"id\",\"Int32\"\n"+
			"\"name\",\"Nullable(String)\"\n"+
			"\"_fivetran_synced\",\"DateTime64(9, 'UTC')\"\n"+
			"\"_fivetran_deleted\",\"Bool\"\n")

	runSDKTestCommand(t, "input3_all_data_types.json")
	assertTableRowsWithFivetranId(t, "input3",
		"true,42,144,100500,100.5,200.5,42.42,\"2024-05-07\",\"2024-04-05 15:33:14\",\"2024-02-03 12:44:22.123456789\",\"foo\",1,2,0,0,\"<a>1</a>\",false,\"abc-123-xyz\"\n"+
			"false,-42,-144,-100500,-100.5,-200.5,-42.42,\"2021-02-03\",\"2021-06-15 04:15:16\",\"2021-02-03 14:47:45.234567890\",\"bar\",0,0,3,4,\"<b>42</b>\",false,\"vbn-543-hjk\"\n")
	assertTableColumns(t, "input3",
		"\"b\",\"Nullable(Bool)\"\n"+
			"\"i16\",\"Nullable(Int16)\"\n"+
			"\"i32\",\"Nullable(Int32)\"\n"+
			"\"i64\",\"Nullable(Int64)\"\n"+
			"\"f32\",\"Nullable(Float32)\"\n"+
			"\"f64\",\"Nullable(Float64)\"\n"+
			"\"dec\",\"Nullable(Decimal(10, 4))\"\n"+
			"\"d\",\"Nullable(Date)\"\n"+
			"\"dt\",\"Nullable(DateTime)\"\n"+
			"\"utc\",\"Nullable(DateTime64(9, 'UTC'))\"\n"+
			"\"s\",\"Nullable(String)\"\n"+
			"\"j\",\"Object('json')\"\n"+
			"\"x\",\"Nullable(String)\"\n"+
			"\"_fivetran_synced\",\"DateTime64(9, 'UTC')\"\n"+
			"\"_fivetran_deleted\",\"Bool\"\n"+
			"\"_fivetran_id\",\"String\"\n")

	largeTableName := "input4_large_file"
	expectedCSV := generateAndWriteInputFile(t, largeTableName, 150_000)
	runSDKTestCommand(t, fmt.Sprintf("%s.json", largeTableName))
	assertTableRowsWithPK(t, largeTableName, expectedCSV)
	assertTableColumns(t, largeTableName,
		"\"id\",\"Int64\"\n"+
			"\"data\",\"Nullable(String)\"\n"+
			"\"created_at\",\"Nullable(DateTime)\"\n"+
			"\"_fivetran_synced\",\"DateTime64(9, 'UTC')\"\n"+
			"\"_fivetran_deleted\",\"Bool\"\n")
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

func runQuery(t *testing.T, query string, expectedOutput string) {
	projectRootDir := getProjectRootDir(t)
	cmd := exec.Command("docker", "exec", "fivetran-destination-clickhouse-server",
		"clickhouse-client", "--query", query)
	cmd.Dir = projectRootDir
	byteOut, err := cmd.Output()
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		t.Error(string(exitError.Stderr))
	}
	out := string(byteOut)
	assert.NoError(t, err)
	assert.Equal(t, expectedOutput, out)
}

func assertTableRowsWithPK(t *testing.T, tableName string, expectedOutput string) {
	query := fmt.Sprintf("SELECT * EXCEPT _fivetran_synced FROM tester.%s FINAL ORDER BY id FORMAT CSV", tableName)
	runQuery(t, query, expectedOutput)
}

func assertTableRowsWithFivetranId(t *testing.T, tableName string, expectedOutput string) {
	query := fmt.Sprintf("SELECT * EXCEPT _fivetran_synced FROM tester.%s FINAL ORDER BY _fivetran_id FORMAT CSV", tableName)
	runQuery(t, query, expectedOutput)
}

func assertTableColumns(t *testing.T, tableName string, expectedOutput string) {
	query := fmt.Sprintf("SELECT name, type FROM system.columns WHERE database = 'tester' AND table = '%s' FORMAT CSV", tableName)
	runQuery(t, query, expectedOutput)
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

func generateAndWriteInputFile(t *testing.T, tableName string, n uint) string {
	rows := make([]Row, n)
	updateRows := make([]Row, n/20)
	deleteRows := make([]Row, n/50)
	assertRows := make([]Row, n)
	createdAt := time.Date(2021, 2, 15, 14, 13, 12, 0, time.UTC)
	for i := uint(0); i < n; i++ {
		rowCreatedAt := createdAt.Add(time.Duration(i) * time.Second)
		row := Row{
			Id:        i + 1,
			Data:      fmt.Sprintf("%d:%d:%d", i, i+1, i+2),
			CreatedAt: rowCreatedAt.Format("2006-01-02T15:04:05"),
		}
		rows[i] = row
		assertRows[i] = row
		assertRows[i].CreatedAt = rowCreatedAt.Format("2006-01-02 15:04:05") // ClickHouse "simple" format (no T)
		if i%20 == 0 {
			updatedData := fmt.Sprintf("%d:%d:%d", i+1, i+2, i+3)
			updateRows[i/20] = Row{
				Id:   i + 1,
				Data: updatedData,
			}
			assertRows[i].Data = updatedData
		}
		if i%50 == 0 {
			deleteRows[i/50] = Row{
				Id: i + 1,
			}
			assertRows[i].FivetranDeleted = true
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

	var csv strings.Builder
	for _, row := range assertRows {
		csv.WriteString(fmt.Sprintf("%d,\"%s\",\"%s\",%t\n", row.Id, row.Data, row.CreatedAt, row.FivetranDeleted))
	}
	return csv.String()
}
