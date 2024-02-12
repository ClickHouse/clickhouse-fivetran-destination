package main

import (
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

	runSDKTestCommand(t, "input_1.json")
	func() {
		query := "SELECT * EXCEPT _fivetran_synced FROM tester.input1 FINAL ORDER BY id FORMAT CSV"
		expectedOutput := "1,200,\\N,false\n" +
			"2,33.345,\"three-three\",false\n" +
			"3,777.777,\"seven-seven-seven\",true\n" +
			"4,50,\"fifty\",false\n"
		runQuery(t, query, expectedOutput)
	}()
	func() {
		query := "SELECT name, type FROM system.columns WHERE database = 'tester' AND table = 'input1' FORMAT CSV"
		expectedOutput := "\"id\",\"Int32\"\n" +
			"\"amount\",\"Nullable(Float32)\"\n" +
			"\"desc\",\"Nullable(String)\"\n" +
			"\"_fivetran_synced\",\"DateTime64(9, 'UTC')\"\n" +
			"\"_fivetran_deleted\",\"Bool\"\n"
		runQuery(t, query, expectedOutput)
	}()

	runSDKTestCommand(t, "input_2.json")
	func() {
		query := "SELECT * EXCEPT _fivetran_synced FROM tester.input2 FINAL ORDER BY id FORMAT CSV"
		expectedOutput := "1,\"1111\",false\n" +
			"2,\"two\",false\n" +
			"3,\"three\",true\n" +
			"4,\"four-four\",true\n" +
			"5,\"it's 5\",true\n"
		runQuery(t, query, expectedOutput)
	}()
	func() {
		query := "SELECT name, type FROM system.columns WHERE database = 'tester' AND table = 'input2' FORMAT CSV"
		expectedOutput := "\"id\",\"Int32\"\n" +
			"\"name\",\"Nullable(String)\"\n" +
			"\"_fivetran_synced\",\"DateTime64(9, 'UTC')\"\n" +
			"\"_fivetran_deleted\",\"Bool\"\n"
		runQuery(t, query, expectedOutput)
	}()
}

func runSDKTestCommand(t *testing.T, inputFileName string) {
	projectRootDir := getProjectRootDir(t)
	cmd := exec.Command("make", "test")
	cmd.Dir = projectRootDir
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, fmt.Sprintf("TEST_ARGS=--input-file=%s", inputFileName))
	byteOut, err := cmd.Output()
	out := string(byteOut)
	assert.NoError(t, err)
	assert.Contains(t, out, "[Test connection and basic operations]: PASSED")
	assert.Contains(t, out, "[Test mutation operations]: PASSED")
}

func runQuery(t *testing.T, query string, expectedOutput string) {
	projectRootDir := getProjectRootDir(t)
	cmd := exec.Command("docker", "exec", "fivetran-destination-clickhouse-server",
		"clickhouse-client", "--query", query)
	cmd.Dir = projectRootDir
	byteOut, err := cmd.Output()
	out := string(byteOut)
	assert.NoError(t, err)
	assert.Equal(t, expectedOutput, out)
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
