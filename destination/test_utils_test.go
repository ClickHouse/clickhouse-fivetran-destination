package main

import (
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

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

const dialTimeout = 10 * time.Millisecond
const maxDialRetries = 300

var sdkConfig atomic.Value

func StartClickHouse(t *testing.T) {
	if isClickHouseReady(t) {
		return
	}
	cmd := exec.Command("docker-compose", "up", "-d", "clickhouse")
	cmd.Dir = GetProjectRootDir(t)
	err := cmd.Run()
	assert.NoError(t, err)
	waitClickHouseIsReady(t)
}

func StartServer(t *testing.T) {
	if isPortReady(t, *port) {
		return
	}
	StartClickHouse(t)
	go main()
	RunQuery(t, "DROP DATABASE IF EXISTS tester")
	RunQuery(t, "CREATE DATABASE IF NOT EXISTS tester")
	waitPortIsReady(t, *port)
}

func GetProjectRootDir(t *testing.T) string {
	cwd, err := os.Getwd()
	assert.NoError(t, err)
	var result string
	if strings.HasSuffix(cwd, "/destination") {
		result = cwd[:len(cwd)-12]
	}
	return result
}

func ReadConfiguration(t *testing.T) *SDKConfig {
	if sdkConfig.Load() != nil {
		return sdkConfig.Load().(*SDKConfig)
	}
	rootDir := GetProjectRootDir(t)
	configBytes, err := os.ReadFile(fmt.Sprintf("%s/sdk_tests/configuration.json", rootDir))
	assert.NoError(t, err,
		"copy the default configuration first: cp sdk_tests/default_configuration.json sdk_tests/configuration.json")
	configMap := make(map[string]string)
	err = json.Unmarshal(configBytes, &configMap)
	assert.NoError(t, err)
	res, err := ParseSDKConfig(configMap)
	assert.NoError(t, err)
	sdkConfig.Store(res)
	return res
}

func RunQuery(t *testing.T, query string) string {
	conf := ReadConfiguration(t)
	cmdArgs := []string{
		"exec", "fivetran-destination-clickhouse-server",
		"clickhouse-client", "--query", query,
		"--host", conf.Hostname,
		"--port", fmt.Sprint(conf.Port),
		"--database", conf.Database,
		"--user", conf.Username,
		"--password", conf.Password,
	}
	if conf.SSL.enabled {
		cmdArgs = append(cmdArgs, "--secure")
	}
	cmd := exec.Command("docker", cmdArgs...)
	out, err := cmd.Output()
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		t.Error(string(exitError.Stderr))
	}
	assert.NoError(t, err)
	return string(out)
}

func Guid() string {
	return strings.ReplaceAll(uuid.New().String(), "-", "_")
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

func isClickHouseReady(t *testing.T) (isReady bool) {
	if isPortReady(t, 8123) {
		cmd := exec.Command("curl", "http://localhost:8123", "--data-binary", "SELECT 1")
		_, err := cmd.Output()
		return err == nil
	}
	return false
}

func waitClickHouseIsReady(t *testing.T) {
	count := 0
	for count < maxDialRetries {
		count++
		if isClickHouseReady(t) {
			return
		}
	}
	t.Fatalf("ClickHouse is not ready after %d retries", maxDialRetries)
}
