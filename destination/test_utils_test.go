package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

const dialTimeout = 10 * time.Millisecond
const maxDialRetries = 300
const clickHousePortHTTP = 8123

func StartClickHouse(t *testing.T) {
	if isClickHouseReady(t) {
		return
	}
	cmd := exec.Command("docker-compose", "up", "-d", "clickhouse")
	cmd.Dir = GetProjectRootDir(nil)
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

func RunQuery(t *testing.T, query string) string {
	cmd := exec.Command("docker", "exec", "fivetran-destination-clickhouse-server",
		"clickhouse-client", "--query", query)
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
	t.Fatal(fmt.Sprintf("Port is not ready after %d retries", maxDialRetries))
}

func isClickHouseReady(t *testing.T) (isReady bool) {
	if isPortReady(t, clickHousePortHTTP) {
		cmd := exec.Command("curl", fmt.Sprintf("http://localhost:%d", clickHousePortHTTP),
			"--data-binary", "SELECT 1")
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
	t.Fatal(fmt.Sprintf("ClickHouse is not ready after %d retries", maxDialRetries))
}
