package config

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

const (
	HostKey     = "host"
	PortKey     = "port"
	UsernameKey = "username"
	PasswordKey = "password"

	AdvancedConfigKey = "advanced_config"
)

type Config struct {
	Host     string
	Port     uint
	Username string
	Password string
	Local    bool
}

// AdvancedConfig is the top-level structure of the optional JSON configuration file
// uploaded via the Fivetran setup form.
type AdvancedConfig struct {
	DestinationSettings     *DestinationSettings     `json:"destination_settings,omitempty"`
	ClickHouseQuerySettings map[string]any            `json:"clickhouse_query_settings,omitempty"`
	Tables                  map[string]*TableSettings `json:"tables,omitempty"`
}

// DestinationSettings controls the internal behavior of the destination connector.
type DestinationSettings struct {
	WriteBatchSize      *uint `json:"write_batch_size,omitempty"`
	SelectBatchSize     *uint `json:"select_batch_size,omitempty"`
	HardDeleteBatchSize *uint `json:"hard_delete_batch_size,omitempty"`
	MaxParallelSelects  *uint `json:"max_parallel_selects,omitempty"`
	MaxIdleConnections  *uint `json:"max_idle_connections,omitempty"`
	MaxOpenConnections  *uint `json:"max_open_connections,omitempty"`
	RequestTimeoutSecs  *uint `json:"request_timeout_seconds,omitempty"`
}

// TableSettings holds per-table CREATE TABLE configuration.
type TableSettings struct {
	OrderBy  []string       `json:"order_by,omitempty"`
	Settings map[string]any `json:"settings,omitempty"`
}

// SettingsClause serializes the Settings map into a SQL-compatible SETTINGS clause value.
// e.g. {"index_granularity": 2048, "storage_policy": "hot_cold"} becomes
// "index_granularity=2048, storage_policy='hot_cold'"
func (ts *TableSettings) SettingsClause() string {
	if ts == nil || len(ts.Settings) == 0 {
		return ""
	}
	parts := make([]string, 0, len(ts.Settings))
	for k, v := range ts.Settings {
		switch val := v.(type) {
		case string:
			parts = append(parts, fmt.Sprintf("%s='%s'", k, val))
		default:
			parts = append(parts, fmt.Sprintf("%s=%v", k, val))
		}
	}
	return strings.Join(parts, ", ")
}

// Parse ClickHouse connection config from a Fivetran config map that we receive on every GRPC call.
func Parse(configuration map[string]string) (*Config, error) {
	host, err := validateHost(getWithDefault(configuration, HostKey, "localhost", true))
	if err != nil {
		return nil, err
	}
	port, err := validatePort(getWithDefault(configuration, PortKey, "9440", true))
	if err != nil {
		return nil, err
	}
	return &Config{
		Host:     host,
		Port:     port,
		Username: getWithDefault(configuration, UsernameKey, "default", true),
		Password: getWithDefault(configuration, PasswordKey, "", false),
		Local:    getWithDefault(configuration, "local", "false", true) == "true",
	}, nil
}

// ParseAdvancedConfig decodes and parses the optional JSON configuration file
// from the Fivetran config map. The file is base64-encoded by Fivetran's UploadField.
// Returns a zero-value AdvancedConfig if the key is absent or empty.
func ParseAdvancedConfig(configuration map[string]string) (*AdvancedConfig, error) {
	raw := getWithDefault(configuration, AdvancedConfigKey, "", false)
	if raw == "" {
		return &AdvancedConfig{}, nil
	}

	jsonBytes, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("failed to decode advanced config (expected base64): %w", err)
	}

	var cfg AdvancedConfig
	if err := json.Unmarshal(jsonBytes, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse advanced config JSON: %w", err)
	}

	return &cfg, nil
}

// ResolveTableSettings returns the TableSettings for a specific table,
// falling back to defaults if no per-table configuration exists.
func (c *AdvancedConfig) ResolveTableSettings(schemaName, tableName string) *TableSettings {
	if c != nil && c.Tables != nil {
		key := schemaName + "." + tableName
		if ts, ok := c.Tables[key]; ok {
			return ts
		}
	}
	return &TableSettings{}
}

func getWithDefault(configuration map[string]string, key string, defaultValue string, trim bool) string {
	value, ok := configuration[key]
	if !ok || value == "" {
		return defaultValue
	}
	if trim {
		return strings.Trim(value, " ")
	}
	return value
}

func validateHost(host string) (string, error) {
	if strings.Contains(host, ":") {
		return "", fmt.Errorf("host %s should not contain protocol or port", host)
	}
	if strings.Contains(host, "/") {
		return "", fmt.Errorf("host %s should not contain path", host)
	}
	return host, nil
}

func validatePort(port string) (uint, error) {
	portInt, err := strconv.Atoi(port)
	if err != nil {
		return 0, fmt.Errorf("port %s must be a number in range [1, 65535]", port)
	}
	if portInt < 1 || portInt > 65535 {
		return 0, fmt.Errorf("port %s must be in range [1, 65535]", port)
	}
	return uint(portInt), nil
}
