package config

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"fivetran.com/fivetran_sdk/destination/common/flags"
	"fivetran.com/fivetran_sdk/destination/common/log"
)

const (
	HostKey     = "host"
	PortKey     = "port"
	UsernameKey = "username"
	PasswordKey = "password"
)

type Config struct {
	Host     string
	Port     uint
	Username string
	Password string
	Local    bool
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

// ParseAll parses both the connection config and the optional advanced config
// from the Fivetran configuration map. If destination configurations are present,
// they are applied to the global flags (clamped into range with a warning rather
// than rejected).
func ParseAll(configuration map[string]string) (*Config, error) {
	advancedCfg, err := ParseAdvancedConfig(configuration)
	if err != nil {
		return nil, fmt.Errorf("failed to parse advanced config: %w", err)
	}
	ValidateAndOverwriteFlags(advancedCfg.DestinationConfigurations)
	jsonBytes, _ := json.Marshal(advancedCfg)
	log.Info(fmt.Sprintf("Destination configurations applied: %s", string(jsonBytes)))
	return Parse(configuration)
}

// --- Advanced Configuration ---

const AdvancedConfigKey = "advanced_config"

// AdvancedConfig is the top-level structure of the optional JSON configuration file
// uploaded via the Fivetran setup form.
type AdvancedConfig struct {
	DestinationConfigurations *DestinationConfigurations `json:"destination_configurations,omitempty"`
}

// DestinationConfigurations controls the internal behavior of the destination connector.
type DestinationConfigurations struct {
	WriteBatchSize      *uint `json:"write_batch_size,omitempty"`
	SelectBatchSize     *uint `json:"select_batch_size,omitempty"`
	MutationBatchSize   *uint `json:"mutation_batch_size,omitempty"`
	HardDeleteBatchSize *uint `json:"hard_delete_batch_size,omitempty"`
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

	warnOnUnknownFields(jsonBytes)

	return &cfg, nil
}

func warnOnUnknownFields(jsonBytes []byte) {
	decoder := json.NewDecoder(bytes.NewReader(jsonBytes))
	decoder.DisallowUnknownFields()
	var strict AdvancedConfig
	if err := decoder.Decode(&strict); err != nil {
		log.Warn(fmt.Sprintf("Advanced config contains unknown fields (they will be ignored): %v", err))
	}
}

// ValidateAndOverwriteFlags overrides the global flag values with values from the
// parsed DestinationConfigurations. Nil fields are left at their flag defaults.
// Values outside the allowed range for each setting are clamped to the nearest
// bound (with a warning logged) rather than rejected, so a stale or aggressive
// advanced_config never breaks startup.
func ValidateAndOverwriteFlags(ds *DestinationConfigurations) {
	if ds == nil {
		return
	}
	applySetting(&flags.WriteBatchSizeSetting, ds.WriteBatchSize)
	applySetting(&flags.SelectBatchSizeSetting, ds.SelectBatchSize)
	applySetting(&flags.MutationBatchSizeSetting, ds.MutationBatchSize)
	applySetting(&flags.HardDeleteBatchSizeSetting, ds.HardDeleteBatchSize)
}

func applySetting(setting *flags.ConfigDefinition, val *uint) {
	if val == nil {
		*setting.Flag = setting.DefaultValue
		return
	}
	if *val < setting.MinValue {
		log.Warn(fmt.Sprintf("%s: value %d is below minimum %d; clamping to %d", setting.Name, *val, setting.MinValue, setting.MinValue))
		*setting.Flag = setting.MinValue
		return
	}
	if *val > setting.MaxValue {
		log.Warn(fmt.Sprintf("%s: value %d exceeds maximum %d; clamping to %d", setting.Name, *val, setting.MaxValue, setting.MaxValue))
		*setting.Flag = setting.MaxValue
		return
	}
	*setting.Flag = *val
}
