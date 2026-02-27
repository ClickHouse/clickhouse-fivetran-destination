package config

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"fivetran.com/fivetran_sdk/destination/common/flags"
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
// from the Fivetran configuration map. If destination settings are present,
// they are validated and applied to the global flags.
func ParseAll(configuration map[string]string) (*Config, error) {
	advancedCfg, err := ParseAdvancedConfig(configuration)
	if err != nil {
		return nil, fmt.Errorf("failed to parse advanced config: %w", err)
	}
	if advancedCfg.DestinationSettings != nil {
		if err := ValidateAndOverwriteFlags(advancedCfg.DestinationSettings); err != nil {
			return nil, fmt.Errorf("invalid destination settings: %w", err)
		}
	}
	return Parse(configuration)
}

// --- Advanced Configuration ---

const AdvancedConfigKey = "advanced_config"

// AdvancedConfig is the top-level structure of the optional JSON configuration file
// uploaded via the Fivetran setup form.
type AdvancedConfig struct {
	DestinationSettings *DestinationSettings `json:"destination_settings,omitempty"`
}

// DestinationSettings controls the internal behavior of the destination connector.
type DestinationSettings struct {
	WriteBatchSize      *uint `json:"write_batch_size,omitempty"`
	SelectBatchSize     *uint `json:"select_batch_size,omitempty"`
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

	return &cfg, nil
}

// ValidateAndOverwriteFlags overrides the global flag values with values from the
// parsed DestinationSettings. Nil fields are left at their flag defaults.
// Returns an error if any value is outside its allowed range.
func ValidateAndOverwriteFlags(ds *DestinationSettings) error {
	if ds == nil {
		return nil
	}
	if err := applySetting(&flags.WriteBatchSizeSetting, ds.WriteBatchSize); err != nil {
		return err
	}
	if err := applySetting(&flags.SelectBatchSizeSetting, ds.SelectBatchSize); err != nil {
		return err
	}
	if err := applySetting(&flags.HardDeleteBatchSizeSetting, ds.HardDeleteBatchSize); err != nil {
		return err
	}
	return nil
}

func applySetting(setting *flags.SettingDefinition, val *uint) error {
	if val == nil {
		*setting.Flag = setting.DefaultValue
		return nil
	}
	if *val < setting.MinValue || *val > setting.MaxValue {
		return fmt.Errorf("%s: value %d out of allowed range [%d, %d]", setting.Name, *val, setting.MinValue, setting.MaxValue)
	}
	*setting.Flag = *val
	return nil
}
