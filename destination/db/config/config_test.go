package config

import (
	"encoding/base64"
	"testing"

	"fivetran.com/fivetran_sdk/destination/common/flags"
	"github.com/stretchr/testify/assert"
)

func TestGetWithDefault(t *testing.T) {
	configuration := map[string]string{"key": "value"}
	assert.Equal(t, "value", getWithDefault(configuration, "key", "default", false))
	assert.Equal(t, "default", getWithDefault(configuration, "missing", "default", false))
	assert.Equal(t, "", getWithDefault(configuration, "missing", "", false))
}

func TestGetWithDefaultTrim(t *testing.T) {
	configuration := map[string]string{"key": " value "}
	assert.Equal(t, "value", getWithDefault(configuration, "key", "default", true))
	assert.Equal(t, " value ", getWithDefault(configuration, "key", "default", false))
	assert.Equal(t, "default", getWithDefault(configuration, "missing", "default", true))
}

func TestParseConfig(t *testing.T) {
	defaultConfig := Config{
		Host:     "localhost",
		Port:     9440,
		Username: "default",
		Password: "",
		Local:    false,
	}
	withHostOnly := defaultConfig
	withHostOnly.Host = "my.host"
	withPortOnly := defaultConfig
	withPortOnly.Port = 9441
	withUsernameOnly := defaultConfig
	withUsernameOnly.Username = "5t"
	withPasswordOnly := defaultConfig
	withPasswordOnly.Password = " foo_bar "
	tests := []struct {
		name          string
		configuration map[string]string
		expected      *Config
	}{
		{
			name: "valid config (all set)",
			configuration: map[string]string{
				"host":     "my.host",
				"port":     "9441",
				"username": "5t",
				"password": " foo_bar ",
			},
			expected: &Config{
				Host:     "my.host",
				Port:     9441,
				Username: "5t",
				Password: " foo_bar ",
				Local:    false,
			},
		},
		{
			name:          "valid config (all defaults)",
			configuration: map[string]string{},
			expected: &Config{
				Host:     "localhost",
				Port:     9440,
				Username: "default",
				Password: "",
				Local:    false,
			},
		},
		{
			name:          "valid config (host only)",
			configuration: map[string]string{"host": "my.host"},
			expected:      &withHostOnly,
		},
		{
			name:          "valid config (port only)",
			configuration: map[string]string{"port": "9441"},
			expected:      &withPortOnly,
		},
		{
			name:          "valid config (username only)",
			configuration: map[string]string{"username": "5t"},
			expected:      &withUsernameOnly,
		},
		{
			name:          "valid config (password only)",
			configuration: map[string]string{"password": " foo_bar "},
			expected:      &withPasswordOnly,
		},
	}
	for _, test := range tests {
		actual, err := Parse(test.configuration)
		assert.NoError(t, err, "Test %s", test.name)
		assert.Equal(t, test.expected, actual, "Test %s", test.name)
	}
}

func TestParseConfigErrors(t *testing.T) {
	tests := []struct {
		name          string
		configuration map[string]string
		expectedError string
	}{
		{
			name:          "invalid port",
			configuration: map[string]string{"port": "foo"},
			expectedError: "port foo must be a number in range [1, 65535]",
		},
		{
			name:          "invalid port > 65535",
			configuration: map[string]string{"port": "65536"},
			expectedError: "port 65536 must be in range [1, 65535]",
		},
		{
			name:          "invalid port 0",
			configuration: map[string]string{"port": "0"},
			expectedError: "port 0 must be in range [1, 65535]",
		},
		{
			name:          "invalid port < 0",
			configuration: map[string]string{"port": "-1"},
			expectedError: "port -1 must be in range [1, 65535]",
		},
	}
	for _, test := range tests {
		actual, err := Parse(test.configuration)
		assert.ErrorContains(t, err, test.expectedError, "Test %s", test.name)
		assert.Equal(t, (*Config)(nil), actual, "Test %s", test.name)
	}
}

func TestConfigHostValidation(t *testing.T) {
	parsed, err := Parse(map[string]string{"host": "my.host"})
	assert.NoError(t, err)
	assert.Equal(t, "my.host", parsed.Host)

	_, err = Parse(map[string]string{"host": "my.host:9000"})
	assert.ErrorContains(t, err, "host my.host:9000 should not contain protocol or port")

	_, err = Parse(map[string]string{"host": "my.host/path"})
	assert.ErrorContains(t, err, "host my.host/path should not contain path")

	_, err = Parse(map[string]string{"host": "tcp://my.host"})
	assert.ErrorContains(t, err, "host tcp://my.host should not contain protocol or port")
}

func encodeJSON(json string) string {
	return base64.StdEncoding.EncodeToString([]byte(json))
}

func parseAdvancedConfigFromRawJson(json string, shouldEncode ...bool) (*AdvancedConfig, error) {
	shouldEncodeFlag := len(shouldEncode) == 0 || shouldEncode[0]
	if shouldEncodeFlag {
		json = encodeJSON(json)
	}
	return ParseAdvancedConfig(map[string]string{
		AdvancedConfigKey: json,
	})
}

func TestParseAdvancedConfigEmpty(t *testing.T) {
	cfg, err := ParseAdvancedConfig(map[string]string{})
	assert.NoError(t, err)
	assert.NotNil(t, cfg)
	assert.Nil(t, cfg.DestinationSettings)
}

func TestParseAdvancedConfigInvalidBase64(t *testing.T) {
	_, err := parseAdvancedConfigFromRawJson("not-valid-base64!!!", false)
	assert.ErrorContains(t, err, "failed to decode advanced config")
}

func TestParseAdvancedConfigInvalidJSON(t *testing.T) {
	_, err := parseAdvancedConfigFromRawJson("{invalid json}")
	assert.ErrorContains(t, err, "failed to parse advanced config JSON")
}

func TestParseAdvancedConfigUnknownFieldsIgnored(t *testing.T) {
	cfg, err := parseAdvancedConfigFromRawJson(`{
		"destination_settings": { "write_batch_size": 100 },
		"some_future_field": "ignored"
	}`)
	assert.NoError(t, err)
	assert.NotNil(t, cfg.DestinationSettings)
	assert.Equal(t, uint(100), *cfg.DestinationSettings.WriteBatchSize)
}

func TestParseAdvancedConfigWithDestinationSettings(t *testing.T) {
	cfg, err := parseAdvancedConfigFromRawJson(`{
		"destination_settings": {
			"write_batch_size": 500000,
			"select_batch_size": 3000,
			"hard_delete_batch_size": 2000
		}
	}`)
	assert.NoError(t, err)

	assert.NotNil(t, cfg.DestinationSettings)
	assert.Equal(t, uint(500000), *cfg.DestinationSettings.WriteBatchSize)
	assert.Equal(t, uint(3000), *cfg.DestinationSettings.SelectBatchSize)
	assert.Equal(t, uint(2000), *cfg.DestinationSettings.HardDeleteBatchSize)
}

func TestParseAdvancedConfigPartialSettings(t *testing.T) {
	cfg, err := parseAdvancedConfigFromRawJson(`{
		"destination_settings": {
			"write_batch_size": 200000
		}
	}`)
	assert.NoError(t, err)
	assert.NotNil(t, cfg.DestinationSettings)
	assert.Equal(t, uint(200000), *cfg.DestinationSettings.WriteBatchSize)
	assert.Nil(t, cfg.DestinationSettings.SelectBatchSize)
	assert.Nil(t, cfg.DestinationSettings.HardDeleteBatchSize)
}

func TestValidateAndOverwriteFlagsNilLeftsFlagsUnchanged(t *testing.T) {
	originalWriteBatch := *flags.WriteBatchSize
	assert.NoError(t, ValidateAndOverwriteFlags(nil))
	assert.Equal(t, originalWriteBatch, *flags.WriteBatchSize)
}

func TestValidateAndOverwriteFlagsOverridesFlags(t *testing.T) {
	originalWriteBatch := *flags.WriteBatchSize
	originalSelectBatch := *flags.SelectBatchSize
	originalHardDeleteBatch := *flags.HardDeleteBatchSize
	defer func() {
		*flags.WriteBatchSize = originalWriteBatch
		*flags.SelectBatchSize = originalSelectBatch
		*flags.HardDeleteBatchSize = originalHardDeleteBatch
	}()

	writeBatch := flags.WriteBatchSizeSetting.MinValue + 1
	selectBatch := flags.SelectBatchSizeSetting.MinValue + 1
	hardDelete := flags.HardDeleteBatchSizeSetting.MinValue + 1

	ds := &DestinationSettings{
		WriteBatchSize:      &writeBatch,
		SelectBatchSize:     &selectBatch,
		HardDeleteBatchSize: &hardDelete,
	}
	assert.NoError(t, ValidateAndOverwriteFlags(ds))

	assert.Equal(t, writeBatch, *flags.WriteBatchSize)
	assert.Equal(t, selectBatch, *flags.SelectBatchSize)
	assert.Equal(t, hardDelete, *flags.HardDeleteBatchSize)
}

func TestValidateAndOverwriteFlagsPartialOverride(t *testing.T) {
	originalWriteBatch := *flags.WriteBatchSize
	originalSelectBatch := *flags.SelectBatchSize
	defer func() {
		*flags.WriteBatchSize = originalWriteBatch
		*flags.SelectBatchSize = originalSelectBatch
	}()

	writeBatch := flags.WriteBatchSizeSetting.MinValue + 1
	ds := &DestinationSettings{
		WriteBatchSize: &writeBatch,
	}
	assert.NoError(t, ValidateAndOverwriteFlags(ds))

	assert.Equal(t, writeBatch, *flags.WriteBatchSize)
	assert.Equal(t, flags.SelectBatchSizeSetting.DefaultValue, *flags.SelectBatchSize)
}

func uintPtr(v uint) *uint { return &v }

func TestValidateAndOverwriteFlagsRejectsOutOfRange(t *testing.T) {
	belowMin := flags.WriteBatchSizeSetting.MinValue - 1
	err := ValidateAndOverwriteFlags(&DestinationSettings{WriteBatchSize: &belowMin})
	assert.ErrorContains(t, err, "out of allowed range")

	aboveMax := flags.WriteBatchSizeSetting.MaxValue + 1
	err = ValidateAndOverwriteFlags(&DestinationSettings{WriteBatchSize: &aboveMax})
	assert.ErrorContains(t, err, "out of allowed range")
}

func TestValidateAndOverwriteFlagsAcceptsBoundaryValues(t *testing.T) {
	originalWriteBatch := *flags.WriteBatchSize
	originalSelectBatch := *flags.SelectBatchSize
	originalHardDeleteBatch := *flags.HardDeleteBatchSize
	defer func() {
		*flags.WriteBatchSize = originalWriteBatch
		*flags.SelectBatchSize = originalSelectBatch
		*flags.HardDeleteBatchSize = originalHardDeleteBatch
	}()

	ds := &DestinationSettings{
		WriteBatchSize:      uintPtr(flags.WriteBatchSizeSetting.MinValue),
		SelectBatchSize:     uintPtr(flags.SelectBatchSizeSetting.MaxValue),
		HardDeleteBatchSize: uintPtr(flags.HardDeleteBatchSizeSetting.MaxValue),
	}
	assert.NoError(t, ValidateAndOverwriteFlags(ds))
	assert.Equal(t, flags.WriteBatchSizeSetting.MinValue, *flags.WriteBatchSize)
	assert.Equal(t, flags.SelectBatchSizeSetting.MaxValue, *flags.SelectBatchSize)
	assert.Equal(t, flags.HardDeleteBatchSizeSetting.MaxValue, *flags.HardDeleteBatchSize)
}

func TestValidateAndOverwriteFlagsDoesNotModifyFlagsOnError(t *testing.T) {
	originalWriteBatch := *flags.WriteBatchSize
	originalSelectBatch := *flags.SelectBatchSize
	defer func() {
		*flags.WriteBatchSize = originalWriteBatch
		*flags.SelectBatchSize = originalSelectBatch
	}()

	validWrite := flags.WriteBatchSizeSetting.MinValue + 1
	invalidSelect := flags.SelectBatchSizeSetting.MaxValue + 1
	ds := &DestinationSettings{
		WriteBatchSize:  &validWrite,
		SelectBatchSize: &invalidSelect,
	}
	err := ValidateAndOverwriteFlags(ds)
	assert.Error(t, err)

	assert.Equal(t, validWrite, *flags.WriteBatchSize)
	assert.Equal(t, originalSelectBatch, *flags.SelectBatchSize)
}
