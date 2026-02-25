package config

import (
	"encoding/base64"
	"testing"

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

func TestParseAdvancedConfigEmpty(t *testing.T) {
	cfg, err := ParseAdvancedConfig(map[string]string{})
	assert.NoError(t, err)
	assert.NotNil(t, cfg)
	assert.Nil(t, cfg.Tables)
	assert.Nil(t, cfg.DestinationSettings)
	assert.Nil(t, cfg.ClickHouseQuerySettings)
}

func TestParseAdvancedConfigFullJSON(t *testing.T) {
	json := `{
		"destination_settings": {
			"write_batch_size": 500000,
			"max_parallel_selects": 20
		},
		"clickhouse_query_settings": {
			"max_insert_threads": 4
		},
		"tables": {
			"mydb.users": {
				"order_by": ["created_at", "user_id"],
				"settings": {"index_granularity": 2048}
			},
			"mydb.events": {
				"order_by": ["event_ts"]
			}
		}
	}`
	cfg, err := ParseAdvancedConfig(map[string]string{
		AdvancedConfigKey: encodeJSON(json),
	})
	assert.NoError(t, err)

	assert.NotNil(t, cfg.DestinationSettings)
	assert.Equal(t, uint(500000), *cfg.DestinationSettings.WriteBatchSize)
	assert.Equal(t, uint(20), *cfg.DestinationSettings.MaxParallelSelects)

	assert.Equal(t, float64(4), cfg.ClickHouseQuerySettings["max_insert_threads"])

	assert.Len(t, cfg.Tables, 2)
	assert.Equal(t, []string{"created_at", "user_id"}, cfg.Tables["mydb.users"].OrderBy)
	assert.Equal(t, map[string]any{"index_granularity": float64(2048)}, cfg.Tables["mydb.users"].Settings)
	assert.Equal(t, []string{"event_ts"}, cfg.Tables["mydb.events"].OrderBy)
}

func TestParseAdvancedConfigInvalidBase64(t *testing.T) {
	_, err := ParseAdvancedConfig(map[string]string{
		AdvancedConfigKey: "not-valid-base64!!!",
	})
	assert.ErrorContains(t, err, "failed to decode advanced config")
}

func TestParseAdvancedConfigInvalidJSON(t *testing.T) {
	_, err := ParseAdvancedConfig(map[string]string{
		AdvancedConfigKey: encodeJSON("{invalid json}"),
	})
	assert.ErrorContains(t, err, "failed to parse advanced config JSON")
}

func TestResolveTableSettingsWithMatch(t *testing.T) {
	cfg := &AdvancedConfig{
		Tables: map[string]*TableSettings{
			"mydb.users": {
				OrderBy: []string{"id", "ts"},
			},
		},
	}
	ts := cfg.ResolveTableSettings("mydb", "users")
	assert.Equal(t, []string{"id", "ts"}, ts.OrderBy)
}

func TestResolveTableSettingsNoMatch(t *testing.T) {
	cfg := &AdvancedConfig{
		Tables: map[string]*TableSettings{
			"mydb.users": {OrderBy: []string{"id"}},
		},
	}
	ts := cfg.ResolveTableSettings("mydb", "other_table")
	assert.Nil(t, ts.OrderBy)
}

func TestResolveTableSettingsNilConfig(t *testing.T) {
	var cfg *AdvancedConfig
	ts := cfg.ResolveTableSettings("mydb", "users")
	assert.Nil(t, ts.OrderBy)
}

func TestResolveTableSettingsEmptyTables(t *testing.T) {
	cfg := &AdvancedConfig{}
	ts := cfg.ResolveTableSettings("mydb", "users")
	assert.Nil(t, ts.OrderBy)
}
