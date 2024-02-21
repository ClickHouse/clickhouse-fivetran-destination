package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetWithDefault(t *testing.T) {
	configuration := map[string]string{"key": "value"}
	assert.Equal(t, "value", GetWithDefault(configuration, "key", "default", false))
	assert.Equal(t, "default", GetWithDefault(configuration, "missing", "default", false))
	assert.Equal(t, "", GetWithDefault(configuration, "missing", "", false))
}

func TestGetWithDefaultTrim(t *testing.T) {
	configuration := map[string]string{"key": " value "}
	assert.Equal(t, "value", GetWithDefault(configuration, "key", "default", true))
	assert.Equal(t, " value ", GetWithDefault(configuration, "key", "default", false))
	assert.Equal(t, "default", GetWithDefault(configuration, "missing", "default", true))
}

func TestParseConfig(t *testing.T) {
	defaultConfig := Config{
		Host:     "localhost:9000",
		Database: "default",
		Username: "default",
		Password: "",
		Local:    false,
	}
	withHostOnly := defaultConfig
	withHostOnly.Host = "my.host:9440"
	withDatabaseOnly := defaultConfig
	withDatabaseOnly.Database = "my_db"
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
				"host":     "my.host:9440",
				"database": "my_db",
				"username": "5t",
				"password": "foo_bar",
			},
			expected: &Config{
				Host:     "my.host:9440",
				Database: "my_db",
				Username: "5t",
				Password: "foo_bar",
				Local:    false,
			},
		},
		{
			name:          "valid config (all defaults)",
			configuration: map[string]string{},
			expected: &Config{
				Host:     "localhost:9000",
				Database: "default",
				Username: "default",
				Password: "",
				Local:    false,
			},
		},
		{
			name:          "valid config (host only)",
			configuration: map[string]string{"host": "my.host:9440"},
			expected:      &withHostOnly,
		},
		{
			name:          "valid config (database only)",
			configuration: map[string]string{"database": "my_db"},
			expected:      &withDatabaseOnly,
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
			configuration: map[string]string{"host": "ch:foo"},
			expectedError: "ch:foo port foo must be a number in range [1, 65535]",
		},
		{
			name:          "invalid port > 65535",
			configuration: map[string]string{"host": "ch:65536"},
			expectedError: "ch:65536 port 65536 must be in range [1, 65535]",
		},
		{
			name:          "invalid port 0",
			configuration: map[string]string{"host": "ch:0"},
			expectedError: "ch:0 port 0 must be in range [1, 65535]",
		},
		{
			name:          "invalid port < 0",
			configuration: map[string]string{"host": "ch:-1"},
			expectedError: "ch:-1 port -1 must be in range [1, 65535]",
		},
	}
	for _, test := range tests {
		actual, err := Parse(test.configuration)
		assert.ErrorContains(t, err, test.expectedError, "Test %s", test.name)
		assert.Equal(t, (*Config)(nil), actual, "Test %s", test.name)
	}
}
