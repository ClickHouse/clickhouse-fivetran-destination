package config

import (
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
