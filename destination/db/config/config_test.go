package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetWithDefault(t *testing.T) {
	configuration := map[string]string{
		"key": "value",
	}
	assert.Equal(t, "value", GetWithDefault(configuration, "key", "default"))
	assert.Equal(t, "default", GetWithDefault(configuration, "missing", "default"))
	assert.Equal(t, "", GetWithDefault(configuration, "missing", ""))
}

func TestParseConfig(t *testing.T) {
	defaultConfig := Config{
		Hostname:       "localhost",
		Port:           9000,
		Database:       "default",
		Username:       "default",
		Password:       "",
		DeploymentType: DeploymentTypeSingleNode,
		SSL: SSLConfig{
			Enabled:    false,
			SkipVerify: false,
		},
	}
	withHostOnly := defaultConfig
	withHostOnly.Hostname = "my.host"
	withPortOnly := defaultConfig
	withPortOnly.Port = 9440
	withDatabaseOnly := defaultConfig
	withDatabaseOnly.Database = "my_db"
	withUsernameOnly := defaultConfig
	withUsernameOnly.Username = "5t"
	withPasswordOnly := defaultConfig
	withPasswordOnly.Password = "foo_bar"
	withCloudDeploymentOnly := defaultConfig
	withCloudDeploymentOnly.DeploymentType = "ClickHouse Cloud"
	withSingleNodeDeploymentOnly := defaultConfig
	withSingleNodeDeploymentOnly.DeploymentType = "On-premise single node"
	withClusterDeploymentOnly := defaultConfig
	withClusterDeploymentOnly.DeploymentType = "On-premise cluster"
	withSSL := defaultConfig
	withSSL.SSL.Enabled = true
	withSSL.SSL.SkipVerify = false
	withSSLSkipVerify := defaultConfig
	withSSLSkipVerify.SSL.Enabled = true
	withSSLSkipVerify.SSL.SkipVerify = true
	tests := []struct {
		name          string
		configuration map[string]string
		expected      *Config
	}{
		{
			name: "valid config (all set)",
			configuration: map[string]string{
				"hostname":              "my.host",
				"port":                  "9440",
				"database":              "my_db",
				"username":              "5t",
				"password":              "foo_bar",
				"ssl":                   "true",
				"ssl_skip_verification": "true",
				"deployment_type":       "ClickHouse Cloud",
			},
			expected: &Config{
				Hostname:       "my.host",
				Port:           9440,
				Database:       "my_db",
				Username:       "5t",
				Password:       "foo_bar",
				DeploymentType: "ClickHouse Cloud",
				SSL: SSLConfig{
					Enabled:    true,
					SkipVerify: true,
				},
			},
		},
		{
			name:          "valid config (all defaults)",
			configuration: map[string]string{},
			expected: &Config{
				Hostname:       "localhost",
				Port:           9000,
				Database:       "default",
				Username:       "default",
				Password:       "",
				DeploymentType: "On-premise single node",
				SSL: SSLConfig{
					Enabled:    false,
					SkipVerify: false,
				},
			},
		},
		{
			name:          "valid config (host only)",
			configuration: map[string]string{"hostname": "my.host"},
			expected:      &withHostOnly,
		},
		{
			name:          "valid config (port only)",
			configuration: map[string]string{"port": "9440"},
			expected:      &withPortOnly,
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
			configuration: map[string]string{"password": "foo_bar"},
			expected:      &withPasswordOnly,
		},
		{
			name:          "valid config (deployment cloud only)",
			configuration: map[string]string{"deployment_type": "ClickHouse Cloud"},
			expected:      &withCloudDeploymentOnly,
		},
		{
			name:          "valid config (deployment on-premise single node only)",
			configuration: map[string]string{"deployment_type": "On-premise single node"},
			expected:      &withSingleNodeDeploymentOnly,
		},
		{
			name:          "valid config (deployment on-premise cluster only)",
			configuration: map[string]string{"deployment_type": "On-premise cluster"},
			expected:      &withClusterDeploymentOnly,
		},
		{
			name:          "valid config (ssl only)",
			configuration: map[string]string{"ssl": "true"},
			expected:      &withSSL,
		},
		{
			name:          "valid config (ssl + ssl_skip_verification)",
			configuration: map[string]string{"ssl": "true", "ssl_skip_verification": "true"},
			expected:      &withSSLSkipVerify,
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
			configuration: map[string]string{"port": "not-a-number"},
			expectedError: "port must be a number in range [1, 65535]",
		},
		{
			name:          "invalid port > 65535",
			configuration: map[string]string{"port": "65536"},
			expectedError: "port must be in range [1, 65535]",
		},
		{
			name:          "invalid port 0",
			configuration: map[string]string{"port": "0"},
			expectedError: "port must be in range [1, 65535]",
		},
		{
			name:          "invalid port < 0",
			configuration: map[string]string{"port": "-1"},
			expectedError: "port must be in range [1, 65535]",
		},
		{
			name:          "invalid deployment type",
			configuration: map[string]string{"deployment_type": "wrong"},
			expectedError: "deployment_type must be one of On-premise single node, On-premise cluster, ClickHouse Cloud",
		},
		{
			name:          "invalid ssl",
			configuration: map[string]string{"ssl": "not-a-boolean"},
			expectedError: "ssl must be a boolean",
		},
		{
			name:          "invalid ssl_skip_verification",
			configuration: map[string]string{"ssl": "true", "ssl_skip_verification": "not-a-boolean"},
			expectedError: "ssl_skip_verification must be a boolean",
		},
	}
	for _, test := range tests {
		actual, err := Parse(test.configuration)
		assert.ErrorContains(t, err, test.expectedError, "Test %s", test.name)
		assert.Equal(t, (*Config)(nil), actual, "Test %s", test.name)
	}
}
