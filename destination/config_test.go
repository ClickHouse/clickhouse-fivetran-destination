package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseSDKConfig(t *testing.T) {
	defaultConfig := SDKConfig{
		Hostname:       "localhost",
		Port:           9000,
		Database:       "default",
		Username:       "default",
		Password:       "",
		DeploymentType: SDKConfigDeploymentTypeSingleNode,
		SSL: SSLConfig{
			enabled:    false,
			skipVerify: false,
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
	withSSL.SSL.enabled = true
	withSSL.SSL.skipVerify = false
	withSSLSkipVerify := defaultConfig
	withSSLSkipVerify.SSL.enabled = true
	withSSLSkipVerify.SSL.skipVerify = true
	tests := []struct {
		name          string
		configuration map[string]string
		expected      *SDKConfig
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
			expected: &SDKConfig{
				Hostname:       "my.host",
				Port:           9440,
				Database:       "my_db",
				Username:       "5t",
				Password:       "foo_bar",
				DeploymentType: "ClickHouse Cloud",
				SSL: SSLConfig{
					enabled:    true,
					skipVerify: true,
				},
			},
		},
		{
			name:          "valid config (all defaults)",
			configuration: map[string]string{},
			expected: &SDKConfig{
				Hostname:       "localhost",
				Port:           9000,
				Database:       "default",
				Username:       "default",
				Password:       "",
				DeploymentType: "On-premise single node",
				SSL: SSLConfig{
					enabled:    false,
					skipVerify: false,
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
		actual, err := ParseSDKConfig(test.configuration)
		assert.NoError(t, err, "Test %s", test.name)
		assert.Equal(t, test.expected, actual, "Test %s", test.name)
	}
}

func TestParseSDKConfigErrors(t *testing.T) {
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
			name:          "invalid cloud",
			configuration: map[string]string{"cloud": "not-a-boolean"},
			expectedError: "cloud must be a boolean",
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
		actual, err := ParseSDKConfig(test.configuration)
		assert.ErrorContains(t, err, test.expectedError, "Test %s", test.name)
		assert.Equal(t, (*SDKConfig)(nil), actual, "Test %s", test.name)
	}
}
