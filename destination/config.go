package main

import (
	"fmt"
	"strconv"
)

const (
	SDKConfigHostnameKey      = "hostname"
	SDKConfigPortKey          = "port"
	SDKConfigDatabaseKey      = "database"
	SDKConfigUsernameKey      = "username"
	SDKConfigPasswordKey      = "password"
	SDKConfigNodesCountKey    = "nodes_count"
	SDKConfigSSLKey           = "ssl"
	SDKConfigSSLSkipVerifyKey = "ssl_skip_verification"
)

type SSLConfig struct {
	enabled    bool
	skipVerify bool
}

type SDKConfig struct {
	Hostname   string
	Port       uint
	Database   string
	Username   string
	Password   string
	NodesCount uint
	SSL        SSLConfig
}

func ParseSDKConfig(configuration map[string]string) (*SDKConfig, error) {
	portStr := GetWithDefault(configuration, SDKConfigPortKey, "9000")
	portInt, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("%s must be a number in range [1, 65535]", SDKConfigPortKey)
	}
	if portInt < 1 || portInt > 65535 {
		return nil, fmt.Errorf("%s must be in range [1, 65535]", SDKConfigPortKey)
	}
	nodesCountStr := GetWithDefault(configuration, SDKConfigNodesCountKey, "1")
	nodesCount, err := strconv.Atoi(nodesCountStr)
	if err != nil {
		return nil, fmt.Errorf("%s must be a number", SDKConfigNodesCountKey)
	}
	if nodesCount < 1 {
		return nil, fmt.Errorf("%s must be greater than 0", SDKConfigNodesCountKey)
	}
	sslEnabledStr := GetWithDefault(configuration, SDKConfigSSLKey, "false")
	sslEnabled, err := strconv.ParseBool(sslEnabledStr)
	if err != nil {
		return nil, fmt.Errorf("%s must be a boolean", SDKConfigSSLKey)
	}
	sslConfig := SSLConfig{
		enabled:    sslEnabled,
		skipVerify: false,
	}
	if sslEnabled {
		sslSkipVerifyStr := GetWithDefault(configuration, SDKConfigSSLSkipVerifyKey, "false")
		sslSkipVerify, err := strconv.ParseBool(sslSkipVerifyStr)
		if err != nil {
			return nil, fmt.Errorf("%s must be a boolean", SDKConfigSSLSkipVerifyKey)
		}
		sslConfig.skipVerify = sslSkipVerify
	}
	return &SDKConfig{
		Hostname:   GetWithDefault(configuration, SDKConfigHostnameKey, "localhost"),
		Port:       uint(portInt),
		Database:   GetWithDefault(configuration, SDKConfigDatabaseKey, "default"),
		Username:   GetWithDefault(configuration, SDKConfigUsernameKey, "default"),
		Password:   GetWithDefault(configuration, SDKConfigPasswordKey, ""),
		NodesCount: uint(nodesCount),
		SSL:        sslConfig,
	}, nil
}

func GetWithDefault(configuration map[string]string, key string, default_ string) string {
	value, ok := configuration[key]
	if !ok || value == "" {
		return default_
	}
	return value
}
