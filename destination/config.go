package main

import (
	"fmt"
	"strconv"
)

type sdkConfigDeploymentType string

const (
	SDKConfigDeploymentTypeSingleNode      = "On-premise single node"
	SDKConfigDeploymentTypeCluster         = "On-premise cluster"
	SDKConfigDeploymentTypeClickHouseCloud = "ClickHouse Cloud"
)

const (
	SDKConfigHostnameKey      = "hostname"
	SDKConfigPortKey          = "port"
	SDKConfigDatabaseKey      = "database"
	SDKConfigUsernameKey      = "username"
	SDKConfigPasswordKey      = "password"
	SDKConfigSSLKey           = "ssl"
	SDKConfigSSLSkipVerifyKey = "ssl_skip_verification"
	SDKConfigDeploymentType   = "deployment_type"
)

type SSLConfig struct {
	enabled    bool
	skipVerify bool
}

type SDKConfig struct {
	Hostname       string
	Port           uint
	Database       string
	Username       string
	Password       string
	DeploymentType sdkConfigDeploymentType
	SSL            SSLConfig
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
	deploymentType := GetWithDefault(configuration, SDKConfigDeploymentType, SDKConfigDeploymentTypeSingleNode)
	switch deploymentType {
	case SDKConfigDeploymentTypeSingleNode, SDKConfigDeploymentTypeCluster, SDKConfigDeploymentTypeClickHouseCloud:
		break
	default:
		return nil, fmt.Errorf("%s must be one of %s, %s, %s", SDKConfigDeploymentType,
			SDKConfigDeploymentTypeSingleNode, SDKConfigDeploymentTypeCluster, SDKConfigDeploymentTypeClickHouseCloud)
	}
	return &SDKConfig{
		Hostname:       GetWithDefault(configuration, SDKConfigHostnameKey, "localhost"),
		Port:           uint(portInt),
		Database:       GetWithDefault(configuration, SDKConfigDatabaseKey, "default"),
		Username:       GetWithDefault(configuration, SDKConfigUsernameKey, "default"),
		Password:       GetWithDefault(configuration, SDKConfigPasswordKey, ""),
		DeploymentType: sdkConfigDeploymentType(deploymentType),
		SSL:            sslConfig,
	}, nil
}

func GetWithDefault(configuration map[string]string, key string, default_ string) string {
	value, ok := configuration[key]
	if !ok || value == "" {
		return default_
	}
	return value
}
