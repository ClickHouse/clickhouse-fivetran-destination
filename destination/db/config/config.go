package config

import (
	"fmt"
	"strconv"
)

type DeploymentType string

// values are used as labels, so they have to be human-readable (see the definition of fivetran_sdk.DropdownField)
const (
	DeploymentTypeSingleNode      = "On-premise single node"
	DeploymentTypeCluster         = "On-premise cluster"
	DeploymentTypeClickHouseCloud = "ClickHouse Cloud"
)

const (
	HostnameKey       = "hostname"
	PortKey           = "port"
	DatabaseKey       = "database"
	UsernameKey       = "username"
	PasswordKey       = "password"
	SSLKey            = "ssl"
	SSLSkipVerifyKey  = "ssl_skip_verification"
	DeploymentTypeKey = "deployment_type"
)

type SSLConfig struct {
	Enabled    bool
	SkipVerify bool
}

type Config struct {
	Hostname       string
	Port           uint
	Database       string
	Username       string
	Password       string
	DeploymentType DeploymentType
	SSL            SSLConfig
}

// Parse ClickHouse connection config from a Fivetran config map that we receive on every GRPC call.
func Parse(configuration map[string]string) (*Config, error) {
	portStr := GetWithDefault(configuration, PortKey, "9000")
	portInt, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("%s must be a number in range [1, 65535]", PortKey)
	}
	if portInt < 1 || portInt > 65535 {
		return nil, fmt.Errorf("%s must be in range [1, 65535]", PortKey)
	}
	sslEnabledStr := GetWithDefault(configuration, SSLKey, "false")
	sslEnabled, err := strconv.ParseBool(sslEnabledStr)
	if err != nil {
		return nil, fmt.Errorf("%s must be a boolean", SSLKey)
	}
	sslConfig := SSLConfig{
		Enabled:    sslEnabled,
		SkipVerify: false,
	}
	if sslEnabled {
		sslSkipVerifyStr := GetWithDefault(configuration, SSLSkipVerifyKey, "false")
		sslSkipVerify, err := strconv.ParseBool(sslSkipVerifyStr)
		if err != nil {
			return nil, fmt.Errorf("%s must be a boolean", SSLSkipVerifyKey)
		}
		sslConfig.SkipVerify = sslSkipVerify
	}
	deploymentType := GetWithDefault(configuration, DeploymentTypeKey, DeploymentTypeSingleNode)
	switch deploymentType {
	case DeploymentTypeSingleNode, DeploymentTypeCluster, DeploymentTypeClickHouseCloud:
		break
	default:
		return nil, fmt.Errorf("%s must be one of %s, %s, %s", DeploymentTypeKey,
			DeploymentTypeSingleNode, DeploymentTypeCluster, DeploymentTypeClickHouseCloud)
	}
	return &Config{
		Hostname:       GetWithDefault(configuration, HostnameKey, "localhost"),
		Port:           uint(portInt),
		Database:       GetWithDefault(configuration, DatabaseKey, "default"),
		Username:       GetWithDefault(configuration, UsernameKey, "default"),
		Password:       GetWithDefault(configuration, PasswordKey, ""),
		DeploymentType: DeploymentType(deploymentType),
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
