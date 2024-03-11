package config

import (
	"fmt"
	"strconv"
	"strings"
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
	if strings.Index(host, ":") != -1 {
		return "", fmt.Errorf("host %s should not contain protocol or port", host)
	}
	if strings.Index(host, "/") != -1 {
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
