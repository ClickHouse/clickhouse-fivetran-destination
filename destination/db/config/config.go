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

// TODO: validate host

// Parse ClickHouse connection config from a Fivetran config map that we receive on every GRPC call.
func Parse(configuration map[string]string) (*Config, error) {
	host := GetWithDefault(configuration, HostKey, "localhost", true)
	portStr := GetWithDefault(configuration, PortKey, "9000", true)
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("port %s must be a number in range [1, 65535]", portStr)
	}
	if port < 1 || port > 65535 {
		return nil, fmt.Errorf("port %s must be in range [1, 65535]", portStr)
	}
	return &Config{
		Host:     host,
		Port:     uint(port),
		Username: GetWithDefault(configuration, UsernameKey, "default", true),
		Password: GetWithDefault(configuration, PasswordKey, "", false),
		Local:    GetWithDefault(configuration, "local", "false", true) == "true",
	}, nil
}

func GetWithDefault(configuration map[string]string, key string, defaultValue string, trim bool) string {
	value, ok := configuration[key]
	if !ok || value == "" {
		return defaultValue
	}
	if trim {
		return strings.Trim(value, " ")
	}
	return value
}
