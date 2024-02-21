package config

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	HostKey     = "host"
	DatabaseKey = "database"
	UsernameKey = "username"
	PasswordKey = "password"
)

type Config struct {
	Host     string
	Database string
	Username string
	Password string
}

// Parse ClickHouse connection config from a Fivetran config map that we receive on every GRPC call.
func Parse(configuration map[string]string) (*Config, error) {
	host := GetWithDefault(configuration, HostKey, "localhost:9000", true)
	hostSplit := strings.Split(host, ":")
	if len(hostSplit) != 2 {
		return nil, fmt.Errorf("%s must be in the format address:port", host)
	}
	port, err := strconv.Atoi(hostSplit[1])
	if err != nil {
		return nil, fmt.Errorf("%s port %s must be a number in range [1, 65535]", host, hostSplit[1])
	}
	if port < 1 || port > 65535 {
		return nil, fmt.Errorf("%s port %s must be in range [1, 65535]", host, hostSplit[1])
	}
	return &Config{
		Host:     GetWithDefault(configuration, HostKey, "localhost:9000", true),
		Database: GetWithDefault(configuration, DatabaseKey, "default", true),
		Username: GetWithDefault(configuration, UsernameKey, "default", true),
		Password: GetWithDefault(configuration, PasswordKey, "", false),
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
