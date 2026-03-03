package db

import (
	"context"
	"testing"

	"fivetran.com/fivetran_sdk/destination/db/config"
	"github.com/stretchr/testify/require"
)

func getTestConnection(t *testing.T, ctx context.Context, configuration map[string]string) *ClickHouseConnection {
	t.Helper()
	connConfig, err := config.Parse(configuration)
	require.NoError(t, err)
	conn, err := GetClickHouseConnection(ctx, connConfig)
	require.NoError(t, err)
	return conn
}
