package db

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"fivetran.com/fivetran_sdk/destination/common/flags"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetConnectionFailureAfterMaxRetries(t *testing.T) {
	currentMaxRetries := *flags.MaxRetries
	currentMaxDelayMs := *flags.InitialRetryDelayMilliseconds
	*flags.MaxRetries = 3
	*flags.InitialRetryDelayMilliseconds = 10
	defer func() {
		*flags.MaxRetries = currentMaxRetries
		*flags.InitialRetryDelayMilliseconds = currentMaxDelayMs
	}()

	ctx := context.Background()
	conn, err := GetClickHouseConnection(ctx, map[string]string{
		"host":     "localhost",
		"port":     "9999",
		"username": "default",
		"local":    "true",
	})
	assert.ErrorContains(t, err, "ClickHouse connection error: ping failed after 3 attempts: dial tcp [::1]:9999: connect: connection refused")
	assert.Nil(t, conn)
}

func TestGetConnectionInvalidUsername(t *testing.T) {
	ctx := context.Background()
	conn, err := GetClickHouseConnection(ctx, map[string]string{
		"host":     "localhost",
		"port":     "9000",
		"username": "invalid-user",
		"local":    "true",
	})
	assert.ErrorContains(t, err, "ClickHouse connection error: code: 516, message: invalid-user: Authentication failed")
	assert.Nil(t, conn)

	conn, err = GetClickHouseConnection(ctx, map[string]string{
		"host":     "localhost",
		"port":     "9000",
		"username": "default",
		"password": "invalid-password",
		"local":    "true",
	})
	assert.ErrorContains(t, err, "ClickHouse connection error: code: 516, message: default: Authentication failed")
	assert.Nil(t, conn)
}

func TestConnection(t *testing.T) {
	ctx := context.Background()
	conn, err := GetClickHouseConnection(ctx, map[string]string{
		"host":     "localhost",
		"port":     "9000",
		"username": "default",
		"local":    "true",
	})
	require.NoError(t, err)
	defer conn.Close()

	err = conn.ConnectionTest(ctx)
	require.NoError(t, err)
}

func TestGrants(t *testing.T) {
	guid := func() string {
		return strings.ReplaceAll(uuid.New().String(), "-", "")
	}

	ctx := context.Background()
	defaultConn, err := GetClickHouseConnection(ctx, map[string]string{
		"host":     "localhost",
		"port":     "9000",
		"username": "default",
		"local":    "true",
	})
	require.NoError(t, err)

	username := fmt.Sprintf("test_grants_user_%s", guid())
	password := fmt.Sprintf("secret_%s", guid())
	defer func() {
		dropUserStatement := fmt.Sprintf("DROP USER IF EXISTS %s", username)
		err = defaultConn.ExecStatement(ctx, dropUserStatement, "[TestGrants] DropUser", false)
		assert.NoError(t, err)
		err = defaultConn.Close()
		require.NoError(t, err)
	}()

	createUserStatement := fmt.Sprintf("CREATE USER %s IDENTIFIED BY '%s'", username, password)
	err = defaultConn.ExecStatement(ctx, createUserStatement, "[TestGrants] CreateUser", false)
	require.NoError(t, err)

	conn, err := GetClickHouseConnection(ctx, map[string]string{
		"host":     "localhost",
		"port":     "9000",
		"username": username,
		"password": password,
		"local":    "true",
	})
	require.NoError(t, err)
	defer conn.Close()

	addGrant := func(grant string) {
		grantCreateDatabaseStatement := fmt.Sprintf("GRANT %s ON *.* TO %s", grant, username)
		err = defaultConn.ExecStatement(ctx, grantCreateDatabaseStatement, "", false)
		require.NoError(t, err)
	}

	// users start with zero privileges and the first check immediately fails
	err = conn.GrantsTest(ctx)
	assert.ErrorContains(t, err, "it's necessary to have the grant SHOW USERS, SHOW ROLES ON *.*")

	// gradually add more privileges
	addGrant("SHOW USERS, SHOW ROLES")
	err = conn.GrantsTest(ctx)
	assert.ErrorContains(t, err, "user is missing the required grants: "+
		"ALTER ADD COLUMN, ALTER DELETE, ALTER DROP COLUMN, ALTER MODIFY COLUMN, ALTER UPDATE, "+
		"CREATE DATABASE, CREATE TABLE, INSERT, SELECT, SHOW COLUMNS, SHOW TABLES")

	addGrant("ALTER ADD COLUMN")
	err = conn.GrantsTest(ctx)
	assert.ErrorContains(t, err, "user is missing the required grants: "+
		"ALTER DELETE, ALTER DROP COLUMN, ALTER MODIFY COLUMN, ALTER UPDATE, "+
		"CREATE DATABASE, CREATE TABLE, INSERT, SELECT, SHOW COLUMNS, SHOW TABLES")

	addGrant("ALTER DELETE")
	err = conn.GrantsTest(ctx)
	assert.ErrorContains(t, err, "user is missing the required grants: "+
		"ALTER DROP COLUMN, ALTER MODIFY COLUMN, ALTER UPDATE, "+
		"CREATE DATABASE, CREATE TABLE, INSERT, SELECT, SHOW COLUMNS, SHOW TABLES")

	addGrant("ALTER DROP COLUMN")
	err = conn.GrantsTest(ctx)
	assert.ErrorContains(t, err, "user is missing the required grants: "+
		"ALTER MODIFY COLUMN, ALTER UPDATE, CREATE DATABASE, CREATE TABLE, INSERT, SELECT, SHOW COLUMNS, SHOW TABLES")

	addGrant("ALTER MODIFY COLUMN")
	err = conn.GrantsTest(ctx)
	assert.ErrorContains(t, err, "user is missing the required grants: "+
		"ALTER UPDATE, CREATE DATABASE, CREATE TABLE, INSERT, SELECT, SHOW COLUMNS, SHOW TABLES")

	addGrant("ALTER UPDATE")
	err = conn.GrantsTest(ctx)
	assert.ErrorContains(t, err, "user is missing the required grants: "+
		"CREATE DATABASE, CREATE TABLE, INSERT, SELECT, SHOW COLUMNS, SHOW TABLES")

	addGrant("CREATE DATABASE")
	err = conn.GrantsTest(ctx)
	assert.ErrorContains(t, err, "user is missing the required grants: "+
		"CREATE TABLE, INSERT, SELECT, SHOW COLUMNS, SHOW TABLES")

	addGrant("CREATE TABLE")
	err = conn.GrantsTest(ctx)
	assert.ErrorContains(t, err, "user is missing the required grants: INSERT, SELECT, SHOW COLUMNS, SHOW TABLES")

	addGrant("INSERT")
	err = conn.GrantsTest(ctx)
	assert.ErrorContains(t, err, "user is missing the required grants: SELECT, SHOW COLUMNS, SHOW TABLES")

	addGrant("SELECT")
	err = conn.GrantsTest(ctx)
	assert.ErrorContains(t, err, "user is missing the required grants: SHOW COLUMNS, SHOW TABLES")

	addGrant("SHOW COLUMNS")
	err = conn.GrantsTest(ctx)
	assert.ErrorContains(t, err, "user is missing the required grants: SHOW TABLES")

	addGrant("SHOW TABLES")
	err = conn.GrantsTest(ctx)
	require.NoError(t, err)
}
