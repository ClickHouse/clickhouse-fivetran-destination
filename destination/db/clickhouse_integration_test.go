package db

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
		err = defaultConn.ExecDDL(ctx, dropUserStatement, "[TestGrants] DropUser")
		assert.NoError(t, err)
		err = defaultConn.Close()
		require.NoError(t, err)
	}()

	createUserStatement := fmt.Sprintf("CREATE USER %s IDENTIFIED BY '%s'", username, password)
	err = defaultConn.ExecDDL(ctx, createUserStatement, "[TestGrants] CreateUser")
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
		err = defaultConn.ExecDDL(ctx, grantCreateDatabaseStatement, "")
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
