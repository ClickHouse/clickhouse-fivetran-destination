package db

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"fivetran.com/fivetran_sdk/destination/common/flags"
	"fivetran.com/fivetran_sdk/destination/common/types"
	"fivetran.com/fivetran_sdk/destination/db/config"
	pb "fivetran.com/fivetran_sdk/proto"
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
	connConfig, err := config.Parse(map[string]string{
		"host":     "localhost",
		"port":     "9999",
		"username": "default",
		"local":    "true",
	})
	require.NoError(t, err)
	conn, err := GetClickHouseConnection(ctx, connConfig)
	assert.ErrorContains(t, err, "ClickHouse connection error: ping failed after 3 attempts: dial tcp [::1]:9999: connect: connection refused")
	assert.Nil(t, conn)
}

func TestGetConnectionInvalidUsername(t *testing.T) {
	ctx := context.Background()
	connConfig, err := config.Parse(map[string]string{
		"host":     "localhost",
		"port":     "9000",
		"username": "invalid-user",
		"local":    "true",
	})
	require.NoError(t, err)
	conn, err := GetClickHouseConnection(ctx, connConfig)
	assert.ErrorContains(t, err, "ClickHouse connection error: code: 516, message: invalid-user: Authentication failed")
	assert.Nil(t, conn)

	connConfig, err = config.Parse(map[string]string{
		"host":     "localhost",
		"port":     "9000",
		"username": "default",
		"password": "invalid-password",
		"local":    "true",
	})
	require.NoError(t, err)
	conn, err = GetClickHouseConnection(ctx, connConfig)
	assert.ErrorContains(t, err, "ClickHouse connection error")
	assert.Nil(t, conn)
}

func TestConnection(t *testing.T) {
	ctx := context.Background()
	conn := getTestConnection(t, ctx, map[string]string{
		"host":     "localhost",
		"port":     "9000",
		"username": "default",
		"local":    "true",
	})
	defer conn.Close()

	err := conn.ConnectionTest(ctx)
	require.NoError(t, err)
}

func TestGrants(t *testing.T) {
	guid := func() string {
		return strings.ReplaceAll(uuid.New().String(), "-", "")
	}

	var err error
	ctx := context.Background()
	defaultConn := getTestConnection(t, ctx, map[string]string{
		"host":     "localhost",
		"port":     "9000",
		"username": "default",
		"local":    "true",
	})

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

	conn := getTestConnection(t, ctx, map[string]string{
		"host":     "localhost",
		"port":     "9000",
		"username": username,
		"password": password,
		"local":    "true",
	})
	defer conn.Close()

	addGrant := func(grant string) {
		grantCreateDatabaseStatement := fmt.Sprintf("GRANT %s TO %s", grant, username)
		fmt.Printf("%s\n", grantCreateDatabaseStatement)
		err = defaultConn.ExecStatement(ctx, grantCreateDatabaseStatement, "", false)
		require.NoError(t, err)
	}

	missingPart := "user is missing the required grants on *.*: "

	// users start with zero privileges and the first check immediately fails
	err = conn.GrantsTest(ctx)
	assert.ErrorContains(t, err, "To execute this query, it's necessary to have the grant SELECT(access_type, database, `table`, column, user_name) ON system.grants")

	// gradually add more privileges
	addGrant("SELECT ON system.grants")
	err = conn.GrantsTest(ctx)
	assert.ErrorContains(t, err, missingPart+"ALTER, CREATE DATABASE, CREATE TABLE, INSERT, SELECT")

	addGrant("ALTER ON *.*")
	err = conn.GrantsTest(ctx)
	assert.ErrorContains(t, err, missingPart+"CREATE DATABASE, CREATE TABLE, INSERT, SELECT")

	addGrant("CREATE DATABASE ON *.*")
	err = conn.GrantsTest(ctx)
	assert.ErrorContains(t, err, missingPart+"CREATE TABLE, INSERT, SELECT")

	addGrant("CREATE TABLE ON *.*")
	err = conn.GrantsTest(ctx)
	assert.ErrorContains(t, err, missingPart+"INSERT, SELECT")

	addGrant("INSERT ON *.*")
	err = conn.GrantsTest(ctx)
	assert.ErrorContains(t, err, missingPart+"SELECT")

	addGrant("SELECT ON *.*")
	err = conn.GrantsTest(ctx)
	require.NoError(t, err)
}

func TestDescribeTable(t *testing.T) {
	conn := getTestConnection(t, context.Background(), map[string]string{
		"host":     "localhost",
		"port":     "9000",
		"username": "default",
		"local":    "true",
	})
	defer conn.Close()

	dbName := "fivetran_test"
	tableName := fmt.Sprintf("test_describe_table_%s", strings.ReplaceAll(uuid.New().String(), "-", "_"))

	err := conn.Exec(context.Background(), fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName))
	require.NoError(t, err)

	err = conn.Exec(context.Background(), fmt.Sprintf(`
		CREATE OR REPLACE TABLE %s.%s (
			nb    Nullable(Bool),
			ni16  Nullable(Int16),
			ni32  Nullable(Int32),
			ni64  Nullable(Int64),
			nf32  Nullable(Float32),
			nf64  Nullable(Float64),
			ndd   Nullable(Decimal(18, 10)),
			nd    Nullable(Date),
			ndt   Nullable(DateTime),
			ndt64 Nullable(DateTime64(9, 'UTC')),
			ns    Nullable(String),
            nxml  Nullable(String) COMMENT 'XML',
			njson Nullable(String) COMMENT 'JSON',
            nbin  Nullable(String) COMMENT 'BIN',

			b     Bool,
			i16   Int16,
			i32   Int32,
			i64   Int64,
			f32   Float32,
			f64   Float64,
			dd    Decimal(4, 2),
			d     Date,
			dt    DateTime,
			dt64  DateTime64(9, 'UTC'),
			s     String,
            xml   String COMMENT 'XML',
			json  String COMMENT 'JSON',
            bin   String COMMENT 'BIN',
           
            _fivetran_id      String,
			_fivetran_synced  DateTime,
			_fivetran_deleted Bool
		) ENGINE MergeTree ORDER BY (b, i16, i32, i64, f32, f64, dd, d, dt, dt64, s, xml, json, bin)`,
		dbName, tableName))
	require.NoError(t, err)

	tableDescription, err := conn.DescribeTable(context.Background(), dbName, tableName)
	require.NoError(t, err)

	nb := &types.ColumnDefinition{Name: "nb", Type: "Nullable(Bool)", IsPrimaryKey: false}
	ni16 := &types.ColumnDefinition{Name: "ni16", Type: "Nullable(Int16)", IsPrimaryKey: false}
	ni32 := &types.ColumnDefinition{Name: "ni32", Type: "Nullable(Int32)", IsPrimaryKey: false}
	ni64 := &types.ColumnDefinition{Name: "ni64", Type: "Nullable(Int64)", IsPrimaryKey: false}
	nf32 := &types.ColumnDefinition{Name: "nf32", Type: "Nullable(Float32)", IsPrimaryKey: false}
	nf64 := &types.ColumnDefinition{Name: "nf64", Type: "Nullable(Float64)", IsPrimaryKey: false}
	ndd := &types.ColumnDefinition{Name: "ndd", Type: "Nullable(Decimal(18, 10))", IsPrimaryKey: false, DecimalParams: &pb.DecimalParams{Precision: 18, Scale: 10}}
	nd := &types.ColumnDefinition{Name: "nd", Type: "Nullable(Date)", IsPrimaryKey: false}
	ndt := &types.ColumnDefinition{Name: "ndt", Type: "Nullable(DateTime)", IsPrimaryKey: false}
	ndt64 := &types.ColumnDefinition{Name: "ndt64", Type: "Nullable(DateTime64(9, 'UTC'))", IsPrimaryKey: false}
	ns := &types.ColumnDefinition{Name: "ns", Type: "Nullable(String)", IsPrimaryKey: false}
	nxml := &types.ColumnDefinition{Name: "nxml", Type: "Nullable(String)", IsPrimaryKey: false, Comment: "XML"}
	njson := &types.ColumnDefinition{Name: "njson", Type: "Nullable(String)", IsPrimaryKey: false, Comment: "JSON"}
	nbin := &types.ColumnDefinition{Name: "nbin", Type: "Nullable(String)", IsPrimaryKey: false, Comment: "BIN"}
	b := &types.ColumnDefinition{Name: "b", Type: "Bool", IsPrimaryKey: true}
	i16 := &types.ColumnDefinition{Name: "i16", Type: "Int16", IsPrimaryKey: true}
	i32 := &types.ColumnDefinition{Name: "i32", Type: "Int32", IsPrimaryKey: true}
	i64 := &types.ColumnDefinition{Name: "i64", Type: "Int64", IsPrimaryKey: true}
	f32 := &types.ColumnDefinition{Name: "f32", Type: "Float32", IsPrimaryKey: true}
	f64 := &types.ColumnDefinition{Name: "f64", Type: "Float64", IsPrimaryKey: true}
	dd := &types.ColumnDefinition{Name: "dd", Type: "Decimal(4, 2)", IsPrimaryKey: true, DecimalParams: &pb.DecimalParams{Precision: 4, Scale: 2}}
	d := &types.ColumnDefinition{Name: "d", Type: "Date", IsPrimaryKey: true}
	dt := &types.ColumnDefinition{Name: "dt", Type: "DateTime", IsPrimaryKey: true}
	dt64 := &types.ColumnDefinition{Name: "dt64", Type: "DateTime64(9, 'UTC')", IsPrimaryKey: true}
	s := &types.ColumnDefinition{Name: "s", Type: "String", IsPrimaryKey: true}
	xml := &types.ColumnDefinition{Name: "xml", Type: "String", IsPrimaryKey: true, Comment: "XML"}
	json := &types.ColumnDefinition{Name: "json", Type: "String", IsPrimaryKey: true, Comment: "JSON"}
	bin := &types.ColumnDefinition{Name: "bin", Type: "String", IsPrimaryKey: true, Comment: "BIN"}
	fivetranId := &types.ColumnDefinition{Name: "_fivetran_id", Type: "String", IsPrimaryKey: false}
	fivetranSynced := &types.ColumnDefinition{Name: "_fivetran_synced", Type: "DateTime", IsPrimaryKey: false}
	fivetranDeleted := &types.ColumnDefinition{Name: "_fivetran_deleted", Type: "Bool", IsPrimaryKey: false}

	require.Equal(t, tableDescription, &types.TableDescription{
		Columns: []*types.ColumnDefinition{
			nb, ni16, ni32, ni64, nf32, nf64, ndd,
			nd, ndt, ndt64, ns, nxml, njson, nbin,
			b, i16, i32, i64, f32, f64, dd,
			d, dt, dt64, s, xml, json, bin,
			fivetranId, fivetranSynced, fivetranDeleted,
		},
		Mapping: map[string]*types.ColumnDefinition{
			"nb": nb, "ni16": ni16, "ni32": ni32, "ni64": ni64, "nf32": nf32, "nf64": nf64, "ndd": ndd,
			"nd": nd, "ndt": ndt, "ndt64": ndt64, "ns": ns, "nxml": nxml, "njson": njson, "nbin": nbin,
			"b": b, "i16": i16, "i32": i32, "i64": i64, "f32": f32, "f64": f64, "dd": dd,
			"d": d, "dt": dt, "dt64": dt64, "s": s, "xml": xml, "json": json, "bin": bin,
			"_fivetran_id": fivetranId, "_fivetran_synced": fivetranSynced, "_fivetran_deleted": fivetranDeleted,
		},
		PrimaryKeys: []string{"b", "i16", "i32", "i64", "f32", "f64", "dd", "d", "dt", "dt64", "s", "xml", "json", "bin"},
	})
}
