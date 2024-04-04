package db

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"fivetran.com/fivetran_sdk/destination/common/types"
	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestCheckScanTypes(t *testing.T) {
	conn, err := GetClickHouseConnection(
		context.Background(),
		map[string]string{
			"host":     "localhost",
			"port":     "9000",
			"username": "default",
			"local":    "true",
		})
	require.NoError(t, err)
	defer conn.Close()

	tableName := fmt.Sprintf("test_check_scan_types_%s", strings.ReplaceAll(uuid.New().String(), "-", "_"))
	err = conn.Exec(context.Background(), fmt.Sprintf(`
		CREATE OR REPLACE TABLE default.%s (
			nb    Nullable(Bool),
			ni16  Nullable(Int16),
			ni32  Nullable(Int32),
			ni64  Nullable(Int64),
			nf32  Nullable(Float32),
			nf64  Nullable(Float64),
			ndd   Nullable(Decimal(4, 2)),
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
		) ENGINE MergeTree ORDER BY (b, i16, i32, i64, f32, f64, dd, d, dt, dt64, s, xml, json, bin)`, tableName))
	require.NoError(t, err)

	fivetranColMap := map[string]*pb.Column{
		"nb":    {Name: "nb", Type: pb.DataType_BOOLEAN, PrimaryKey: false},
		"ni16":  {Name: "ni16", Type: pb.DataType_SHORT, PrimaryKey: false},
		"ni32":  {Name: "ni32", Type: pb.DataType_INT, PrimaryKey: false},
		"ni64":  {Name: "ni64", Type: pb.DataType_LONG, PrimaryKey: false},
		"nf32":  {Name: "nf32", Type: pb.DataType_FLOAT, PrimaryKey: false},
		"nf64":  {Name: "nf64", Type: pb.DataType_DOUBLE, PrimaryKey: false},
		"ndd":   {Name: "ndd", Type: pb.DataType_DECIMAL, PrimaryKey: false},
		"nd":    {Name: "nd", Type: pb.DataType_NAIVE_DATE, PrimaryKey: false},
		"ndt":   {Name: "ndt", Type: pb.DataType_NAIVE_DATETIME, PrimaryKey: false},
		"ndt64": {Name: "ndt64", Type: pb.DataType_NAIVE_DATETIME, PrimaryKey: false},
		"ns":    {Name: "ns", Type: pb.DataType_STRING, PrimaryKey: false},
		"nxml":  {Name: "nxml", Type: pb.DataType_XML, PrimaryKey: false},
		"njson": {Name: "njson", Type: pb.DataType_JSON, PrimaryKey: false},
		"nbin":  {Name: "nbin", Type: pb.DataType_BINARY, PrimaryKey: false},

		"b":    {Name: "b", Type: pb.DataType_BOOLEAN, PrimaryKey: true},
		"i16":  {Name: "i16", Type: pb.DataType_SHORT, PrimaryKey: true},
		"i32":  {Name: "i32", Type: pb.DataType_INT, PrimaryKey: true},
		"i64":  {Name: "i64", Type: pb.DataType_LONG, PrimaryKey: true},
		"f32":  {Name: "f32", Type: pb.DataType_FLOAT, PrimaryKey: true},
		"f64":  {Name: "f64", Type: pb.DataType_DOUBLE, PrimaryKey: true},
		"dd":   {Name: "dd", Type: pb.DataType_DECIMAL, PrimaryKey: true},
		"d":    {Name: "d", Type: pb.DataType_NAIVE_DATE, PrimaryKey: true},
		"dt":   {Name: "dt", Type: pb.DataType_NAIVE_DATETIME, PrimaryKey: true},
		"dt64": {Name: "dt64", Type: pb.DataType_NAIVE_DATETIME, PrimaryKey: true},
		"s":    {Name: "s", Type: pb.DataType_STRING, PrimaryKey: true},
		"xml":  {Name: "xml", Type: pb.DataType_XML, PrimaryKey: true},
		"json": {Name: "json", Type: pb.DataType_JSON, PrimaryKey: true},
		"bin":  {Name: "bin", Type: pb.DataType_BINARY, PrimaryKey: true},

		"_fivetran_id":      {Name: "_fivetran_id", Type: pb.DataType_STRING, PrimaryKey: false},
		"_fivetran_deleted": {Name: "_fivetran_deleted", Type: pb.DataType_BOOLEAN, PrimaryKey: false},
		"_fivetran_synced":  {Name: "_fivetran_synced", Type: pb.DataType_NAIVE_DATETIME, PrimaryKey: false},
	}

	colTypes, err := conn.GetColumnTypes(context.Background(), "default", tableName)
	require.NoError(t, err)
	driverCols := types.MakeDriverColumns(colTypes)

	err = types.CheckScanTypes(fivetranColMap, driverCols.Mapping)
	require.NoError(t, err)

	err = types.CheckScanTypes(map[string]*pb.Column{
		"nb": {Name: "nb", Type: pb.DataType_BOOLEAN, PrimaryKey: false},
	}, driverCols.Mapping)
	require.ErrorContains(t, err, "columns count in the table definition (31) does not match the input file (1)")

	fivetranColMap["nb"] = &pb.Column{Name: "nb", Type: pb.DataType_UNSPECIFIED, PrimaryKey: false}
	err = types.CheckScanTypes(fivetranColMap, driverCols.Mapping)
	require.ErrorContains(t, err, "unknown Fivetran data type UNSPECIFIED")

	fivetranColMap["nb"] = &pb.Column{Name: "nb", Type: pb.DataType_STRING, PrimaryKey: false}
	err = types.CheckScanTypes(fivetranColMap, driverCols.Mapping)
	require.ErrorContains(t, err, "type Nullable(Bool) (scan type: *bool) which is incompatible with the input STRING (scan type: *string)")
}
