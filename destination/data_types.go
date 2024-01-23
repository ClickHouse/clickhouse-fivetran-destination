package main

import (
	"errors"
	"fmt"
	"strings"

	pb "fivetran.com/fivetran_sdk/proto"
)

var (
	ClickHouseDataTypes = map[string]pb.DataType{
		"Bool":    pb.DataType_BOOLEAN,
		"Int8":    pb.DataType_SHORT,
		"Int16":   pb.DataType_SHORT,
		"Int32":   pb.DataType_INT,
		"Int64":   pb.DataType_LONG,
		"Float32": pb.DataType_FLOAT,
		"Float64": pb.DataType_DOUBLE,
		"Date":    pb.DataType_NAIVE_DATE,
		"Date32":  pb.DataType_NAIVE_DATE,
		"String":  pb.DataType_STRING,
		"UUID":    pb.DataType_STRING,
	}
	FivetranDataTypes = map[pb.DataType]string{
		pb.DataType_BOOLEAN:        "Bool",
		pb.DataType_SHORT:          "Int16",
		pb.DataType_INT:            "Int32",
		pb.DataType_LONG:           "Int64",
		pb.DataType_FLOAT:          "Float32",
		pb.DataType_DOUBLE:         "Float64",
		pb.DataType_DECIMAL:        "Decimal",
		pb.DataType_STRING:         "String",
		pb.DataType_BINARY:         "String",
		pb.DataType_XML:            "String",
		pb.DataType_NAIVE_DATE:     "Date",
		pb.DataType_NAIVE_DATETIME: "DateTime",
		pb.DataType_UTC_DATETIME:   "DateTime",
		pb.DataType_JSON:           "JSON",
	}
)

func GetFivetranDataType(colType string) pb.DataType {
	colType = RemoveLowCardinalityAndNullable(colType)
	dataType, ok := ClickHouseDataTypes[colType]
	if !ok {
		dataType = pb.DataType_UNSPECIFIED
	}
	return dataType
}

func GetClickHouseColumnType(dataType pb.DataType, decimalParams *pb.DecimalParams) (string, error) {
	colType, ok := FivetranDataTypes[dataType]
	if !ok {
		return "", errors.New(fmt.Sprintf("Unknown datatype %s", dataType.String()))
	}
	if colType == "Decimal" && decimalParams != nil {
		return ToDecimalTypeWithParams(decimalParams), nil
	}
	return colType, nil
}

func ToDecimalTypeWithParams(decimalParams *pb.DecimalParams) string {
	var (
		precision uint32
		scale     uint32
	)
	// See precision and scale valid ranges: https://clickhouse.com/docs/en/sql-reference/data-types/decimal
	if decimalParams.Precision > MaxDecimalPrecision {
		precision = MaxDecimalPrecision
	} else {
		precision = decimalParams.Precision
	}
	if decimalParams.Scale > precision {
		scale = precision
	} else {
		scale = decimalParams.Scale
	}
	return fmt.Sprintf("Decimal(%d, %d)", precision, scale)
}

func RemoveLowCardinalityAndNullable(colType string) string {
	if strings.HasPrefix(colType, "LowCardinality") {
		colType = colType[15 : len(colType)-1]
	}
	if strings.HasPrefix(colType, "Nullable") {
		colType = colType[9 : len(colType)-1]
	}
	return colType
}

/**
  i8            Int8,
  i16           Int16,
  i32           Int32,
  i64           Int64,
  i128          Int128,
  i256          Int256,
  ui8           UInt8,
  ui16          UInt16,
  ui32          UInt32,
  ui64          UInt64,
  ui128         UInt128,
  ui256         UInt256,
  f32           Float32,
  f64           Float64,
  dec32         Decimal32(2),
  dec64         Decimal64(2),
  dec128        Decimal128(2),
  dec128_native Decimal(35, 30),
  dec128_text   Decimal(35, 31),
  dec256        Decimal256(2),
  dec256_native Decimal(65, 2),
  dec256_text   Decimal(66, 2),
  p             Point,
  r             Ring,
  pg            Polygon,
  mpg           MultiPolygon,
  b             Bool,
  s             String,
  fs            FixedString(3),
  uuid          UUID,
  d             Date,
  d32           Date32,
  dt            DateTime,
  dt_tz1        DateTime('UTC'),
  dt_tz2        DateTime('Europe/Amsterdam'),
  dt64          DateTime64(3),
  dt64_3_tz1    DateTime64(3, 'UTC'),
  dt64_3_tz2    DateTime64(3, 'Asia/Shanghai'),
  dt64_6        DateTime64(6, 'UTC'),
  dt64_9        DateTime64(9, 'UTC'),
  enm           Enum('hallo' = 1, 'welt' = 2),
  agg           AggregateFunction(uniq, UInt64),
  sagg          SimpleAggregateFunction(sum, Double),
  a             Array(String),
  o             JSON,
  t             Tuple(Int32, String, Nullable(String), LowCardinality(String), LowCardinality(Nullable(String)), Tuple(Int32, String)),
  m             Map(Int32, String),
  m_complex     Map(Int32, Map(Int32, LowCardinality(Nullable(String)))),
  nested        Nested (col1 String, col2 UInt32),
  ip4           IPv4,
  ip6           IPv6,
  ns            Nullable(String),
  nfs           Nullable(FixedString(3)),
  ndt64         Nullable(DateTime64(3)),
  ndt64_tz      Nullable(DateTime64(3, 'Asia/Shanghai')),
  ls            LowCardinality(String),
  lfs           LowCardinality(FixedString(3)),
  lns           LowCardinality(Nullable(String)),
  lnfs          LowCardinality(Nullable(FixedString(3))),
*/
