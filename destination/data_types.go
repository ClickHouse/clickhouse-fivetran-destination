package main

import (
	"fmt"
	"strconv"
	"strings"

	pb "fivetran.com/fivetran_sdk/proto"
)

var (
	ClickHouseDataTypes = map[string]pb.DataType{
		"Bool":                 pb.DataType_BOOLEAN,
		"Int16":                pb.DataType_SHORT,
		"Int32":                pb.DataType_INT,
		"Int64":                pb.DataType_LONG,
		"Float32":              pb.DataType_FLOAT,
		"Float64":              pb.DataType_DOUBLE,
		"Date":                 pb.DataType_NAIVE_DATE,
		"DateTime":             pb.DataType_NAIVE_DATETIME,
		"DateTime64(9, 'UTC')": pb.DataType_UTC_DATETIME,
		"String":               pb.DataType_STRING,
		"UUID":                 pb.DataType_STRING,
		"JSON":                 pb.DataType_JSON,
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
		pb.DataType_UTC_DATETIME:   "DateTime64(9, 'UTC')",
		pb.DataType_JSON:           "JSON",
	}
	FivetranMetadataColumnTypes = map[string]string{
		"_fivetran_id":      "String",
		"_fivetran_synced":  "DateTime64(9, 'UTC')",
		"_fivetran_deleted": "Bool",
	}
)

func GetFivetranDataType(colType string) (pb.DataType, *pb.DecimalParams, error) {
	colType = RemoveLowCardinalityAndNullable(colType)
	decimalParams := GetDecimalParams(colType)
	if decimalParams != nil {
		return pb.DataType_DECIMAL, decimalParams, nil
	}
	dataType, ok := ClickHouseDataTypes[colType]
	if !ok { // shouldn't happen if the tables are created by the connector
		return pb.DataType_UNSPECIFIED, nil, fmt.Errorf("can't map type %s to Fivetran types", colType)
	}
	return dataType, nil, nil
}

// GetClickHouseDataType
// - Fivetran Metadata fields have known types and are not Nullable
// - JSON fields are not Nullable by ClickHouse design
// - PrimaryKey fields are not Nullable (assumption)
// - all other fields are Nullable by default
func GetClickHouseDataType(col *pb.Column) (string, error) {
	metaColType, ok := FivetranMetadataColumnTypes[col.Name]
	if ok {
		return metaColType, nil
	}
	colType, ok := FivetranDataTypes[col.Type]
	if !ok {
		return "", fmt.Errorf("unknown datatype %s", col.Type.String())
	}
	if colType == "Decimal" && col.Decimal != nil {
		return ToDecimalTypeWithParams(col.Decimal), nil
	}
	if col.PrimaryKey || colType == "JSON" {
		return colType, nil
	}
	return fmt.Sprintf("Nullable(%s)", colType), nil
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

func GetDecimalParams(dataType string) *pb.DecimalParams {
	if strings.HasPrefix(dataType, "Decimal(") {
		decimalParams := strings.Split(dataType[8:len(dataType)-1], ",")
		if len(decimalParams) != 2 {
			return nil
		}
		precision, err := strconv.Atoi(decimalParams[0])
		if err != nil {
			return nil
		}
		scale, err := strconv.Atoi(decimalParams[1])
		if err != nil {
			return nil
		}
		return &pb.DecimalParams{
			Precision: uint32(precision),
			Scale:     uint32(scale),
		}
	}
	return nil
}
