package main

import (
	"fmt"
	"strings"

	pb "fivetran.com/fivetran_sdk/proto"
)

const (
	MaxDecimalPrecision = 76
	FivetranID          = "_fivetran_id"
	FivetranSynced      = "_fivetran_synced"
	FivetranDeleted     = "_fivetran_deleted"
	FivetranJSON        = "JSON"
	FivetranBinary      = "BINARY"
	FivetranXML         = "XML"
)

type ClickHouseType struct {
	Type    string
	Comment string
}

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
	}
	FivetranDataTypes = map[pb.DataType]ClickHouseType{
		pb.DataType_BOOLEAN:        {Type: "Bool"},
		pb.DataType_SHORT:          {Type: "Int16"},
		pb.DataType_INT:            {Type: "Int32"},
		pb.DataType_LONG:           {Type: "Int64"},
		pb.DataType_FLOAT:          {Type: "Float32"},
		pb.DataType_DOUBLE:         {Type: "Float64"},
		pb.DataType_DECIMAL:        {Type: "Decimal"},
		pb.DataType_NAIVE_DATE:     {Type: "Date"},
		pb.DataType_NAIVE_DATETIME: {Type: "DateTime"},
		pb.DataType_UTC_DATETIME:   {Type: "DateTime64(9, 'UTC')"},
		pb.DataType_STRING:         {Type: "String"},
	}
	// FivetranDataTypesWithComments
	// Fivetran STRING, XML, BINARY, JSON all are valid ClickHouse String types,
	// and by default we don't have a way to get the original Fivetran type from just a ClickHouse String.
	// So we add comments to the table columns using COMMENT clause to be able to distinguish them.
	// NB: ClickHouse has JSON data type, see https://clickhouse.com/docs/en/sql-reference/data-types/json
	// however, it's marked as experimental and not production ready, so we use String instead.
	FivetranDataTypesWithComments = map[pb.DataType]ClickHouseType{
		pb.DataType_JSON:   {Type: "String", Comment: FivetranJSON},
		pb.DataType_BINARY: {Type: "String", Comment: FivetranBinary},
		pb.DataType_XML:    {Type: "String", Comment: FivetranXML},
	}
	// ColumnCommentToFivetranType
	// Mapping back to Fivetran types from FivetranDataTypesWithComments(ClickHouseType.Comment)
	ColumnCommentToFivetranType = map[string]pb.DataType{
		FivetranJSON:   pb.DataType_JSON,
		FivetranBinary: pb.DataType_BINARY,
		FivetranXML:    pb.DataType_XML,
	}
	// FivetranMetadataColumnTypes
	// Fivetran metadata columns have known constant names and types, and not Nullable.
	FivetranMetadataColumnTypes = map[string]ClickHouseType{
		FivetranID:      {Type: "String"},
		FivetranSynced:  {Type: "DateTime64(9, 'UTC')"},
		FivetranDeleted: {Type: "Bool"},
	}
)

// GetFivetranDataType
// Maps ClickHouse data types to Fivetran data types, taking Nullable into consideration.
// NB: STRING, JSON, XML, BINARY are all valid CH String types, we can only distinguish them by the column comment.
func GetFivetranDataType(col *ColumnDefinition) (pb.DataType, *pb.DecimalParams, error) {
	dataType, ok := ColumnCommentToFivetranType[col.Comment]
	if ok {
		return dataType, nil, nil
	}
	colType := RemoveNullable(col.Type)

	var decimalParams *pb.DecimalParams = nil
	if col.DecimalParams != nil {
		decimalParams = &pb.DecimalParams{
			Precision: uint32(col.DecimalParams.Precision),
			Scale:     uint32(col.DecimalParams.Scale),
		}
	}
	if decimalParams != nil {
		return pb.DataType_DECIMAL, decimalParams, nil
	}

	dataType, ok = ClickHouseDataTypes[colType]
	if !ok { // shouldn't happen if the tables are created by the connector
		return pb.DataType_UNSPECIFIED, nil, fmt.Errorf("can't map type %s to Fivetran types", colType)
	}
	return dataType, nil, nil
}

// GetClickHouseDataType
// - Fivetran Metadata fields have known types and are not Nullable
// - PrimaryKey fields are not Nullable (assumption)
// - all other fields are Nullable by default
func GetClickHouseDataType(col *pb.Column) (ClickHouseType, error) {
	metaType, ok := FivetranMetadataColumnTypes[col.Name]
	if ok {
		return metaType, nil
	}
	chType, ok := FivetranDataTypes[col.Type]
	if !ok {
		chType, ok = FivetranDataTypesWithComments[col.Type]
		if !ok {
			return ClickHouseType{}, fmt.Errorf("unknown datatype %s", col.Type.String())
		}
	}
	if chType.Type == "Decimal" && col.Decimal != nil {
		chType.Type = ToDecimalTypeWithParams(col.Decimal)
	}
	if col.PrimaryKey {
		return chType, nil
	}
	chType.Type = fmt.Sprintf("Nullable(%s)", chType.Type)
	return chType, nil
}

// ToDecimalTypeWithParams
// If Fivetran decimal precision or scale is greater than the maximum allowed by ClickHouse (P = 76), we set 76 instead.
// If Fivetran scale is greater than its precision, we set the scale equal to the precision.
// See precision and scale valid ranges: https://clickhouse.com/docs/en/sql-reference/data-types/decimal
func ToDecimalTypeWithParams(decimalParams *pb.DecimalParams) string {
	var (
		precision uint32
		scale     uint32
	)
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

// RemoveNullable
// Since LowCardinality is never applied by the destination app, we only need to handle Nullable.
func RemoveNullable(colType string) string {
	if strings.HasPrefix(colType, "Nullable") {
		colType = colType[9 : len(colType)-1]
	}
	return colType
}
