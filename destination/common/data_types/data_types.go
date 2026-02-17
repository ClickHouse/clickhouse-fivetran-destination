package data_types

import (
	"fmt"
	"strings"

	c "fivetran.com/fivetran_sdk/destination/common/constants"
	pb "fivetran.com/fivetran_sdk/proto"
)

type ClickHouseType struct {
	Type    string
	Comment string
}

var (
	ClickHouseToFivetranType = map[string]pb.DataType{
		c.Bool:        pb.DataType_BOOLEAN,
		c.Int16:       pb.DataType_SHORT,
		c.Int32:       pb.DataType_INT,
		c.Int64:       pb.DataType_LONG,
		c.Float32:     pb.DataType_FLOAT,
		c.Float64:     pb.DataType_DOUBLE,
		c.Decimal:     pb.DataType_DECIMAL,
		c.Date:        pb.DataType_NAIVE_DATE,
		c.DateTime:    pb.DataType_NAIVE_DATETIME,
		c.DateTimeUTC: pb.DataType_UTC_DATETIME,
		c.String:      pb.DataType_STRING,
	}
	FivetranToClickHouseType = map[pb.DataType]ClickHouseType{
		pb.DataType_BOOLEAN:        {Type: c.Bool},
		pb.DataType_SHORT:          {Type: c.Int16},
		pb.DataType_INT:            {Type: c.Int32},
		pb.DataType_LONG:           {Type: c.Int64},
		pb.DataType_FLOAT:          {Type: c.Float32},
		pb.DataType_DOUBLE:         {Type: c.Float64},
		pb.DataType_DECIMAL:        {Type: c.Decimal},
		pb.DataType_NAIVE_DATE:     {Type: c.Date},
		pb.DataType_NAIVE_DATETIME: {Type: c.DateTime},
		pb.DataType_UTC_DATETIME:   {Type: c.DateTimeUTC},
		pb.DataType_STRING:         {Type: c.String},
	}
	// FivetranToClickHouseTypeWithComment
	// Fivetran STRING, XML, BINARY, JSON all are valid ClickHouse String types,
	// and by default we don't have a way to get the original Fivetran type from just a ClickHouse String.
	// So we add comments to the table columns using COMMENT clause to be able to distinguish them.
	// There is no corresponding type for Fivetran NAIVE_TIME in ClickHouse, so we use String with a comment as well.
	// NB: ClickHouse has JSON data type, see https://clickhouse.com/docs/en/sql-reference/data-types/json
	// however, it's marked as experimental and not production ready, so we use String instead.
	FivetranToClickHouseTypeWithComment = map[pb.DataType]ClickHouseType{
		pb.DataType_XML:        {Type: c.String, Comment: c.XMLColumnComment},
		pb.DataType_JSON:       {Type: c.String, Comment: c.JSONColumnComment},
		pb.DataType_BINARY:     {Type: c.String, Comment: c.BinaryColumnComment},
		pb.DataType_NAIVE_TIME: {Type: c.String, Comment: c.NaiveTimeColumnComment},
	}
	// ClickHouseColumnCommentToFivetranType
	// Mapping back to Fivetran types from FivetranToClickHouseTypeWithComment(ClickHouseType.Comment)
	ClickHouseColumnCommentToFivetranType = map[string]pb.DataType{
		c.XMLColumnComment:       pb.DataType_XML,
		c.JSONColumnComment:      pb.DataType_JSON,
		c.BinaryColumnComment:    pb.DataType_BINARY,
		c.NaiveTimeColumnComment: pb.DataType_NAIVE_TIME,
	}
	// FivetranMetadataColumnToClickHouseType
	// Fivetran metadata columns have known constant names and types, and not Nullable.
	FivetranMetadataColumnToClickHouseType = map[string]ClickHouseType{
		c.FivetranID:      {Type: c.String},
		c.FivetranSynced:  {Type: c.DateTimeUTC},
		c.FivetranDeleted: {Type: c.Bool},
	}
)

// ToFivetranDataType maps ClickHouse data types to Fivetran data types, taking Nullable into consideration.
// STRING, JSON, XML, BINARY are all valid CH String types, we distinguish them by the column comment.
func ToFivetranDataType(
	colType string,
	colComment string,
	decimalParams *pb.DecimalParams,
) (pb.DataType, *pb.DecimalParams, error) {
	dataType, ok := ClickHouseColumnCommentToFivetranType[colComment]
	if ok {
		return dataType, nil, nil
	}
	// Since LowCardinality is never applied by the destination app, we only need to handle Nullable.
	if strings.HasPrefix(colType, c.Nullable) { // Nullable(String) -> String
		colType = colType[len(c.Nullable)+1 : len(colType)-1]
	}
	if decimalParams != nil {
		return pb.DataType_DECIMAL, decimalParams, nil
	}
	dataType, ok = ClickHouseToFivetranType[colType]
	if !ok { // shouldn't happen if the tables are created by the connector
		return pb.DataType_UNSPECIFIED, nil, fmt.Errorf("can't map type %s to Fivetran types", colType)
	}
	return dataType, nil, nil
}

// ToClickHouseDataType converts a Fivetran column definition to a ClickHouse type.
//   - Fivetran Metadata fields have known types and are not Nullable
//   - Primary key fields are not Nullable
//   - All other fields are Nullable by default
func ToClickHouseDataType(col *pb.Column) (ClickHouseType, error) {
	metaType, ok := FivetranMetadataColumnToClickHouseType[col.Name]
	if ok {
		return metaType, nil
	}
	chType, ok := FivetranToClickHouseType[col.Type]
	if !ok {
		chType, ok = FivetranToClickHouseTypeWithComment[col.Type]
		if !ok {
			return ClickHouseType{}, fmt.Errorf("unknown datatype %s", col.Type.String())
		}
	}
	if chType.Type == c.Decimal && col.Params != nil {
		decimal := col.Params.GetDecimal()
		if decimal != nil {
			chType.Type = ToClickHouseDecimalType(decimal)
		}
	}
	if col.PrimaryKey {
		return chType, nil
	}
	chType.Type = fmt.Sprintf("%s(%s)", c.Nullable, chType.Type)
	return chType, nil
}

// ToClickHouseDecimalType converts Fivetran decimal parameters to a ClickHouse Decimal type string.
// If Fivetran decimal precision or scale is greater than the maximum allowed by ClickHouse (P = 76), we set 76 instead.
// If Fivetran scale is greater than its precision, we set the scale equal to the precision.
// See precision and scale valid ranges: https://clickhouse.com/docs/en/sql-reference/data-types/decimal
func ToClickHouseDecimalType(decimalParams *pb.DecimalParams) string {
	var (
		precision uint32
		scale     uint32
	)
	if decimalParams.Precision > c.MaxDecimalPrecision {
		precision = c.MaxDecimalPrecision
	} else {
		precision = decimalParams.Precision
	}
	if decimalParams.Scale > precision {
		scale = precision
	} else {
		scale = decimalParams.Scale
	}
	return fmt.Sprintf("%s(%d, %d)", c.Decimal, precision, scale)
}
