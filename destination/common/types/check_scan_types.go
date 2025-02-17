package types

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"fivetran.com/fivetran_sdk/destination/common/constants"
	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/shopspring/decimal"
)

func CheckScanTypes(
	fivetranColMap map[string]*pb.Column,
	driverColMap map[string]*DriverColumn,
) error {
	if len(fivetranColMap) != len(driverColMap) {
		return fmt.Errorf(
			"columns count in the table definition (%d) does not match the input file (%d). Table definition columns: %s, input file columns: %s",
			len(driverColMap), len(fivetranColMap),
			joinColNames(driverColMap), joinColNames(fivetranColMap))
	}
	for colName, driverCol := range driverColMap {
		fivetranCol, ok := fivetranColMap[colName]
		if !ok {
			return fmt.Errorf("column %s was not found in the table definition", colName)
		}
		var scanType reflect.Type
		switch colName {
		case constants.FivetranID:
			scanType = scanTypeString
		case constants.FivetranSynced:
			scanType = scanTypeTime
		case constants.FivetranDeleted:
			scanType = scanTypeBool
		default:
			scanType, ok = pkToFivetranToScanType[fivetranCol.PrimaryKey][fivetranCol.Type]
			if !ok {
				return fmt.Errorf("unknown Fivetran data type %s", fivetranCol.Type.String())
			}
		}
		if driverCol.ScanType != scanType {
			return fmt.Errorf("database column %s (PK: %v) has type %s (scan type: %s) which is incompatible with the input %s (scan type: %s)",
				fivetranCol.Name, fivetranCol.PrimaryKey, driverCol.DatabaseType, driverCol.ScanType.String(), fivetranCol.Type.String(), scanType)
		}
	}
	return nil
}

func joinColNames[Value any](cols map[string]Value) string {
	var names []string
	for name := range cols {
		names = append(names, name)
	}
	return strings.Join(names, ", ")
}

var (
	zeroBool    = false
	zeroInt16   = int16(0)
	zeroInt32   = int32(0)
	zeroInt64   = int64(0)
	zeroFloat32 = float32(0)
	zeroFloat64 = float64(0)
	zeroTime    = time.Time{}
	zeroDecimal = decimal.Decimal{}
	zeroString  = ""

	// Known Fivetran metadata and PK columns are non-Nullable.
	scanTypeBool    = reflect.TypeOf(zeroBool)
	scanTypeInt16   = reflect.TypeOf(zeroInt16)
	scanTypeInt32   = reflect.TypeOf(zeroInt32)
	scanTypeInt64   = reflect.TypeOf(zeroInt64)
	scanTypeFloat32 = reflect.TypeOf(zeroFloat32)
	scanTypeFloat64 = reflect.TypeOf(zeroFloat64)
	scanTypeTime    = reflect.TypeOf(zeroTime)
	scanTypeDecimal = reflect.TypeOf(zeroDecimal)
	scanTypeString  = reflect.TypeOf(zeroString)

	// All other columns are defined as Nullable (i.e. in a row, it's a pointer to a pointer).
	scanTypeNullableBool    = reflect.TypeOf(&zeroBool)
	scanTypeNullableInt16   = reflect.TypeOf(&zeroInt16)
	scanTypeNullableInt32   = reflect.TypeOf(&zeroInt32)
	scanTypeNullableInt64   = reflect.TypeOf(&zeroInt64)
	scanTypeNullableFloat32 = reflect.TypeOf(&zeroFloat32)
	scanTypeNullableFloat64 = reflect.TypeOf(&zeroFloat64)
	scanTypeNullableTime    = reflect.TypeOf(&zeroTime)
	scanTypeNullableDecimal = reflect.TypeOf(&zeroDecimal)
	scanTypeNullableString  = reflect.TypeOf(&zeroString)
)

// IsPrimaryKey? -> Fivetran data type -> Go driver scan type
var pkToFivetranToScanType = map[bool]map[pb.DataType]reflect.Type{
	true: {
		pb.DataType_BOOLEAN:        scanTypeBool,
		pb.DataType_INT:            scanTypeInt32,
		pb.DataType_SHORT:          scanTypeInt16,
		pb.DataType_LONG:           scanTypeInt64,
		pb.DataType_FLOAT:          scanTypeFloat32,
		pb.DataType_DOUBLE:         scanTypeFloat64,
		pb.DataType_DECIMAL:        scanTypeDecimal,
		pb.DataType_NAIVE_DATE:     scanTypeTime,
		pb.DataType_NAIVE_DATETIME: scanTypeTime,
		pb.DataType_UTC_DATETIME:   scanTypeTime,
		pb.DataType_STRING:         scanTypeString,
		pb.DataType_XML:            scanTypeString,
		pb.DataType_JSON:           scanTypeString,
		pb.DataType_BINARY:         scanTypeString,
		pb.DataType_NAIVE_TIME:     scanTypeString,
	},
	false: {
		pb.DataType_BOOLEAN:        scanTypeNullableBool,
		pb.DataType_INT:            scanTypeNullableInt32,
		pb.DataType_SHORT:          scanTypeNullableInt16,
		pb.DataType_LONG:           scanTypeNullableInt64,
		pb.DataType_FLOAT:          scanTypeNullableFloat32,
		pb.DataType_DOUBLE:         scanTypeNullableFloat64,
		pb.DataType_DECIMAL:        scanTypeNullableDecimal,
		pb.DataType_NAIVE_DATE:     scanTypeNullableTime,
		pb.DataType_NAIVE_DATETIME: scanTypeNullableTime,
		pb.DataType_UTC_DATETIME:   scanTypeNullableTime,
		pb.DataType_STRING:         scanTypeNullableString,
		pb.DataType_XML:            scanTypeNullableString,
		pb.DataType_JSON:           scanTypeNullableString,
		pb.DataType_BINARY:         scanTypeNullableString,
		pb.DataType_NAIVE_TIME:     scanTypeNullableString,
	},
}
