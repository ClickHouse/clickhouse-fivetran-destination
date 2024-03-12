package validation

import (
	"fmt"
	"reflect"
	"time"

	"fivetran.com/fivetran_sdk/destination/common/types"
	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/shopspring/decimal"
)

func ValidateScanTypes(fivetranColMap map[string]*pb.Column, driverColMap map[string]*types.DriverColumn) error {
	if len(fivetranColMap) != len(driverColMap) {
		return fmt.Errorf("columns count in the table definition (%d) does not match the input file (%d)",
			len(fivetranColMap), len(driverColMap)) // TODO <- list all columns
	}
	for csvColName, colType := range driverColMap {
		fivetranCol, ok := fivetranColMap[csvColName]
		if !ok {
			return fmt.Errorf("column %s was not found in the table definition", csvColName)
		}
		err := validateScanType(fivetranCol, colType)
		if err != nil {
			return err
		}
	}
	return nil
}

func validateScanType(fivetranCol *pb.Column, driverCol *types.DriverColumn) error {
	scanType, ok := fivetranToScanType[fivetranCol.Type]
	if !ok {
		return fmt.Errorf("unknown Fivetran data type %s", fivetranCol.Type.String())
	}
	if driverCol.ScanType != scanType {
		return scanTypeError(fivetranCol, driverCol)
	}
	return nil
}

func scanTypeError(fivetranCol *pb.Column, driverCol *types.DriverColumn) error {
	return fmt.Errorf("database column %s has type %s which is incompatible with the input %s (%s)",
		fivetranCol.Name, driverCol.DatabaseType, fivetranCol.Type.String(), driverCol.ScanType.String())
}

var (
	scanTypeFloat32 = reflect.TypeOf(float32(0))
	scanTypeFloat64 = reflect.TypeOf(float64(0))
	scanTypeInt16   = reflect.TypeOf(int16(0))
	scanTypeInt32   = reflect.TypeOf(int32(0))
	scanTypeInt64   = reflect.TypeOf(int64(0))
	scanTypeBool    = reflect.TypeOf(true)
	scanTypeTime    = reflect.TypeOf(time.Time{})
	scanTypeString  = reflect.TypeOf("")
	scanTypeDecimal = reflect.TypeOf(decimal.Decimal{})
)

var fivetranToScanType = map[pb.DataType]reflect.Type{
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
}
