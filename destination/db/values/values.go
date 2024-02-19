package values

import (
	"fmt"
	"strconv"
	"time"

	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/shopspring/decimal"
)

func Quote(colType pb.DataType, value string) string {
	switch colType {
	case // quote types that we can pass as a string
		pb.DataType_NAIVE_DATE,
		pb.DataType_NAIVE_DATETIME,
		pb.DataType_UTC_DATETIME,
		pb.DataType_STRING,
		pb.DataType_BINARY,
		pb.DataType_XML,
		pb.DataType_JSON:
		return fmt.Sprintf("'%s'", value)
	default:
		return value
	}
}

func Parse(colName string, colType pb.DataType, val string) (any, error) {
	switch colType {
	case pb.DataType_BOOLEAN:
		result, err := strconv.ParseBool(val)
		if err != nil {
			return nil, fmt.Errorf("can't parse value %s as boolean for column %s: %w", val, colName, err)
		}
		return result, nil
	case pb.DataType_SHORT:
		result, err := strconv.ParseInt(val, 10, 16)
		if err != nil {
			return nil, fmt.Errorf("can't parse value %s as int16 for column %s: %w", val, colName, err)
		}
		return int16(result), nil
	case pb.DataType_INT:
		result, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("can't parse value %s as int32 for column %s: %w", val, colName, err)
		}
		return int32(result), nil
	case pb.DataType_LONG:
		result, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("can't parse value %s as int64 for column %s: %w", val, colName, err)
		}
		return result, nil
	case pb.DataType_FLOAT:
		result, err := strconv.ParseFloat(val, 32)
		if err != nil {
			return nil, fmt.Errorf("can't parse value %s as float32 for column %s: %w", val, colName, err)
		}
		return result, nil
	case pb.DataType_DOUBLE:
		result, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil, fmt.Errorf("can't parse value %s as float64 for column %s: %w", val, colName, err)
		}
		return result, nil
	case pb.DataType_DECIMAL:
		result, err := decimal.NewFromString(val)
		if err != nil {
			return nil, fmt.Errorf("can't parse value %s as decimal for column %s: %w", val, colName, err)
		}
		return result, nil
	case pb.DataType_NAIVE_DATE:
		result, err := time.Parse("2006-01-02", val)
		if err != nil {
			return nil, fmt.Errorf("can't parse value %s as naive date for column %s: %w", val, colName, err)
		}
		return result, nil
	case pb.DataType_NAIVE_DATETIME:
		result, err := time.Parse("2006-01-02T15:04:05", val)
		if err != nil {
			return nil, fmt.Errorf("can't parse value %s as naive datetime for column %s: %w", val, colName, err)
		}
		return result, nil
	case pb.DataType_UTC_DATETIME:
		result, err := time.Parse("2006-01-02T15:04:05.000000000Z", val)
		if err != nil {
			return nil, fmt.Errorf("can't parse value %s as UTC datetime for column %s: %w", val, colName, err)
		}
		return result, nil
	case // "string" types work as-is
		pb.DataType_BINARY,
		pb.DataType_XML,
		pb.DataType_STRING,
		pb.DataType_JSON:
		return val, nil
	default:
		return nil, fmt.Errorf("no target type for column %s with type %s", colName, colType.String())
	}
}
