package values

import (
	"fmt"
	"strconv"
	"time"

	"fivetran.com/fivetran_sdk/destination/common/constants"
	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/shopspring/decimal"
)

func Value(colType pb.DataType, value string) (string, error) {
	switch colType {
	case // quote types that we can pass as a string
		pb.DataType_NAIVE_DATE,
		pb.DataType_NAIVE_DATETIME,
		pb.DataType_STRING,
		pb.DataType_DECIMAL,
		pb.DataType_FLOAT,
		pb.DataType_DOUBLE,
		pb.DataType_BINARY,
		pb.DataType_XML,
		pb.DataType_JSON:
		return fmt.Sprintf("'%s'", value), nil
	// specify DateTime64(9) as nanos instead
	case pb.DataType_UTC_DATETIME:
		utcDateTime, err := time.Parse(constants.UTCDateTimeFormat, value)
		if err != nil {
			return "", fmt.Errorf("can't parse value %s as UTC datetime: %w", value, err)
		}
		return fmt.Sprintf("'%d'", utcDateTime.UnixNano()), nil
	default:
		return value, nil
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
		result, err := time.Parse(constants.NaiveDateFormat, val)
		if err != nil {
			return nil, fmt.Errorf("can't parse value %s as naive date for column %s: %w", val, colName, err)
		}
		// Date32 date range is the same as DateTime64, so that makes it [1900-01-01, 2299-12-31].
		// See https://clickhouse.com/docs/en/sql-reference/data-types/date32
		// See https://clickhouse.com/docs/en/sql-reference/data-types/datetime64
		year := result.Year()
		if year < 1900 {
			return time.Date(1900, time.January, 1, 0, 0, 0, 0, time.UTC), nil
		}
		if year > 2299 {
			return time.Date(2299, time.December, 31, 0, 0, 0, 0, time.UTC), nil
		}
		return result, nil
	case pb.DataType_NAIVE_DATETIME:
		result, err := time.Parse(constants.NaiveDateTimeFormat, val)
		if err != nil {
			return nil, fmt.Errorf("can't parse value %s as naive datetime for column %s: %w", val, colName, err)
		}
		// Supported range of values: [1900-01-01 00:00:00, 2299-12-31 23:59:59.99999999].
		// See https://clickhouse.com/docs/en/sql-reference/data-types/datetime64
		year, month, day := result.Date()
		if year > 2262 || (year == 2262 && month > 4) || (year == 2262 && month == 4 && day > 11) {
			return time.Date(2262, time.April, 11, 23, 47, 16, 0, time.UTC), nil
		}
		if year < 1900 {
			return time.Date(1900, time.January, 1, 0, 0, 0, 0, time.UTC), nil
		}
		return result, nil
	case pb.DataType_UTC_DATETIME:
		result, err := time.Parse(constants.UTCDateTimeFormat, val)
		if err != nil {
			return nil, fmt.Errorf("can't parse value %s as UTC datetime for column %s: %w", val, colName, err)
		}
		// With max precision (9, which is nanoseconds), the maximum supported value is 2262-04-11 23:47:16 in UTC.
		// See https://clickhouse.com/docs/en/sql-reference/data-types/datetime64
		year, month, day := result.Date()
		if year > 2262 || (year == 2262 && month > 4) || (year == 2262 && month == 4 && day > 11) {
			return time.Date(2262, time.April, 11, 23, 47, 16, 0, time.UTC), nil
		}
		if year < 1900 {
			return time.Date(1900, time.January, 1, 0, 0, 0, 0, time.UTC), nil
		}
		hours, minutes, seconds := result.Clock()
		if year == 2262 && month == 4 && day == 11 && hours == 23 {
			if minutes > 47 || minutes == 47 && seconds > 16 || minutes == 47 && seconds == 16 && result.Nanosecond() > 0 {
				return time.Date(2262, time.April, 11, 23, 47, 16, 0, time.UTC), nil
			}
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
