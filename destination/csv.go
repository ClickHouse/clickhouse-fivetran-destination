package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/shopspring/decimal"
)

type (
	CSVRow []string
	CSV    [][]string
)

// CSVRowToInsertValues
// TODO: check if the order of the columns in CSV is the same as in the table
func CSVRowToInsertValues(row CSVRow, table *pb.Table, nullStr string) ([]any, error) {
	if len(row) != len(table.Columns) {
		return nil, fmt.Errorf("expected %d columns, but row contains %d", len(table.Columns), len(row))
	}
	result := make([]any, len(row))
	for i, col := range table.Columns {
		if row[i] == nullStr {
			if col.Type == pb.DataType_JSON {
				result[i] = "{}" // JSON can't be nullable, so we use an empty object instead
			} else {
				result[i] = nil
			}
			continue
		}
		switch col.Type {
		case pb.DataType_BOOLEAN:
			value, err := strconv.ParseBool(row[i])
			if err != nil {
				return nil, err
			}
			result[i] = value
		case pb.DataType_SHORT:
			value, err := strconv.ParseInt(row[i], 10, 16)
			if err != nil {
				return nil, err
			}
			result[i] = int16(value)
		case pb.DataType_INT:
			value, err := strconv.ParseInt(row[i], 10, 32)
			if err != nil {
				return nil, err
			}
			result[i] = int32(value)
		case pb.DataType_LONG:
			value, err := strconv.ParseInt(row[i], 10, 64)
			if err != nil {
				return nil, err
			}
			result[i] = value
		case pb.DataType_FLOAT:
			value, err := strconv.ParseFloat(row[i], 32)
			if err != nil {
				return nil, err
			}
			result[i] = value
		case pb.DataType_DOUBLE:
			value, err := strconv.ParseFloat(row[i], 64)
			if err != nil {
				return nil, err
			}
			result[i] = value
		case pb.DataType_DECIMAL:
			value, err := decimal.NewFromString(row[i])
			if err != nil {
				return nil, err
			}
			result[i] = value
		case pb.DataType_NAIVE_DATE:
			value, err := time.Parse("2006-01-02", row[i])
			if err != nil {
				return nil, err
			}
			result[i] = value
		case pb.DataType_NAIVE_DATETIME:
			value, err := time.Parse("2006-01-02T15:04:05", row[i])
			if err != nil {
				return nil, err
			}
			result[i] = value
		case pb.DataType_UTC_DATETIME:
			value, err := time.Parse("2006-01-02T15:04:05.000000000Z", row[i])
			if err != nil {
				return nil, err
			}
			result[i] = value
		case
			pb.DataType_BINARY,
			pb.DataType_XML,
			pb.DataType_STRING,
			pb.DataType_JSON:
			// Some strings in CSVs are quoted, some are not. We need to handle both cases.
			if strings.HasPrefix(row[i], "\"") && strings.HasSuffix(row[i], "\"") {
				value, err := strconv.Unquote(row[i])
				if err != nil {
					return nil, err
				}
				result[i] = value
			} else {
				result[i] = row[i]
			}
		case pb.DataType_UNSPECIFIED:
			return nil, fmt.Errorf("column %s has unspecified type", col.Name)
		default:
			return nil, fmt.Errorf("no target type for column %s with type %s", col.Name, col.Type.String())
		}
	}
	return result, nil
}
