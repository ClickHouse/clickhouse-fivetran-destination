package main

import (
	"fmt"

	pb "fivetran.com/fivetran_sdk/proto"
)

func FailedTestResponse(name string, err error) *pb.TestResponse {
	return &pb.TestResponse{
		Response: &pb.TestResponse_Failure{
			Failure: fmt.Sprintf("Test %s failed, cause: %s", name, err),
		},
	}
}

func TableDescriptionToColumns(tableDescription map[string]string) []*pb.Column {
	columns := make([]*pb.Column, len(tableDescription))
	i := 0
	for colName, colType := range tableDescription {
		columns[i] = &pb.Column{
			Name:       colName,
			Type:       GetDataType(colType),
			PrimaryKey: false,
			Decimal:    nil,
		}
		i++
	}
	return columns
}
