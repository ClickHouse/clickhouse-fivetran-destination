package main

import (
	"fmt"

	pb "fivetran.com/fivetran_sdk/proto"
)

var ConfigurationFormResponse = &pb.ConfigurationFormResponse{
	SchemaSelectionSupported: true,
	TableSelectionSupported:  true,
	Fields: []*pb.FormField{
		{
			Name:     "hostname",
			Label:    "Hostname",
			Required: true,
			Type: &pb.FormField_TextField{
				TextField: pb.TextField_PlainText,
			},
		},
		{
			Name:     "port",
			Label:    "Port",
			Required: false,
			Type: &pb.FormField_TextField{
				TextField: pb.TextField_PlainText,
			},
		},
		{
			Name:     "database",
			Label:    "Database",
			Required: false,
			Type: &pb.FormField_TextField{
				TextField: pb.TextField_PlainText,
			},
		},
		{
			Name:     "username",
			Label:    "Username",
			Required: false,
			Type: &pb.FormField_TextField{
				TextField: pb.TextField_Password,
			},
		},
		{
			Name:     "password",
			Label:    "Password",
			Required: false,
			Type: &pb.FormField_TextField{
				TextField: pb.TextField_Password,
			},
		},
		{
			Name:     "ssl",
			Label:    "SSL",
			Required: false,
			Type: &pb.FormField_ToggleField{
				ToggleField: &pb.ToggleField{},
			},
		},
	},
	Tests: []*pb.ConfigurationTest{
		{
			Name:  ConnectionTest,
			Label: "Test connection and basic operations",
		},
		{
			Name:  MutationTest,
			Label: "Test mutation operations",
		},
	},
}

func FailedWriteBatchResponse(schemaName string, tableName string, err error) *pb.WriteBatchResponse {
	logError("WriteBatch", err)
	return &pb.WriteBatchResponse{
		Response: &pb.WriteBatchResponse_Failure{
			Failure: fmt.Sprintf("Failed to write batch into `%s`.`%s`, cause: %s", schemaName, tableName, err),
		},
	}
}

func FailedDescribeTableResponse(schemaName string, tableName string, err error) *pb.DescribeTableResponse {
	logError("DescribeTable", err)
	return &pb.DescribeTableResponse{
		Response: &pb.DescribeTableResponse_Failure{
			Failure: fmt.Sprintf("Failed to describe table `%s`.`%s`, cause: %s", schemaName, tableName, err),
		},
	}
}

func NotFoundDescribeTableResponse() *pb.DescribeTableResponse {
	return &pb.DescribeTableResponse{
		Response: &pb.DescribeTableResponse_NotFound{NotFound: true},
	}
}

func FailedTestResponse(name string, err error) *pb.TestResponse {
	logError("Test", err)
	return &pb.TestResponse{
		Response: &pb.TestResponse_Failure{
			Failure: fmt.Sprintf("Test %s failed, cause: %s", name, err),
		},
	}
}

func FailedCreateTableResponse(schemaName string, tableName string, err error) *pb.CreateTableResponse {
	logError("CreateTable", err)
	return &pb.CreateTableResponse{
		Response: &pb.CreateTableResponse_Failure{
			Failure: fmt.Sprintf("Failed to create table `%s`.`%s`, cause: %s", schemaName, tableName, err),
		},
	}
}

func FailedAlterTableResponse(schemaName string, tableName string, err error) *pb.AlterTableResponse {
	logError("AlterTable", err)
	return &pb.AlterTableResponse{
		Response: &pb.AlterTableResponse_Failure{
			Failure: fmt.Sprintf("Failed to alter table `%s`.`%s`, cause: %s", schemaName, tableName, err),
		},
	}
}

func FailedTruncateTableResponse(schemaName string, tableName string, err error) *pb.TruncateResponse {
	logError("TruncateTable", err)
	return &pb.TruncateResponse{
		Response: &pb.TruncateResponse_Failure{
			Failure: fmt.Sprintf("Failed to truncate table `%s`.`%s`, cause: %s", schemaName, tableName, err),
		},
	}
}

func logError(endpoint string, err error) {
	LogError(fmt.Errorf("%s error: %w", endpoint, err))
}
