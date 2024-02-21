package service

import (
	"fmt"

	"fivetran.com/fivetran_sdk/destination/common/log"
	"fivetran.com/fivetran_sdk/destination/db/config"
	pb "fivetran.com/fivetran_sdk/proto"
)

var hostDescription = "ClickHouse Cloud service host. Format: address:port"

func GetConfigurationFormResponse() *pb.ConfigurationFormResponse {
	return &pb.ConfigurationFormResponse{
		SchemaSelectionSupported: true,
		TableSelectionSupported:  true,
		Fields: []*pb.FormField{
			{
				Name:        config.HostKey,
				Label:       "Host",
				Description: &hostDescription,
				Required:    true,
				Type: &pb.FormField_TextField{
					TextField: pb.TextField_PlainText,
				},
			},
			{
				Name:     config.DatabaseKey,
				Label:    "Database",
				Required: true,
				Type: &pb.FormField_TextField{
					TextField: pb.TextField_PlainText,
				},
			},
			{
				Name:     config.UsernameKey,
				Label:    "Username",
				Required: true,
				Type: &pb.FormField_TextField{
					TextField: pb.TextField_Password,
				},
			},
			{
				Name:     config.PasswordKey,
				Label:    "Password",
				Required: true,
				Type: &pb.FormField_TextField{
					TextField: pb.TextField_Password,
				},
			},
		},
		Tests: []*pb.ConfigurationTest{
			{
				Name:  ConnectionTest,
				Label: "Test connection and basic operations",
			},
		},
	}
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
	log.Error(fmt.Errorf("%s failed: %w", endpoint, err))
}
